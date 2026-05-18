from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Dict, Iterable, List, Optional

from databricks.labs.dqx.check_funcs import make_condition
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def _and_all(predicates: Iterable[Column]) -> Column:
    result = F.lit(True)
    for predicate in predicates:
        result = result & predicate
    return result


def _or_any(predicates: Iterable[Column]) -> Column:
    result = F.lit(False)
    for predicate in predicates:
        result = result | predicate
    return result


_TECHNICAL_COMMON_PREFIX = "__dqx_timeliness_common__"


def _safe_rule_name(rule_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]+", "_", rule_id).strip("_").lower()


def _materialized_common_column(rule_id: str) -> str:
    return f"{_TECHNICAL_COMMON_PREFIX}{_safe_rule_name(rule_id)}"


def _quote_sql_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


@dataclass(frozen=True)
class TimelinessContext:
    columns: Dict[str, str]
    accepted_formats: List[str]
    invalid_date_values: set[str]

    @classmethod
    def from_config(
        cls,
        columns: Optional[Dict[str, str]],
        date_parsing: Optional[Dict[str, Any]],
    ) -> "TimelinessContext":
        date_parsing = date_parsing or {}
        invalid_values = {
            str(v).strip().lower()
            for v in (date_parsing.get("invalid_values") or [])
            if v is not None
        }
        return cls(
            columns=columns or {},
            accepted_formats=date_parsing.get("accepted_formats") or [],
            invalid_date_values=invalid_values,
        )

    def field_name(self, field: str) -> str:
        return self.columns.get(field, field)

    def col(self, field: str) -> Column:
        return F.col(self.field_name(field))

    def normalized_string(self, field: str) -> Column:
        return F.lower(F.trim(self.col(field).cast("string")))

    def parsed_date(self, field: str) -> Column:
        raw = self.col(field)
        if not self.accepted_formats:
            return F.to_date(raw)

        parsed = [
            F.to_date(raw.cast("string"), fmt)
            for fmt in self.accepted_formats
        ]
        return F.coalesce(*parsed)

    def is_present(self, field: str) -> Column:
        raw = self.col(field)
        as_text = F.trim(raw.cast("string"))
        present = raw.isNotNull() & (as_text != F.lit(""))
        if self.invalid_date_values:
            present = present & (~F.lower(as_text).isin(list(self.invalid_date_values)))
        return present

    def is_valid_date(self, field: str) -> Column:
        return self.is_present(field) & self.parsed_date(field).isNotNull()

    def sql_expression(self, expression: str) -> Column:
        """
        Compile a Spark SQL predicate.

        Expressions can use physical DataFrame columns directly, matching
        check_funcs.sql_expression style. Optional {logical_field}
        placeholders are also supported and resolved through the YAML
        columns map.
        """
        rendered = re.sub(
            r"\{([A-Za-z_][A-Za-z0-9_]*)\}",
            lambda m: _quote_sql_identifier(self.field_name(m.group(1))),
            expression,
        )
        return F.expr(rendered)


class PredicateCompiler:
    def __init__(self, ctx: TimelinessContext):
        self.ctx = ctx

    def condition(self, block: Optional[Dict[str, Any]]) -> Column:
        if not block:
            return F.lit(True)
        if isinstance(block, str):
            return self.ctx.sql_expression(block)
        if "all" in block:
            return _and_all(self.clause(c) for c in (block.get("all") or []))
        if "any" in block:
            return _or_any(self.clause(c) for c in (block.get("any") or []))
        return self.clause(block)

    def clause(self, clause: Dict[str, Any]) -> Column:
        sql = (
            clause.get("sql_expression")
            or clause.get("sql")
            or clause.get("sql_expr")
            or clause.get("expression")
        )
        if sql:
            return self.ctx.sql_expression(str(sql))

        field = clause.get("field") or clause.get("column")
        op = (clause.get("op") or "eq").lower()

        if op == "eq":
            return self.ctx.col(field) == F.lit(clause.get("value"))
        if op in {"ne", "neq", "not_eq"}:
            return self.ctx.col(field) != F.lit(clause.get("value"))
        if op == "in":
            return self.ctx.col(field).isin(clause.get("values") or [])
        if op in {"not_in", "nin"}:
            return ~self.ctx.col(field).isin(clause.get("values") or [])
        if op == "eq_ci":
            return self.ctx.normalized_string(field) == F.lit(str(clause.get("value")).lower())
        if op == "in_ci":
            values = [str(v).lower() for v in (clause.get("values") or [])]
            return self.ctx.normalized_string(field).isin(values)
        if op == "is_null":
            return self.ctx.col(field).isNull()
        if op == "is_not_null":
            return self.ctx.col(field).isNotNull()

        raise ValueError(f"Unsupported timeliness_matrix operator: {op}")

    def valid_dates(self, fields: Iterable[str]) -> Column:
        return _and_all(self.ctx.is_valid_date(field) for field in fields)

    def date_order(self, order_cfg: Any) -> Column:
        if not order_cfg:
            return F.lit(True)

        checks = order_cfg if isinstance(order_cfg, list) else [order_cfg]
        predicates = []
        for check in checks:
            before = self.ctx.parsed_date(check["before"])
            after = self.ctx.parsed_date(check["after"])
            strict = bool(check.get("strict", True))
            predicates.append(before < after if strict else before <= after)
        return _and_all(predicates)

    def common_rule(self, block: Optional[Dict[str, Any]]) -> Column:
        if not block:
            return F.lit(True)

        predicates: List[Column] = []
        if any(k in block for k in ("all", "any", "field", "column", "sql_expression", "sql", "sql_expr", "expression")):
            predicates.append(self.condition(block))
        if "valid_dates" in block:
            predicates.append(self.valid_dates(block.get("valid_dates") or []))
        if "date_order" in block:
            predicates.append(self.date_order(block.get("date_order")))
        return _and_all(predicates)


class DatePointCompiler:
    def __init__(self, ctx: TimelinessContext, date_points: Dict[str, Any]):
        self.ctx = ctx
        self.date_points = date_points

    def resolve(self, point_cfg: Any) -> Column:
        if isinstance(point_cfg, str):
            if point_cfg not in self.date_points:
                raise KeyError(f"Unknown timeliness date point: {point_cfg}")
            point_cfg = self.date_points[point_cfg]
        return self.compile(point_cfg)

    def compile(self, point_cfg: Dict[str, Any]) -> Column:
        strategy = (point_cfg.get("strategy") or "field").lower()
        if strategy in {"field", "column"}:
            return self.ctx.parsed_date(point_cfg.get("field") or point_cfg.get("column"))
        if strategy == "latest_of":
            fields = point_cfg.get("fields") or point_cfg.get("columns") or []
            return F.greatest(*[self.ctx.parsed_date(field) for field in fields])
        if strategy == "earliest_of":
            fields = point_cfg.get("fields") or point_cfg.get("columns") or []
            return F.least(*[self.ctx.parsed_date(field) for field in fields])
        raise ValueError(f"Unsupported timeliness date point strategy: {strategy}")


class SlaCompiler:
    def __init__(self, sla_rules: Dict[str, Any]):
        self.sla_rules = sla_rules

    def resolve(self, sla_cfg: Any) -> Dict[str, Any]:
        if isinstance(sla_cfg, str):
            if sla_cfg not in self.sla_rules:
                raise KeyError(f"Unknown timeliness SLA rule: {sla_cfg}")
            return self.sla_rules[sla_cfg]
        return sla_cfg

    def breach(self, actual_days: Column, sla_cfg: Dict[str, Any]) -> Column:
        op = (sla_cfg.get("op") or "gt").lower()
        threshold = F.lit(int(sla_cfg["value"]))
        if op == "gt":
            return actual_days > threshold
        if op in {"gte", "ge"}:
            return actual_days >= threshold
        if op == "lt":
            return actual_days < threshold
        if op in {"lte", "le"}:
            return actual_days <= threshold
        if op == "eq":
            return actual_days == threshold
        raise ValueError(f"Unsupported timeliness SLA operator: {op}")


class TimelinessMatrixCompiler:
    def __init__(
        self,
        *,
        columns: Optional[Dict[str, str]],
        common_validations: Optional[Dict[str, Any]],
        common_rules: Optional[Dict[str, Any]],
        date_points: Optional[Dict[str, Any]],
        sla_rules: Optional[Dict[str, Any]],
        date_parsing: Optional[Dict[str, Any]],
    ):
        self.ctx = TimelinessContext.from_config(columns, date_parsing)
        self.predicates = PredicateCompiler(self.ctx)
        self.date_points = DatePointCompiler(self.ctx, date_points or {})
        self.slas = SlaCompiler(sla_rules or {})
        self.common_validations = common_validations or {}
        self.compiled_common_rules = self._compile_common_rules(common_rules or {})
        self.common_gate = self._compile_common_gate()

    def compile_rule_matches(self, rules: List[Dict[str, Any]]) -> List[Column]:
        return [self.compile_rule(rule) for rule in rules]

    def compile_rule(self, rule: Dict[str, Any]) -> Column:
        code = self._test_code(rule)
        rule_gate = self._compile_referenced_common_rules(rule)
        rule_gate = rule_gate & self.predicates.condition(rule.get("when"))
        rule_gate = rule_gate & self.predicates.valid_dates(rule.get("additional_valid_dates") or [])
        rule_gate = rule_gate & self.predicates.date_order(rule.get("date_order"))

        start_date = self.date_points.resolve(rule.get("start") or rule.get("start_date"))
        end_date = self.date_points.resolve(rule.get("end") or rule.get("end_date"))
        actual_days = F.datediff(end_date, start_date)
        sla_cfg = self.slas.resolve(rule.get("sla") or rule.get("calendar_days"))
        rule_gate = rule_gate & self.slas.breach(actual_days, sla_cfg)

        return F.when(self.common_gate & rule_gate, F.lit(code))

    def _compile_common_rules(self, common_rules: Dict[str, Any]) -> Dict[str, Column]:
        return {
            rule_id: self._compile_common_rule(rule_id, rule_cfg)
            for rule_id, rule_cfg in common_rules.items()
        }

    def _compile_common_rule(self, rule_id: str, rule_cfg: Dict[str, Any]) -> Column:
        materialized_column = (
            rule_cfg.get("materialized_column")
            or rule_cfg.get("materialize_as")
        )
        if materialized_column:
            return F.col(materialized_column)
        return self.predicates.common_rule(rule_cfg)

    def _compile_common_gate(self) -> Column:
        return _and_all([
            self.predicates.condition(self.common_validations.get("population")),
            self.predicates.valid_dates(
                self.common_validations.get("required_valid_dates") or []
            ),
            self.predicates.date_order(self.common_validations.get("date_order")),
        ])

    def _compile_referenced_common_rules(self, rule: Dict[str, Any]) -> Column:
        used_rule_ids = rule.get("use_common_rules") or []
        missing = [
            rule_id for rule_id in used_rule_ids
            if rule_id not in self.compiled_common_rules
        ]
        if missing:
            raise KeyError(f"Unknown timeliness common rules: {missing}")
        return _and_all(self.compiled_common_rules[rule_id] for rule_id in used_rule_ids)

    @staticmethod
    def _test_code(rule: Dict[str, Any]) -> str:
        return (
            rule.get("test_code")
            or (rule.get("metadata") or {}).get("test_code")
            or rule.get("error_code")
            or rule["id"]
        )


def _message_from_matches(
    rule_matches: List[Column],
    *,
    match_policy: str,
    code_separator: str,
) -> Column:
    if not rule_matches:
        return F.lit(None).cast("string")

    if match_policy == "all":
        message = F.concat_ws(code_separator, *rule_matches)
        return F.when(message == F.lit(""), F.lit(None).cast("string")).otherwise(message)

    return F.coalesce(*rule_matches)


def _iter_timeliness_rule_configs(rule_pack_cfg: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    for rule_cfg in rule_pack_cfg.get("rules", []) or []:
        yield rule_cfg

    for col_payload in (rule_pack_cfg.get("columns", {}) or {}).values():
        for rule_cfg in (col_payload or {}).get("rules", []) or []:
            yield rule_cfg


def precompute_timeliness_common_rules(
    df: DataFrame,
    rule_pack_cfg: Dict[str, Any],
) -> tuple[DataFrame, List[str]]:
    """
    Materialize timeliness common rules as boolean columns before DQX runs.

    This gives true per-row reuse: every common rule is evaluated once into a
    technical column, and every T-rule references that boolean column.
    """
    technical_columns: List[str] = []

    for rule_cfg in _iter_timeliness_rule_configs(rule_pack_cfg):
        args = rule_cfg.get("arguments") or {}
        common_rules = args.get("common_rules") or {}
        if not common_rules:
            continue
        if args.get("precompute_common_rules", True) is False:
            continue

        ctx = TimelinessContext.from_config(
            args.get("columns") or {},
            args.get("date_parsing"),
        )
        predicate_compiler = PredicateCompiler(ctx)

        for rule_id, common_rule_cfg in common_rules.items():
            materialized_column = (
                common_rule_cfg.get("materialize_as")
                or common_rule_cfg.get("materialized_column")
                or _materialized_common_column(rule_id)
            )
            common_rule_cfg["materialized_column"] = materialized_column

            if materialized_column in df.columns or materialized_column in technical_columns:
                continue

            df = df.withColumn(
                materialized_column,
                predicate_compiler.common_rule(common_rule_cfg).cast("boolean"),
            )
            technical_columns.append(materialized_column)

    return df, technical_columns


def timeliness_matrix(
    column: str,
    *,
        columns: Optional[Dict[str, str]] = None,
    common_validations: Optional[Dict[str, Any]] = None,
    variants: Optional[List[Dict[str, Any]]] = None,
    rules: Optional[List[Dict[str, Any]]] = None,
    common_rules: Optional[Dict[str, Any]] = None,
    date_points: Optional[Dict[str, Any]] = None,
    sla_rules: Optional[Dict[str, Any]] = None,
    date_parsing: Optional[Dict[str, Any]] = None,
    emit: Optional[Dict[str, Any]] = None,
    alias: Optional[str] = None,
) -> Column:
    """
    Execute a reusable timeliness decision table as one DQX row rule.

    The public callable stays small so the YAML mapping remains stable while
    the matrix compiler handles reusable predicates, date points, and SLAs.
    """
    emit = emit or {}
    rule_entries = rules if rules is not None else (variants or [])
    match_policy = (emit.get("match_policy") or "first").lower()
    code_separator = emit.get("code_separator") or "|"

    compiler = TimelinessMatrixCompiler(
        columns=columns or {},
        common_validations=common_validations,
        common_rules=common_rules,
        date_points=date_points,
        sla_rules=sla_rules,
        date_parsing=date_parsing,
    )
    message = _message_from_matches(
        compiler.compile_rule_matches(rule_entries),
        match_policy=match_policy,
        code_separator=code_separator,
    )

    return make_condition(
        condition=message.isNotNull(),
        message=message,
        alias=alias or f"{column}_timeliness_matrix",
    )
