from __future__ import annotations

import inspect
import re
from typing import Any, Dict, Iterable, List, Optional

from databricks.labs.dqx.check_funcs import make_condition
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from zkblock.dqx import yaml_constants as YC
from zkblock.dqx.dqx_rule_registry import DQRuleRegistry


def _and_all(predicates: Iterable[Column]) -> Column:
    result = F.lit(True)
    for predicate in predicates:
        result = result & predicate
    return result


def _safe_rule_name(rule_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]+", "_", rule_id).strip("_").lower()


def _materialized_precompute_column(rule_name: str) -> str:
    return f"{YC.TIMELINESS_PRECOMPUTE_COLUMN_PREFIX}{_safe_rule_name(rule_name)}"


def _rule_name(rule_cfg: Dict[str, Any]) -> str:
    return rule_cfg.get(YC.NAME_KEY) or rule_cfg.get(YC.ID_KEY)


def _sql_condition(block: Any) -> Column:
    if not block:
        return F.lit(True)
    if isinstance(block, str):
        return F.expr(block)
    expression = block.get("sql_expression") or block.get("expression")
    if not expression:
        raise ValueError("Timeliness 'when' only supports sql_expression/expression.")
    return F.expr(str(expression))


def _rule_condition_from_make_condition(
    *,
    rule_cfg: Dict[str, Any],
    registry: DQRuleRegistry,
    default_column: str,
) -> Column:
    """
    Execute a standard row-rule config and return its boolean match predicate.

    Contract: row rules return a DQX make_condition Column where non-null means
    the condition matched. Timeliness orchestration combines those booleans.
    """
    if registry is None:
        raise ValueError("Timeliness standard rule execution requires a registry.")

    fn = registry.get_row(rule_cfg[YC.ID_KEY])
    kwargs = dict(rule_cfg.get(YC.ARGUMENTS_KEY, {}) or {})

    sig = inspect.signature(fn)
    if rule_cfg.get(YC.MESSAGE_KEY) and "message" in sig.parameters and "message" not in kwargs:
        kwargs["message"] = rule_cfg[YC.MESSAGE_KEY]

    name = _rule_name(rule_cfg)
    if name and "alias" in sig.parameters and "alias" not in kwargs:
        kwargs["alias"] = _materialized_precompute_column(name)

    if "expression" in sig.parameters and "column" not in sig.parameters:
        match_or_null = fn(**kwargs)
    else:
        rule_column = (
            rule_cfg.get("column")
            or rule_cfg.get(YC.ANCHOR_COLUMN_KEY)
            or default_column
        )
        match_or_null = fn(rule_column, **kwargs)

    predicate = match_or_null.isNotNull()
    rule_filter = rule_cfg.get(YC.FILTER_KEY)
    if rule_filter:
        predicate = predicate & F.expr(rule_filter)
    return predicate


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


class TimelinessMatrixCompiler:
    def __init__(
        self,
        *,
        anchor_column: str,
        registry: DQRuleRegistry,
        precompute_rules: Optional[Dict[str, Any]],
    ):
        self.anchor_column = anchor_column
        self.registry = registry
        self.compiled_precompute_rules = self._compile_precompute_rules(precompute_rules or {})

    def compile_case_matches(self, timeliness_cases: List[Dict[str, Any]]) -> List[Column]:
        return [self.compile_case(case_cfg) for case_cfg in timeliness_cases]

    def compile_case(self, case_cfg: Dict[str, Any]) -> Column:
        code = (
            case_cfg.get("test_code")
            or (case_cfg.get(YC.METADATA_KEY) or {}).get("test_code")
            or case_cfg[YC.ID_KEY]
        )

        case_gate = self._compile_referenced_precompute_rules(case_cfg)
        case_gate = case_gate & _sql_condition(case_cfg.get("when"))

        case_rules = case_cfg.get(YC.RULES_KEY) or []
        if not case_rules:
            raise ValueError(f"Timeliness case '{case_cfg.get(YC.ID_KEY)}' must define rules.")

        case_gate = case_gate & _and_all(
            _rule_condition_from_make_condition(
                rule_cfg=rule_cfg,
                registry=self.registry,
                default_column=self.anchor_column,
            )
            for rule_cfg in case_rules
        )

        return F.when(case_gate, F.lit(code))

    def _compile_precompute_rules(self, precompute_rules: Dict[str, Any]) -> Dict[str, Column]:
        compiled: Dict[str, Column] = {}
        for rule_cfg in precompute_rules.get(YC.RULES_KEY, []) or []:
            name = _rule_name(rule_cfg)
            if not name:
                raise ValueError("Every timeliness precompute rule requires a 'name'.")
            if name in compiled:
                raise ValueError(f"Duplicate timeliness precompute rule name: {name}")

            materialized_column = rule_cfg.get(YC.MATERIALIZED_COLUMN_KEY)
            if materialized_column:
                compiled[name] = F.col(materialized_column)
            else:
                compiled[name] = _rule_condition_from_make_condition(
                    rule_cfg=rule_cfg,
                    registry=self.registry,
                    default_column=self.anchor_column,
                )

        return compiled

    def _compile_referenced_precompute_rules(self, case_cfg: Dict[str, Any]) -> Column:
        used_rule_names = case_cfg.get(YC.USE_PRECOMPUTE_RULES_KEY) or []
        missing = [
            rule_name for rule_name in used_rule_names
            if rule_name not in self.compiled_precompute_rules
        ]
        if missing:
            raise KeyError(f"Unknown timeliness precompute rules: {missing}")
        return _and_all(self.compiled_precompute_rules[rule_name] for rule_name in used_rule_names)


def precompute_timeliness_rules(
    df: DataFrame,
    rule_pack_cfg: Dict[str, Any],
    registry: Optional[DQRuleRegistry] = None,
) -> tuple[DataFrame, List[str]]:
    """
    Materialize timeliness precompute rules as boolean columns before DQX runs.

    The presence of `precompute_rules` means precompute is enabled. Technical
    column names are derived from YC.TIMELINESS_PRECOMPUTE_COLUMN_PREFIX.
    """
    technical_columns: List[str] = []

    for matrix_cfg in rule_pack_cfg.get(YC.TIMELINESS_RULES_KEY, []) or []:
        precompute_rules = matrix_cfg.get(YC.PRECOMPUTE_RULES_KEY) or {}
        if not precompute_rules:
            continue

        default_column = (
            matrix_cfg.get(YC.ANCHOR_COLUMN_KEY)
            or matrix_cfg.get("column")
            or rule_pack_cfg.get(YC.ROW_ANCHOR_COLUMN_KEY)
            or (df.columns[0] if df.columns else None)
        )
        if default_column is None:
            raise ValueError("Timeliness precompute requires an anchor_column or a non-empty DataFrame.")

        seen_names = set()
        for rule_cfg in precompute_rules.get(YC.RULES_KEY, []) or []:
            name = _rule_name(rule_cfg)
            if not name:
                raise ValueError("Every timeliness precompute rule requires a 'name'.")
            if name in seen_names:
                raise ValueError(f"Duplicate timeliness precompute rule name: {name}")
            seen_names.add(name)

            materialized_column = _materialized_precompute_column(name)
            rule_cfg[YC.MATERIALIZED_COLUMN_KEY] = materialized_column

            if materialized_column in df.columns or materialized_column in technical_columns:
                continue

            df = df.withColumn(
                materialized_column,
                _rule_condition_from_make_condition(
                    rule_cfg=rule_cfg,
                    registry=registry,
                    default_column=default_column,
                ).cast("boolean"),
            )
            technical_columns.append(materialized_column)

    return df, technical_columns

def timeliness_matrix(
    column: str,
    *,
    registry: Optional[DQRuleRegistry] = None,
    precompute_rules: Optional[Dict[str, Any]] = None,
    timeliness_cases: Optional[List[Dict[str, Any]]] = None,
    emit: Optional[Dict[str, Any]] = None,
    alias: Optional[str] = None,
) -> Column:
    """
    Execute a timeliness matrix as one DQX row rule.

    The matrix does not implement its own field/op DSL. Both precompute rules
    and case rules are standard registered row rules returning make_condition().
    """
    emit = emit or {}
    match_policy = (emit.get("match_policy") or "first").lower()
    code_separator = emit.get("code_separator") or "|"

    compiler = TimelinessMatrixCompiler(
        anchor_column=column,
        registry=registry,
        precompute_rules=precompute_rules,
    )
    message = _message_from_matches(
        compiler.compile_case_matches(timeliness_cases or []),
        match_policy=match_policy,
        code_separator=code_separator,
    )

    return make_condition(
        condition=message.isNotNull(),
        message=message,
        alias=alias or f"{column}_timeliness_matrix",
    )
