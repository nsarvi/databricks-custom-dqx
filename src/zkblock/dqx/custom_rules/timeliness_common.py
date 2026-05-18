from __future__ import annotations

from typing import Any, Dict, Optional

from databricks.labs.dqx.check_funcs import make_condition
from pyspark.sql import Column

from zkblock.dqx.custom_rules.timeliness import PredicateCompiler, TimelinessContext


def _predicate(
    *,
    columns: Dict[str, str],
    rule: Dict[str, Any],
    date_parsing: Optional[Dict[str, Any]] = None,
) -> Column:
    ctx = TimelinessContext.from_config(columns, date_parsing)
    compiler = PredicateCompiler(ctx)
    return compiler.common_rule(rule)


def _common_check(
    column: str,
    *,
    columns: Dict[str, str],
    rule: Dict[str, Any],
    date_parsing: Optional[Dict[str, Any]] = None,
    message: Optional[str] = None,
    alias: Optional[str] = None,
) -> Column:
    passed = _predicate(columns=columns, rule=rule, date_parsing=date_parsing)
    rule_name = alias or f"{column}_timeliness_common"
    return make_condition(
        condition=~passed,
        message=message or rule_name,
        alias=rule_name,
    )


def standard_non_part_b_approval_denial(
    column: str,
    *,
    columns: Dict[str, str],
    date_parsing: Optional[Dict[str, Any]] = None,
    message: Optional[str] = None,
    alias: Optional[str] = None,
) -> Column:
    return _common_check(
        column,
        columns=columns,
        date_parsing=date_parsing,
        message=message,
        alias=alias or "standard_non_part_b_approval_denial",
        rule={
            "all": [
                {"field": "primary_part_b", "op": "eq", "value": "N"},
                {"field": "processing_priority", "op": "eq", "value": "S"},
                {"field": "disposition", "op": "in", "values": ["F", "P", "A"]},
            ]
        },
    )


def no_extension(
    column: str,
    *,
    columns: Dict[str, str],
    date_parsing: Optional[Dict[str, Any]] = None,
    message: Optional[str] = None,
    alias: Optional[str] = None,
) -> Column:
    return _common_check(
        column,
        columns=columns,
        date_parsing=date_parsing,
        message=message,
        alias=alias or "no_extension",
        rule={"all": [{"field": "extension_taken", "op": "eq", "value": "N"}]},
    )


def with_extension(
    column: str,
    *,
    columns: Dict[str, str],
    date_parsing: Optional[Dict[str, Any]] = None,
    message: Optional[str] = None,
    alias: Optional[str] = None,
) -> Column:
    return _common_check(
        column,
        columns=columns,
        date_parsing=date_parsing,
        message=message,
        alias=alias or "with_extension",
        rule={"all": [{"field": "extension_taken", "op": "eq", "value": "Y"}]},
    )


def request_and_verbal_valid(
    column: str,
    *,
    columns: Dict[str, str],
    date_parsing: Optional[Dict[str, Any]] = None,
    message: Optional[str] = None,
    alias: Optional[str] = None,
) -> Column:
    return _common_check(
        column,
        columns=columns,
        date_parsing=date_parsing,
        message=message,
        alias=alias or "request_and_verbal_valid",
        rule={"valid_dates": ["request_received_date", "verbal_notification_date"]},
    )


def aor_valid(
    column: str,
    *,
    columns: Dict[str, str],
    date_parsing: Optional[Dict[str, Any]] = None,
    message: Optional[str] = None,
    alias: Optional[str] = None,
) -> Column:
    return _common_check(
        column,
        columns=columns,
        date_parsing=date_parsing,
        message=message,
        alias=alias or "aor_valid",
        rule={"valid_dates": ["aor_received_date"]},
    )


def aor_na(
    column: str,
    *,
    columns: Dict[str, str],
    date_parsing: Optional[Dict[str, Any]] = None,
    message: Optional[str] = None,
    alias: Optional[str] = None,
) -> Column:
    return _common_check(
        column,
        columns=columns,
        date_parsing=date_parsing,
        message=message,
        alias=alias or "aor_na",
        rule={"all": [{"field": "aor_received_date", "op": "eq_ci", "value": "NA"}]},
    )


def written_valid(
    column: str,
    *,
    columns: Dict[str, str],
    date_parsing: Optional[Dict[str, Any]] = None,
    message: Optional[str] = None,
    alias: Optional[str] = None,
) -> Column:
    return _common_check(
        column,
        columns=columns,
        date_parsing=date_parsing,
        message=message,
        alias=alias or "written_valid",
        rule={"valid_dates": ["written_notification_date"]},
    )


def written_na(
    column: str,
    *,
    columns: Dict[str, str],
    date_parsing: Optional[Dict[str, Any]] = None,
    message: Optional[str] = None,
    alias: Optional[str] = None,
) -> Column:
    return _common_check(
        column,
        columns=columns,
        date_parsing=date_parsing,
        message=message,
        alias=alias or "written_na",
        rule={"all": [{"field": "written_notification_date", "op": "eq_ci", "value": "NA"}]},
    )


def verbal_before_written(
    column: str,
    *,
    columns: Dict[str, str],
    date_parsing: Optional[Dict[str, Any]] = None,
    message: Optional[str] = None,
    alias: Optional[str] = None,
) -> Column:
    return _common_check(
        column,
        columns=columns,
        date_parsing=date_parsing,
        message=message,
        alias=alias or "verbal_before_written",
        rule={
            "date_order": {
                "before": "verbal_notification_date",
                "after": "written_notification_date",
            }
        },
    )
