from __future__ import annotations
import inspect
import pyspark.sql.functions as F
from databricks.labs.dqx.check_funcs import make_condition
import importlib
from zkblock.dqx import yaml_constants as YC
from zkblock.dqx.dqx_rule_registry import DQRuleRegistry
from pyspark.sql import Column


def _resolve_row_rule(fn_name: str):
    """
    Resolve a row-rule function by short name:
    1) this module (custom rules you defined here)
    2) databricks.labs.dqx.check_funcs (built-ins)
    """

    this_mod = importlib.import_module(__name__)
    if hasattr(this_mod, fn_name):
        return getattr(this_mod, fn_name)

    dqx_mod = importlib.import_module("databricks.labs.dqx.check_funcs")
    if hasattr(dqx_mod, fn_name):
        return getattr(dqx_mod, fn_name)

    raise ValueError(f"Unknown row rule function: {fn_name}")


def fast_fail_rule_chain(
    column: str,
    chain: list[dict] | None = None,
    registry: DQRuleRegistry = None,
    alias: str | None = None
) -> Column:
    """
    Build: when(pred1, 'rule_id_1').when(pred2, 'rule_id_2') ... .otherwise(lit(None))
    where pred_i = rule_i(column, **kwargs).isNotNull()
    Emits ONLY the first failing rule's ID (string), else NULL.
    """
    if not chain:
        return make_condition(
            condition=F.lit(False),
            message=F.lit(None).cast("string"),
            alias=alias or f"{column}_fast_fail_rule_chain"
        )

    when_clause = None
    for step in chain:
        rid = step[YC.ID_KEY]
        fn = registry.get_row(step[YC.ID_KEY])
        kwargs = step.get(YC.KWARGS_KEY) or {}

        sig = inspect.signature(fn)
        fail_msg_or_null = None
        if "expression" in sig.parameters and "column" not in sig.parameters:
            fail_msg_or_null = fn(**kwargs)
        else:
            fail_msg_or_null = fn(column, **kwargs)

        predicate = fail_msg_or_null.isNotNull()
        step_filter = step.get(YC.FILTER_KEY)
        if step_filter:
            predicate = predicate & F.expr(step_filter)

        # Return the ID of the first failing rule
        rule_id = F.lit(step[YC.ID_KEY])
        when_clause = F.when(predicate, rule_id) if when_clause is None else when_clause.when(predicate, rule_id)

    first_fail_id = when_clause.otherwise(F.lit(None)).cast("string")

    # DQX-style: NULL = pass; 'rule_id' = first failing rule id
    return make_condition(
        condition=first_fail_id.isNotNull(),
        message=first_fail_id,
        alias=alias or f"{column}_fast_fail_rule_chain"
    )
