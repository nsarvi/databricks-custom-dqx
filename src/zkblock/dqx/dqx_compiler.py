from __future__ import annotations
from typing import Any, Dict, List, Union

from databricks.labs.dqx.rule import DQRowRule, DQDatasetRule
from idea4.dqx.dqx_rule_registry import DQRuleRegistry
from idea4.dqx import yaml_constants as YC
from databricks.labs.dqx.rule import DQRule
from idea4.dqx.utils.logging_utils import LoggingHandler

# DQRuleT = Union[DQRowRule, DQDatasetRule]

from typing import Dict, Any, List, Optional, Iterable
import logging

logger = LoggingHandler(__name__).get_logger()


def compile_rule_pack_to_dqx_rules(
    rule_pack_cfg: Dict[str, Any],
    registry: DQRuleRegistry,
    df_columns: Optional[Iterable[str]] = None,
    case_insensitive: bool = False,
) -> List[DQRule]:

    rules: List[DQRule] = []

    columns_cfg = rule_pack_cfg.get(YC.COLUMNS_KEY, {})
    fast_fail = bool(rule_pack_cfg.get(YC.FAST_FAIL_KEY, False))
    logger.debug("Fast fail mode: %s", fast_fail)

    # Build a lookup for column existence if df_columns is provided
    if df_columns is not None:
        if case_insensitive:
            df_col_map = {c.lower(): c for c in df_columns}
            def col_exists(col: str) -> bool:
                return col.lower() in df_col_map
        else:
            df_col_set = set(df_columns)
            def col_exists(col: str) -> bool:
                return col in df_col_set
    else:
        # If df_columns not provided, treat all columns as allowed
        def col_exists(col: str) -> bool:
            return True

    for col_name, col_payload in columns_cfg.items():
        # If we are DF-aware, skip columns not present
        if df_columns is not None and not col_exists(col_name):
            logger.warning(
                f"Skipping all rules for column {col_name} because it does not exist in the DataFrame."
            )
            continue

        col_rules = (col_payload or {}).get(YC.RULES_KEY, [])

        if fast_fail:
            logger.debug("Building fast fail rule chain for column %s", col_name)
            chain: List[Dict[str, Any]] = []
            crits: List[str] = []
            rule_order: List[str] = []
            chain_metadata: Dict[str, Any] = {}
            for rule_cfg in col_rules:
                kwargs = dict(rule_cfg.get(YC.ARGUMENTS_KEY, {}) or {})
                rid = rule_cfg[YC.ID_KEY]
                # add filter
                r_filter = rule_cfg.get(YC.FILTER_KEY, None)
                r_metadata = dict(rule_cfg.get(YC.METADATA_KEY, {}) or {})
                r_yaml_message = rule_cfg.get(YC.MESSAGE_KEY, "")
                if r_yaml_message:
                    # Keep parity with normal flow
                    r_metadata[YC.VALIDATION_MESSAGE_KEY] = r_yaml_message

                chain.append({YC.ID_KEY: rid, YC.KWARGS_KEY: kwargs, YC.FILTER_KEY: r_filter})
                rule_order.append(rid)
                chain_metadata[rid] = r_metadata
                crit = (rule_cfg.get(YC.CRITICALITY_KEY, "error") or "error").lower()
                crits.append(crit)

            if not chain:
                continue
            chain_user_metadata = {
                "fast_fail": True,
                "column": col_name,
                "rule_order": rule_order,
                "chain_metadata": chain_metadata,  # { rid -> metadata }
            }
            agg_crit = "error" if "error" in crits else ("warn" if "warn" in crits else "info")

            rules.append(
                DQRowRule(
                    column=col_name,
                    check_func=registry.get_row(YC.FAST_FAIL_RULE_CHAIN_KEY),
                    name=YC.FAST_FAIL_RULE_CHAIN_KEY,
                    criticality=agg_crit,
                    check_func_args=[],
                    check_func_kwargs={
                        "chain": chain,
                        "registry": registry,
                        "alias": f"{col_name}_first_fail",
                    },
                    user_metadata=None,
                )
            )
        else:
            for rule_cfg in col_rules:
                yaml_user_metadata = rule_cfg.get(YC.METADATA_KEY, {})
                validation_message = rule_cfg.get(YC.MESSAGE_KEY, "")
                if validation_message:
                    user_metadata = dict(yaml_user_metadata)
                    user_metadata[YC.VALIDATION_MESSAGE_KEY] = validation_message
                else:
                    user_metadata = yaml_user_metadata

                rules.append(
                    DQRowRule(
                        column=col_name,
                        check_func=registry.get_row(rule_cfg[YC.ID_KEY]),
                        name=rule_cfg[YC.ID_KEY],
                        criticality=(rule_cfg.get(YC.CRITICALITY_KEY, "error") or "error").lower(),
                        filter=rule_cfg.get(YC.FILTER_KEY, None),
                        check_func_args=[],
                        check_func_kwargs=dict(rule_cfg.get(YC.ARGUMENTS_KEY, {}) or {}),
                        user_metadata=user_metadata
                    )
                )

    # Construct dataset-level rules
    ds_rules_cfg = rule_pack_cfg.get(YC.DATASET_RULES_KEY, [])
    for rule_cfg in ds_rules_cfg:
        rid = rule_cfg[YC.ID_KEY]
        # metadata + message normalization
        yaml_user_metadata = rule_cfg.get(YC.METADATA_KEY, {}) or {}
        validation_message = rule_cfg.get(YC.MESSAGE_KEY, "") or ""
        if validation_message:
            user_metadata = dict(yaml_user_metadata)
            user_metadata[YC.VALIDATION_MESSAGE_KEY] = validation_message
        else:
            user_metadata = yaml_user_metadata

        rules.append(
            DQDatasetRule(
                check_func=registry.get_dataset(rid),  # short ID or dotted path
                name=rid,
                criticality=(rule_cfg.get(YC.CRITICALITY_KEY, "error") or "error").lower(),
                filter=rule_cfg.get(YC.FILTER_KEY, None),
                check_func_args=[],
                check_func_kwargs=dict(rule_cfg.get(YC.ARGUMENTS_KEY, {}) or {}),
                user_metadata=user_metadata,
            )
        )

    return rules
