# src/humana/validation/dq_compiler.py

from __future__ import annotations
from typing import Any, Dict, List, Union

from databricks.labs.dqx.rule import DQRowRule, DQDatasetRule
from humana.validation.dq_rule_registry import DQRuleRegistry

DQRuleT = Union[DQRowRule, DQDatasetRule]


def compile_validation_to_dqx_rules(
    validation_cfg: Dict[str, Any],
    registry: DQRuleRegistry,
) -> List[DQRuleT]:
    rules: List[DQRuleT] = []

    # -------------------------
    # Column (per-column) rules -> DQRowRule(column=...)
    # -------------------------
    columns_cfg = validation_cfg.get("columns", {}) or {}

    for col_name, col_payload in columns_cfg.items():
        col_rules = (col_payload or {}).get("rules", []) or []
        for rule_cfg in col_rules:
            rules.append(
                DQRowRule(
                    column=col_name,
                    check_func=registry.get_row(rule_cfg["id"]),
                    name=rule_cfg["id"],
                    criticality=(rule_cfg.get("criticality", "error") or "error").lower(),
                    filter=rule_cfg.get("filter"),
                    check_func_args=[],
                    check_func_kwargs=dict(rule_cfg.get("arguments", {}) or {}),
                )
            )

    # -------------------------
    # Dataset rules -> DQDatasetRule
    # -------------------------
    dataset_rules_cfg = validation_cfg.get("dataset_rules", []) or []
    for rule_cfg in dataset_rules_cfg:
        rules.append(
            DQDatasetRule(
                check_func=registry.get_dataset(rule_cfg["id"]),
                name=rule_cfg["id"],
                criticality=(rule_cfg.get("criticality", "error") or "error").lower(),
                filter=rule_cfg.get("filter"),
                check_func_args=[],
                check_func_kwargs=dict(rule_cfg.get("arguments", {}) or {}),
            )
        )

    return rules
