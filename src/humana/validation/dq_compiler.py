from __future__ import annotations
from typing import Any, Dict, List, Union
from databricks.labs.dqx.rule import DQColRule, DQDatasetRule
from humana.validation.dq_rule_registry import DQRuleRegistry
DQRuleT = Union[DQColRule, DQDatasetRule]
def compile_validation_to_dqx_rules(validation_cfg: Dict[str, Any], registry: DQRuleRegistry) -> List[DQRuleT]:
    out=[]
    cols = validation_cfg.get("columns", {}) or {}
    for col_name, col_payload in cols.items():
        for r in (col_payload or {}).get("rules", []) or []:
            out.append(DQColRule(
                check_func=registry.get_row(r["id"]),
                col_name=col_name,
                name=r["id"],
                criticality=(r.get("criticality","error") or "error").lower(),
                filter=r.get("filter"),
                check_func_args=[],
                check_func_kwargs=dict(r.get("arguments",{}) or {}),
            ))
    for r in validation_cfg.get("dataset_rules", []) or []:
        out.append(DQDatasetRule(
            check_func=registry.get_dataset(r["id"]),
            name=r["id"],
            criticality=(r.get("criticality","error") or "error").lower(),
            filter=r.get("filter"),
            check_func_args=[],
            check_func_kwargs=dict(r.get("arguments",{}) or {}),
        ))
    return out
