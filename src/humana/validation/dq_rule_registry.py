from __future__ import annotations
import importlib, inspect
from typing import Any, Callable, Dict, List, Optional, Tuple
from pyspark.sql import Column, DataFrame
RowRuleFunc = Callable[..., Column]
DatasetApplyFn = Callable[[DataFrame, Optional[Dict[str, DataFrame]]], DataFrame]
DatasetRuleFunc = Callable[..., Tuple[str, DatasetApplyFn]]
class DQRuleRegistry:
    _ROW_BUILTIN_MODULES = ["databricks.labs.dqx.check_funcs"]
    def __init__(self) -> None:
        self._row: Dict[str, RowRuleFunc] = {}
        self._dataset: Dict[str, DatasetRuleFunc] = {}
    def register_row(self, rule_id: str, fn: RowRuleFunc) -> None:
        self._row[rule_id] = fn
    def register_dataset(self, rule_id: str, fn: DatasetRuleFunc) -> None:
        self._dataset[rule_id] = fn
    def _import_attr(self, dotted: str):
        mod_path, attr = dotted.rsplit(".", 1)
        return getattr(importlib.import_module(mod_path), attr)
    def _resolve_from_modules(self, name: str, modules: List[str]):
        for m in modules:
            try:
                mod = importlib.import_module(m)
                if hasattr(mod, name):
                    return getattr(mod, name)
            except Exception:
                pass
        return None
    def get_row(self, rule_id: str) -> RowRuleFunc:
        if rule_id in self._row:
            return self._row[rule_id]
        if "." not in rule_id:
            fn = self._resolve_from_modules(rule_id, self._ROW_BUILTIN_MODULES)
            if fn:
                return fn
        if "." in rule_id:
            return self._import_attr(rule_id)
        raise KeyError(f"Unknown row rule '{rule_id}'")
    def get_dataset(self, rule_id: str) -> DatasetRuleFunc:
        if rule_id in self._dataset:
            return self._dataset[rule_id]
        if "." in rule_id:
            return self._import_attr(rule_id)
        raise KeyError(f"Unknown dataset rule '{rule_id}'")
    def list_row_rules(self, fully_qualified: bool = False) -> List[Dict[str, Any]]:
        items=[]
        for name, fn in sorted(self._row.items()):
            items.append({"id": name, "source":"custom","module":fn.__module__,"signature":str(inspect.signature(fn))})
        mod = importlib.import_module("databricks.labs.dqx.check_funcs")
        for name, fn in vars(mod).items():
            if name.startswith("_") or not inspect.isfunction(fn):
                continue
            rid = f"{mod.__name__}.{name}" if fully_qualified else name
            items.append({"id":rid,"source":"dqx","module":mod.__name__,"signature":str(inspect.signature(fn))})
        dedup={}
        for it in items:
            if it["id"] not in dedup:
                dedup[it["id"]] = it
        return list(dedup.values())
