
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any


@dataclass(frozen=True)
class RuleSpec:
    id: str
    criticality: str = "error"
    arguments: Dict[str, Any] = field(default_factory=dict)
    filter: Optional[str] = None


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    rules: List[RuleSpec]


@dataclass(frozen=True)
class ValidationSpec:
    id: str
    table_name: Optional[str]
    columns: List[ColumnSpec]
    dataset_rules: List[RuleSpec] = field(default_factory=list)
