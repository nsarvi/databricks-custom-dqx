from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


@dataclass(frozen=True)
class TableSpec:
    key: str
    full_name: str
    schema_file: str
    format: str = "delta"
    mode: str = "append"
    partitioned_by: Optional[List[str]] = None


class UCPersistenceManager:

    from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType, MapType

def errors_to_json_array(df):
    if "_errors" not in df.columns:
        return F.array().cast("array<string>")

    dt = df.schema["_errors"].dataType

    # DQX typical: array<struct> or array<map>
    if isinstance(dt, ArrayType) and (isinstance(dt.elementType, StructType) or isinstance(dt.elementType, MapType)):
        return F.when(F.col("_errors").isNull(), F.array().cast("array<string>")) \
                .otherwise(F.transform(F.col("_errors"), lambda e: F.to_json(e)).cast("array<string>"))

    # Already array<string>
    if isinstance(dt, ArrayType):
        return F.col("_errors").cast("array<string>")

    # Scalar fallback (rare): wrap as single-element array
    return F.when(F.col("_errors").isNull(), F.array().cast("array<string>")) \
            .otherwise(F.array(F.col("_errors").cast("string")))


    def __init__(self, spark: SparkSession, config_utils, idea_cfg: Dict[str, Any], base_path: str):
        self.spark = spark
        self.config_utils = config_utils
        self.idea_cfg = idea_cfg or {}
        self.base_path = base_path.rstrip("/")

        self.persistence_cfg = (self.idea_cfg.get("persistence") or {})
        self.enabled = bool(self.persistence_cfg.get("enabled", True))
        self.tables_cfg = (self.persistence_cfg.get("tables") or {})

        self._specs: Dict[str, TableSpec] = self._load_specs()

    def _load_specs(self) -> Dict[str, TableSpec]:
        specs: Dict[str, TableSpec] = {}
        for key, t in self.tables_cfg.items():
            if t is None or t.get("enabled") is False:
                continue
            if "name" not in t or "schema_file" not in t:
                raise ValueError(f"persistence.tables.{key} must have 'name' and 'schema_file'")

            specs[key] = TableSpec(
                key=key,
                full_name=t["name"],
                schema_file=t["schema_file"],
                format=(t.get("format") or "delta").lower(),
                mode=(t.get("mode") or "append").lower(),
                partitioned_by=t.get("partitioned_by"),
            )
        return specs

    def _resolve_path(self, maybe_relative: str) -> str:
        if maybe_relative.startswith("/") or "://" in maybe_relative:
            return maybe_relative
        return f"{self.base_path}/{maybe_relative}"

    def _load_structtype(self, schema_path: str) -> StructType:
        schema_json = self.config_utils.read_json(schema_path)
        return StructType.fromJson(schema_json)

    def ensure_table(self, table_key: str) -> None:
        if not self.enabled:
            return
        spec = self._specs.get(table_key)
        if not spec:
            return

        schema_path = self._resolve_path(spec.schema_file)
        st = self._load_structtype(schema_path)

        ddl_cols = []
        for f in st.fields:
            dt = getattr(f.dataType, "catalogString", f.dataType.simpleString())
            not_null = "" if f.nullable else " NOT NULL"
            ddl_cols.append(f"`{f.name}` {dt}{not_null}")

        cols_ddl = ",\n  ".join(ddl_cols)

        part_ddl = ""
        if spec.partitioned_by:
            part_ddl = f"\nPARTITIONED BY ({', '.join([f'`{c}`' for c in spec.partitioned_by])})"

        sql = f"""
CREATE TABLE IF NOT EXISTS {spec.full_name} (
  {cols_ddl}
)
USING {spec.format}{part_ddl}
"""
        self.spark.sql(sql)

    def ensure_all_tables(self) -> None:
        if not self.enabled:
            return
        for key in self._specs.keys():
            self.ensure_table(key)

    def append(self, table_key: str, df: DataFrame) -> None:
        if not self.enabled:
            return
        spec = self._specs.get(table_key)
        if not spec:
            return
        self.ensure_table(table_key)
        df.write.mode(spec.mode).saveAsTable(spec.full_name)
