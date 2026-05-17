from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from idea4.dqx import yaml_constants as YC
from idea4.dqx.utils.config_utils import ConfigUtils
from idea4.dqx.utils.logging_utils import LoggingHandler


logger = LoggingHandler(__name__).get_logger()


@dataclass(frozen=True)
class TableSpec:
    key: str
    full_name: str
    schema_file: str
    format: str = "delta"
    mode: str = "append"
    partitioned_by: Optional[List[str]] = None
    options: Optional[Dict[str, str]] = None


class PersistenceManager :

    def __init__(self, spark: SparkSession, idea4_config: Dict[str, Any]):
        self.logger = logger
        self.spark = spark
        self.idea_cfg = idea4_config or {}
        self.persistence_cfg = (self.idea_cfg.get(YC.DQX_METRICS_PERSISTENCE_KEY) or {})
        self.enabled = bool(self.persistence_cfg.get(YC.ENABLED_KEY, True))
        self.tables_cfg = (self.persistence_cfg.get(YC.TABLES_KEY) or {})
        self.schema_file_map: Dict[str, StructType] = {}
        self.metrics_table_map: Dict[str, TableSpec] = self._load_metrics_tables_map()

    def _load_metrics_tables_map(self) -> Dict[str, TableSpec]:
        metrics_table_specs: Dict[str, TableSpec] = {}
        for key, t in self.tables_cfg.items():
            if t is None or t.get(YC.ENABLED_KEY) is False:
                continue
            if YC.NAME_KEY not in t or YC.SCHEMA_FILE_KEY not in t:
                self.logger.debug(f"persistence.tables.{key} is missing 'name' and/or 'schema_file'.")
                raise ValueError(f"persistence.tables.{key} must have 'name' and 'schema_file'")
            schema_file = t.get(YC.SCHEMA_FILE_KEY)
            metrics_table_specs[key] = TableSpec(
                key=key,
                full_name=t[YC.NAME_KEY],
                schema_file=schema_file,
                format=(t.get(YC.FORMAT_KEY) or YC.TABLE_DELTA_KEY).lower(),
                mode=(t.get(YC.TABLE_MODE_KEY) or YC.TABLE_DELTA_MODE_APPEND).lower(),
                partitioned_by=t.get(YC.PARTITION_BY_KEY),
                options=t.get(YC.OPTIONS_KEY),
            )
            # Add schema_file to schema_file_map
            self.schema_file_map[schema_file] = ConfigUtils.get_schema(schema_file)

        return metrics_table_specs

    def _create_table(self, table_key: str):
        # Lookup the TableSpec object
        spec = self.metrics_table_map.get(table_key)
        if not spec:
            self.logger.debug(f"No configuration found for table_id: {table_key}")
            raise ValueError(f"Configuration not found for table_id: {table_key}")

        table_name = spec.full_name
        if not table_name:
            self.logger.debug(f"Table name not found in configuration for table_id: {table_key}")
            raise ValueError(f"Table name not found in configuration for table_id: {table_key}")

        # Check if table exists
        table_exists = self.spark.catalog.tableExists(table_name)
        if not table_exists:
            schema_file_path = spec.schema_file
            if not schema_file_path:
                self.logger.info(f"Table {table_name} does not exist. Proceeding to create for table_id: {table_key}")
                raise ValueError(f"Schema file not specified for table {table_name}.")

            self.logger.info(f"Creating table {table_name} as it does not exist.")
            # Load schema JSON from file
            schema = ConfigUtils.get_schema(schema_file_path)
            # Create empty DataFrame with schema
            df = self.spark.createDataFrame([], schema)

            # Write DataFrame to create table
            writer = df.write \
                .format(spec.format or YC.TABLE_DELTA_KEY) \
                .mode(spec.mode or YC.TABLE_DELTA_MODE_APPEND) \
                .options(**spec.options if hasattr(spec, YC.OPTIONS_KEY) else {})
            partition_by = spec.partitioned_by or []

            if partition_by:
                writer = writer.partitionBy(*partition_by)

            writer.saveAsTable(table_name)
            self.logger.info(f"Table {table_name} created successfully.")

    def ensure_table(self, table_key: str, table_spec: TableSpec):
        if not self.spark.catalog.tableExists(table_spec.full_name):
            self._create_table(table_key)

    def create_dataframe_from_rows(self, table_key: str,rows: List[List[Any]]) -> DataFrame:
        # Load the schema JSON from the file
        table_spec = self.metrics_table_map.get(table_key)
        schema = self.schema_file_map.get(table_spec.schema_file)
        # Get the schema field order
        field_order = [field.name for field in schema.fields]
        # Map each row list into a tuple respecting schema order
        data_tuples = [
            tuple(row.get(field, None) for field in field_order)
            for row in rows
        ]
        # Create DataFrame
        return self.spark.createDataFrame(data_tuples, schema)

    def write_to_table(self, table_key: str, df: DataFrame) -> None:
        if not self.enabled:
            return
        table_spec = self.metrics_table_map.get(table_key)
        if not table_spec:
            self.logger.debug(f"Ignored attempt to write to table {table_key} when table_key is not present in metrics_table_map")
            return
        self.ensure_table(table_key,table_spec)
        df.write.mode(table_spec.mode).saveAsTable(table_spec.full_name)
