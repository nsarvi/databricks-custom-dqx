from datetime import date, datetime, timezone
import json
from typing import Dict, List, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, ArrayType, MapType
from databricks.connect import DatabricksSession
from idea4.dqx import yaml_constants as YC
from pyspark.sql import Column
from idea4.dqx.dqx_rule_registry import DQRuleRegistry
from idea4.dqx.persistence import PersistenceManager
from idea4.dqx.utils.logging_utils import LoggingHandler
from idea4.dqx.utils.config_utils import ConfigUtils
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from pyspark.dbutils import DBUtils
from idea4.dqx.table_reader import TableReader


logger = LoggingHandler(__name__).get_logger()


class BaseDQXEngine:

    def __init__(self, rule_config_path: str, dqx_config_file: str = None):
        self.spark = self._get_spark()
        self.dbutils = self._get_dbutils(self.spark)
        self.ws = self._get_workspace_client()
        self.dq_engine = DQEngine(self.ws)

        # Load and merge configs
        self.dqx_config = self._load_and_merge_dqx_configs(dqx_config_file)
        self.registry = self._build_rule_registry(dqx_config_file)

        # Unified load (rule_packs + sources)
        self.all_configs = ConfigUtils.load_all_configs(rule_config_path)
        self.rule_packs_config = self.all_configs.get(YC.RULE_PACKS_KEY, {})

        # Rule pack lookup
        self.rule_pack_lookup = self._build_rule_pack_lookup()

        # table reader
        self.table_reader = TableReader(self.spark)

        # Load reference tables
        self.reference_dfs = self._load_reference_dfs()

        # Source map
        self.sources_map = self.all_configs.get(YC.SOURCES_KEY, {})

        # Persistence Manager
        self.persistence_manager = PersistenceManager(self.spark, self.dqx_config)
        logger.info("Context initialized successfully.")

    @staticmethod
    def _get_spark() -> SparkSession:
        try:
            return DatabricksSession.builder.getOrCreate()
        except ImportError as ir:
            logger.error(f"Error while importing Databricks Connect library {ir.__cause__}")
            return SparkSession.builder.getOrCreate()
        except Exception as e:
            logger.error(f"Error while getting SparkSession via Databricks Connect {e.__cause__}")
            return SparkSession.builder.getOrCreate()

    @staticmethod
    def _get_dbutils(spark: SparkSession) -> DBUtils:
        try:
            return DBUtils(spark)
        except ImportError as ir:
            logger.warning(
                f"Error while importing Databricks SDK library {ir.__cause__}. Runtime context will not be available"
            )
        except Exception as e:
            logger.warning(
                f"Error while getting DBUtils via Databricks SDK {e.__cause__}. Runtime context will not be available"
            )

    @staticmethod
    def _get_workspace_client() -> WorkspaceClient:
        """Get WorkspaceClient."""
        try:
            return WorkspaceClient()
        except ImportError as ir:
            logger.error(f"Error while importing Databricks SDK library {ir.__cause__}")
            raise
        except Exception as e:
            logger.error(f"Error while getting WorkspaceClient via Databricks SDK {e.__cause__}")
            raise

    def _build_rule_pack_lookup(self) -> dict:
        """Preprocess configs to lookup dictionary."""
        return {
            rule_packs[YC.ID_KEY]: rule_packs
            for rule_packs in self.rule_packs_config.get(YC.RULE_PACKS_KEY, [])
        }

    def _get_rule_pack_by_id(self, rule_pack_id: str) -> List[Dict[str, Any]]:
        """Get validations based on combine_id."""
        packs_map = self.rule_packs_config or {}
        rule_pack = packs_map.get(rule_pack_id)
        if not rule_pack:
            raise KeyError(f"Unknown rule_pack id: {rule_pack_id}")
        return rule_pack

    def _build_rule_registry(self, dqx_config_file: str = None) -> DQRuleRegistry:
        """Build DQRuleRegistry based on rule_mapping_file from dqx_config."""
        logger.info("Building DQRuleRegistry based on rule_mapping_file from dqx_config.")
        registry = DQRuleRegistry()
        default_dqx_config = ConfigUtils.load_dqx_config(YC.DEFAULT_DQX_CONFIG_FILE)
        framework_paths = default_dqx_config.get(YC.RULE_MAPPING_FILES_KEY, [])
        custom_rule_paths = []
        if dqx_config_file:
            custom_dqx_config = ConfigUtils.load_dqx_config(dqx_config_file)
            custom_rule_paths = custom_dqx_config.get(YC.RULE_MAPPING_FILES_KEY, [])

        # Ensure both are lists
        if not isinstance(framework_paths, list):
            framework_paths = []
        if not isinstance(custom_rule_paths, list):
            custom_rule_paths = []

        # Expecting rule_mapping.yaml to have a list under 'row_rules'
        ConfigUtils._build_registry(framework_paths, registry, False)
        ConfigUtils._build_registry(custom_rule_paths, registry, True)

        return registry

    def _load_and_merge_dqx_configs(self, dqx_config_file: str = None) -> Dict:
        """Loads the idea4_dqx_config.yaml file into two dicts, keyed by validation id."""
        config_file = dqx_config_file or YC.DEFAULT_DQX_CONFIG_FILE
        logger.info(f"Loading DQX config from {config_file}")
        default_dqx_config = ConfigUtils.load_dqx_config(YC.DEFAULT_DQX_CONFIG_FILE)
        custom_dqx_config = {}
        if dqx_config_file:
            logger.info(f"Custom DQX config provided: {dqx_config_file}")
            custom_dqx_config = ConfigUtils.load_dqx_config(dqx_config_file)
            return self._deep_merge_dicts(default_dqx_config, custom_dqx_config)
        else:
            return default_dqx_config

    def _deep_merge_dicts(self, base: dict, override: dict) -> dict:
        result = dict(base)  # shallow copy
        for key, value in override.items():
            if (
                key in result
                and isinstance(result[key], dict)
                and isinstance(value, dict)
            ):
                result[key] = self._deep_merge_dicts(result[key], value)
            else:
                result[key] = value
        return result

    def _load_reference_dfs(self) -> Dict[str, DataFrame]:
        """
        Loads the reference tables from the YAML file.
        """
        reference_dfs: Dict[str, DataFrame] = {}
        ref_cfg: Dict[str, Any] = (self.dqx_config or {}).get(YC.REFERENCE_TABLES_KEY, {}) or {}
        for ref_id, src in ref_cfg.items():
            try:
                df = self.table_reader.read_source_table(
                    {
                        # Map your idea4_dqx_config structure to TableReader's expected keys
                        YC.SOURCE_ID_KEY: ref_id,
                        YC.READ_TYPE_KEY: src.get(YC.READ_TYPE_KEY, YC.TABLE_KEY),
                        YC.TABLE_NAME_KEY: src.get(YC.TABLE_NAME_KEY),
                        YC.SUBQUERY_KEY: src.get(YC.SUBQUERY_KEY),
                        YC.SQL_FILE_KEY: src.get(YC.SQL_FILE_KEY),
                        YC.OPTIONS_KEY: src.get(YC.OPTIONS_KEY, {}),
                        # If you pre-process SQL file content elsewhere into SQL_FILE_TEXT_KEY, set it here too
                        YC.SQL_FILE_TEXT_KEY: src.get(YC.SQL_FILE_TEXT_KEY),
                    }
                )
                reference_dfs[ref_id] = df
            except Exception as e:
                logger.error(f"Failed to load reference table '{ref_id}': {e}")
        return reference_dfs

    def _errors_to_json_array(self, df: DataFrame, col_name: str) -> Column:
        if col_name not in df.columns:
            return F.array().cast("array<string>")
        col_data_type = df.schema[col_name].dataType

        # DQX typical: array<struct> or array<map>
        if isinstance(col_data_type, ArrayType) and (
            isinstance(col_data_type.elementType, StructType)
            or isinstance(col_data_type.elementType, MapType)
        ):
            return F.when(F.col(col_name).isNull(), F.array().cast("array<string>")) \
                .otherwise(F.transform(F.col(col_name), lambda e: F.to_json(e)).cast("array<string>"))

        # Already array<string>
        if isinstance(col_data_type, ArrayType):
            return F.col(col_name).cast("array<string>")

        # Scalar fallback: wrap as single-element array
        return F.when(F.col(col_name).isNull(), F.array().cast("array<string>")) \
            .otherwise(F.array(F.col(col_name).cast("string")))

    def _build_run_summary_df(
        self,
        table_key: str,
        dq_run_id: str,
        validation_id: str,
        runtime: dict,
        status: str,
        input_df: DataFrame,
        valid_df: DataFrame,
        quarantine_df: DataFrame,
        persist_counts: bool,
    ) -> DataFrame:
        in_cnt = v_cnt = q_cnt = None
        if persist_counts:
            in_cnt = input_df.count()
            v_cnt = valid_df.count()
            q_cnt = quarantine_df.count()

        row = [{
            "dq_run_id": dq_run_id,
            "validation_id": validation_id,
            "run_ts_utc": datetime.now(timezone.utc).replace(tzinfo=None),
            "status": status if q_cnt is None or q_cnt == 0 else YC.FAILURE_KEY,
            "job_id": runtime.get("job_id"),
            "run_id": runtime.get("run_id"),
            "task_run_id": runtime.get("task_run_id"),
            "task_key": runtime.get("task_key"),
            "notebook_path": runtime.get("notebook_path"),
            "cluster_id": runtime.get("cluster_id"),
            "user": runtime.get("user"),
            "input_row_count": in_cnt,
            "valid_row_count": v_cnt,
            "quarantine_row_count": q_cnt,
            "tags": json.dumps(runtime.get("tags", {})),
        }]

        return self.persistence_manager.create_dataframe_from_rows(table_key, row)

    def _build_quarantine_df(
        self,
        quarantine_df: DataFrame,
        dq_run_id: str,
        validation_id: str,
        runtime: dict,
    ) -> DataFrame:
        payload_cols = [c for c in quarantine_df.columns]

        out = (quarantine_df
            .withColumn("dq_run_id", F.lit(dq_run_id))
            .withColumn("validation_id", F.lit(validation_id))
            .withColumn("ingest_dt", F.lit(date.today()))
            .withColumn("job_id", F.lit(runtime.get("job_id")))
            .withColumn("run_id", F.lit(runtime.get("run_id")))
            .withColumn("task_run_id", F.lit(runtime.get("task_run_id")))
            .withColumn("task_key", F.lit(runtime.get("task_key")))
            .withColumn("notebook_path", F.lit(runtime.get("notebook_path")))
            .withColumn("cluster_id", F.lit(runtime.get("cluster_id")))
            .withColumn("user", F.lit(runtime.get("user")))
            .withColumn("_errors", self._errors_to_json_array(quarantine_df, "_errors"))
            .withColumn("_warnings", self._errors_to_json_array(quarantine_df, "_warnings"))
            .withColumn("payload_json", F.to_json(F.struct(*[F.col(c) for c in payload_cols])))
            .select(
                "dq_run_id", "validation_id", "ingest_dt",
                "job_id", "run_id", "task_run_id", "task_key", "notebook_path", "cluster_id", "user",
                "_errors", "_warnings", "payload_json"
            )
        )
        return out

    def _build_step_metadata_lookup(self, rule_pack_cfg: Dict[str, Any]) -> Dict[str, str]:
        """
        Build a driver-side lookup:
          { "<COLUMN>::<RULE_ID>" : "<json>" }

        JSON contains ONLY:
        - validation_message (from YAML 'message' if present, else "")
        - metadata           (the YAML 'metadata' map serialized to JSON, else "{}")
        """
        rule_pack_map: Dict[str, str] = {}
        cols = (rule_pack_cfg or {}).get(YC.COLUMNS_KEY, {}) or {}

        for col_name, payload in cols.items():
            rules = (payload or {}).get(YC.RULES_KEY, []) or []
            for rule in rules:
                rid = rule.get(YC.ID_KEY)
                if not rid:
                    continue

                # Minimal step metadata
                step_meta: Dict[str, Any] = {}

                # YAML message -> validation_message
                vm = rule.get(YC.MESSAGE_KEY)
                step_meta[YC.VALIDATION_MESSAGE_KEY] = vm if vm is not None else ""

                # YAML metadata block -> serialized JSON
                user_meta = rule.get(YC.METADATA_KEY) or {}
                step_meta[YC.METADATA_KEY] = user_meta  # keep as dict here; we'll json.dumps below

                rule_pack_map[f"{col_name}::{rid}"] = json.dumps(step_meta, ensure_ascii=False)

        logger.debug(
            "Built step-metadata (validation_message + metadata) lookup with %d entries for rule pack: %s",
            len(rule_pack_map), rule_pack_cfg.get("id")
        )
        return rule_pack_map

    def _enrich_errors_from_config(self, quarantine_df: DataFrame, rule_pack_cfg: Dict[str, Any]) -> DataFrame:
        """
        Enrich `_errors` in place (no new column, no composite changes).

        For each error element `x` with schema:
        name, message, columns, filter, function, run_time, run_id, user_metadata

        We compute the lookup key as:
            "<first_column>::<rule_id>"
        where:
            first_column = x.columns[0]
            rule_id      = x.message       # you emit rule_id as the message

        Then we fetch a minimal JSON blob (built by _build_step_metadata_lookup)
        that contains: { "validation_message": "<YAML message or ''>", "metadata": { ... } }
        and write its two parts directly into user_metadata:
            user_metadata["validation_message"] = <string>
            user_metadata["metadata"]           = <JSON string>

        If a lookup is missing, we default to "" and "{}" respectively.
        """

        # Driver-side lookup produced by your existing _build_step_metadata_lookup(...)
        lookup = self._build_step_metadata_lookup(rule_pack_cfg) or {}

        # Create a literal map<string,string> for the lookup (create_map(k1,v1,k2,v2,...))
        if lookup:
            kv = []
            for k, v in lookup.items():
                kv.extend([F.lit(k), F.lit(v)])
            meta_map_lit = F.create_map(*kv)
        else:
            meta_map_lit = F.create_map()  # empty map

        # Helpers
        empty_map = F.map_from_arrays(F.array(), F.array())
        e = F.col("_errors")

        # Build "<first_column>::<rule_id>" using message as rule_id
        def _lookup_json(x):
            return F.element_at(
                meta_map_lit,
                F.concat_ws(
                    "::",
                    F.element_at(x.getField("columns"), F.lit(1)),  # first column name; element_at is 1-based
                    x.getField("message")                           # rule_id (you emit rule_id as message)
                )
            )

        # Extract pieces from the minimal JSON:
        # - $.validation_message -> string
        # - $.metadata           -> JSON object (emit as JSON string)
        def _enriched_user_metadata(x):
            base = F.coalesce(
                x.getField("user_metadata").cast(MapType(StringType(), StringType())),
                empty_map
            )

            json_blob = _lookup_json(x)  # JSON string or NULL

            validation_message = F.coalesce(
                F.get_json_object(json_blob, "$.validation_message"),
                F.lit("")  # default
            )

            # metadata object as JSON string; parse to map then to_json to ensure "{}" when absent
            metadata_json = F.coalesce(
                F.to_json(
                    F.from_json(
                        F.get_json_object(json_blob, "$.metadata"),
                        MapType(StringType(), StringType())
                    )
                ),
                F.lit("{}")
            )

            return F.map_concat(
                base,
                F.create_map(
                    F.lit("validation_message"), validation_message,
                    F.lit("metadata"), metadata_json
                )
            )

        # Rebuild the struct with same fields (only user_metadata changes)
        new_errors = F.when(e.isNull(), e).otherwise(
            F.transform(
                e,
                lambda x: F.struct(
                    x.getField("name").alias("name"),
                    x.getField("message").alias("message"),
                    x.getField("columns").alias("columns"),
                    x.getField("filter").alias("filter"),
                    x.getField("function").alias("function"),
                    x.getField("run_time").alias("run_time"),
                    x.getField("run_id").alias("run_id"),
                    _enriched_user_metadata(x).alias("user_metadata"),
                )
            )
        )

        return quarantine_df.withColumn("_errors", new_errors)
