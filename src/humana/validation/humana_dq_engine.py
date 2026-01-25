from __future__ import annotations

import json
import uuid
from datetime import date, datetime, timezone
from typing import Any, Dict, Optional, Tuple, List

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

from adm.integration.base_integration import BaseIntegration
from adm.utils.config_utils import ConfigUtils

from idea4.dqx.runtime_context import get_runtime_context
from idea4.dqx.dq_rule_registry import DQRuleRegistry
from idea4.dqx.rule_mapping_loader import build_registry_from_mapping
from idea4.dqx.dq_compiler import compile_validation_to_dqx_rules
from idea4.dqx.persistence.uc_persistence import UCPersistenceManager


class HumanaDQXEngine:
    DEFAULT_IDEA_CFG = "idea_dqx_config.yaml"
    DEFAULT_RULE_MAPPING = "rule_mapping.yaml"

    def __init__(self, validation_config_path: str, dqx_config_file: Optional[str] = None):
        """
        validation_config_path: folder containing:
          - idea_dqx_config.yaml
          - validation YAML files (or subfolder)
          - schemas/*.json
          - optional rule_mapping.yaml (unless dqx_config_file provided)

        dqx_config_file: optional explicit path to rule_mapping.yaml
        """
        self.validation_config_path = validation_config_path.rstrip("/")
        self.dqx_config_file = dqx_config_file  # mapping file (optional)

        # internal defaults (caller sees none of this)
        self.integration = BaseIntegration()
        self.config_utils = ConfigUtils()

        self.spark = self.integration.spark
        self.dbutils = getattr(self.integration, "dbutils", None)

        self.ws = WorkspaceClient()
        self.dq_engine = DQEngine(self.ws)

        # load idea config (persistence)
        self.idea_cfg = self.config_utils.read_yaml(f"{self.validation_config_path}/{self.DEFAULT_IDEA_CFG}")

        # persistence manager auto-creates tables
        self.persistence = UCPersistenceManager(
            spark=self.spark,
            config_utils=self.config_utils,
            idea_cfg=self.idea_cfg,
            base_path=self.validation_config_path,
        )
        self.persistence.ensure_all_tables()

        # registry (custom mapping optional + built-in DQX fallback always)
        self.registry = self._build_registry()

        # load all validations
        self._validation_index: Dict[str, Dict[str, Any]] = {}
        self._load_validations()

    def _build_registry(self) -> DQRuleRegistry:
        # always start with an empty registry (built-ins resolved at lookup time)
        reg = DQRuleRegistry()

        mapping_path = self.dqx_config_file or f"{self.validation_config_path}/{self.DEFAULT_RULE_MAPPING}"
        try:
            mapping_cfg = self.config_utils.read_yaml(mapping_path)
            mapped = build_registry_from_mapping(mapping_cfg)
            # merge into reg
            for k, fn in mapped._row.items():      # internal dicts are OK for this simple merge
                reg.register_row(k, fn)
            for k, fn in mapped._dataset.items():
                reg.register_dataset(k, fn)
        except Exception:
            # mapping is optional by your requirement
            pass

        return reg

    def _load_validations(self) -> None:
        """
        Reads all YAML configs under validation_config_path.
        If you keep validations under validation_config_path/validations, it will also work
        as long as your ConfigUtils.read_all_yaml reads recursively or you point it there.
        """
        cfgs = self.config_utils.read_all_yaml(self.validation_config_path)
        if isinstance(cfgs, dict):
            cfgs = [cfgs]

        for cfg in cfgs:
            for v in (cfg.get("validations") or []):
                vid = v["id"]
                if vid in self._validation_index:
                    raise ValueError(f"Duplicate validation id '{vid}'")
                self._validation_index[vid] = v

    def _get_validation(self, validation_id: str) -> Dict[str, Any]:
        if validation_id not in self._validation_index:
            raise KeyError(f"Validation id not found: {validation_id}")
        return self._validation_index[validation_id]

    def _resolve_df(self, validation_cfg: Dict[str, Any], df: Optional[DataFrame]) -> DataFrame:
        if df is not None:
            return df

        # optional: allow validation YAML to specify a table to read
        table_name = validation_cfg.get("table_name")
        if not table_name:
            raise ValueError("df not provided and validation_cfg.table_name not set")
        return self.spark.table(table_name)

    # ----------------------------
    # Main entrypoint
    # ----------------------------
    def validate(
        self,
        validation_id: str,
        df: Optional[DataFrame] = None,
        phase_id: Optional[str] = None,
        persist_counts: bool = True,
    ) -> Tuple[DataFrame, DataFrame, DataFrame]:
        """
        Returns: (valid_df, quarantine_df, summary_df)
        Also persists summary/quarantine(+optional rule_failures) to UC tables if enabled in idea_dqx_config.yaml.
        """
        vcfg = self._get_validation(validation_id)
        input_df = self._resolve_df(vcfg, df)

        dq_run_id = str(uuid.uuid4())
        rt = get_runtime_context(self.dbutils)

        dq_rules = compile_validation_to_dqx_rules(vcfg, self.registry)
        valid_df, quarantine_df = self.dq_engine.apply_checks_and_split(input_df, dq_rules)

        # build outputs for persistence
        summary_out = self._build_run_summary_df(
            dq_run_id=dq_run_id,
            validation_id=validation_id,
            runtime=rt,
            status="SUCCESS",
            input_df=input_df,
            valid_df=valid_df,
            quarantine_df=quarantine_df,
            persist_counts=persist_counts,
        )

        quarantine_out = self._build_quarantine_df(
            quarantine_df=quarantine_df,
            dq_run_id=dq_run_id,
            validation_id=validation_id,
            runtime=rt,
            phase_id=phase_id,
        )

        # persist
        self.persistence.append("run_summary", summary_out)
        self.persistence.append("quarantine", quarantine_out)

        # optional rule_failures
        rf_cfg = ((self.idea_cfg.get("persistence") or {}).get("tables") or {}).get("rule_failures") or {}
        if rf_cfg.get("enabled", False):
            rf_out = self._build_rule_failures_df(quarantine_df, dq_run_id, validation_id)
            self.persistence.append("rule_failures", rf_out)

        return valid_df, quarantine_df, summary_out

    # ----------------------------
    # Output DF builders
    # ----------------------------
    def _build_run_summary_df(
        self,
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
            "status": status,

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

            "metadata_json": json.dumps(runtime.get("tags", {})),
        }]
        return self.spark.createDataFrame(row)

    def _build_quarantine_df(
        self,
        quarantine_df: DataFrame,
        dq_run_id: str,
        validation_id: str,
        runtime: dict,
        phase_id: Optional[str],
    ) -> DataFrame:
        payload_cols = [c for c in quarantine_df.columns if c not in ("_errors", "_warnings")]

        out = (quarantine_df
            .withColumn("dq_run_id", F.lit(dq_run_id))
            .withColumn("validation_id", F.lit(validation_id))
            .withColumn("phase_id", F.lit(phase_id))
            .withColumn("ingest_dt", F.lit(date.today()))
            .withColumn("job_id", F.lit(runtime.get("job_id")))
            .withColumn("run_id", F.lit(runtime.get("run_id")))
            .withColumn("task_run_id", F.lit(runtime.get("task_run_id")))
            .withColumn("task_key", F.lit(runtime.get("task_key")))
            .withColumn("notebook_path", F.lit(runtime.get("notebook_path")))
            .withColumn("cluster_id", F.lit(runtime.get("cluster_id")))
            .withColumn("user", F.lit(runtime.get("user")))
            .withColumn("payload_json", F.to_json(F.struct(*[F.col(c) for c in payload_cols])))
            .select(
                "dq_run_id","validation_id","phase_id","ingest_dt",
                "job_id","run_id","task_run_id","task_key","notebook_path","cluster_id","user",
                "_errors","_warnings","payload_json"
            )
        )
        return out

    def _build_rule_failures_df(self, quarantine_df: DataFrame, dq_run_id: str, validation_id: str) -> DataFrame:
        err = (quarantine_df
            .select(F.explode_outer("_errors").alias("rule_name"))
            .where(F.col("rule_name").isNotNull())
            .groupBy("rule_name")
            .count()
            .withColumn("severity", F.lit("error"))
        )

        warn = (quarantine_df
            .select(F.explode_outer("_warnings").alias("rule_name"))
            .where(F.col("rule_name").isNotNull())
            .groupBy("rule_name")
            .count()
            .withColumn("severity", F.lit("warn"))
        )

        out = (err.unionByName(warn, allowMissingColumns=True)
            .withColumnRenamed("count", "fail_count")
            .withColumn("dq_run_id", F.lit(dq_run_id))
            .withColumn("validation_id", F.lit(validation_id))
            .withColumn("ingest_dt", F.lit(date.today()))
            .select("dq_run_id","validation_id","ingest_dt","severity","rule_name","fail_count")
        )
        return out
