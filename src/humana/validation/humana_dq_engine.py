from __future__ import annotations
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from databricks.labs.dqx.engine import DQEngine
from humana.validation.dq_compiler import compile_validation_to_dqx_rules
from humana.validation.dq_rule_registry import DQRuleRegistry
class HumanaDQEngine:
    def __init__(self, integration, config_utils, dq_config_path: str, registry: Optional[DQRuleRegistry]=None,
                 default_ref_dfs: Optional[Dict[str, DataFrame]]=None, df_sources: Optional[Dict[str, DataFrame]]=None,
                 reload_on_validate: bool=False) -> None:
        self.integration = integration
        self.spark = integration.spark
        self.ws = integration.workspace_client
        self.config_utils = config_utils
        self.dq_config_path = dq_config_path
        self.reload_on_validate = reload_on_validate
        self.registry = registry or DQRuleRegistry()
        self.default_ref_dfs = default_ref_dfs or {}
        self.df_sources = df_sources or {}
        self.dq_engine = DQEngine(self.ws)
        self._validation_index={}
        self._reload_configs()
    def _reload_configs(self) -> None:
        configs = self.config_utils.read_all_yaml(self.dq_config_path)
        if isinstance(configs, dict):
            configs=[configs]
        idx={}
        for cfg in configs:
            for v in (cfg.get("validations",[]) or []):
                if v["id"] in idx:
                    raise ValueError(f"Duplicate validation id '{v['id']}'")
                idx[v["id"]] = v
        self._validation_index = idx
    def validate(self, validation_id: str, df: Optional[DataFrame]=None,
                 ref_dfs: Optional[Dict[str, DataFrame]]=None, extra_meta: Optional[Dict[str,str]]=None):
        v = self._validation_index[validation_id]
        input_df = df if df is not None else self.spark.table(v["table_name"])
        dq_rules = compile_validation_to_dqx_rules(v, self.registry)
        merged_refs = dict(self.default_ref_dfs)
        if ref_dfs: merged_refs.update(ref_dfs)
        if merged_refs:
            valid_df, quarantine_df = self.dq_engine.apply_checks_and_split(input_df, dq_rules, ref_dfs=merged_refs)
        else:
            valid_df, quarantine_df = self.dq_engine.apply_checks_and_split(input_df, dq_rules)
        run_id = str(uuid.uuid4())
        run_ts = datetime.now(timezone.utc).isoformat()
        for k,vv in [("_dq_run_id",run_id),("_dq_run_ts",run_ts),("_dq_validation_id",validation_id)]:
            valid_df = valid_df.withColumn(k, F.lit(vv))
            quarantine_df = quarantine_df.withColumn(k, F.lit(vv))
        summary_df = self.spark.createDataFrame([(run_id, run_ts, validation_id)], ["run_id","run_ts","validation_id"])
        return valid_df, quarantine_df, summary_df


        @staticmethod
    def _build_default_registry() -> DQRuleRegistry:
        """
        Central place for default custom-rule registration.
        Automatically invoked during engine construction.
        """
        reg = DQRuleRegistry()

        # Lazy imports avoid import-time side effects
        from humana.validation import builtin_row_rules as rr
        from humana.validation import dq_dataset_rules as ds

        # -------- Row rules --------
        reg.register_row("is_length_invalid", rr.is_length_invalid)
        reg.register_row("is_value_invalid", rr.is_value_invalid)
        reg.register_row("is_startswith_invalid", rr.is_startswith_invalid)
        # reg.register_row("is_alphanum_invalid", rr.is_alphanum_invalid)

        # -------- Dataset rules --------
        reg.register_dataset("uniqueness_invalid", ds.uniqueness_invalid)
        reg.register_dataset("overlaps_invalid", ds.overlaps_invalid)
        reg.register_dataset("referential_invalid", ds.referential_invalid)

        return reg
