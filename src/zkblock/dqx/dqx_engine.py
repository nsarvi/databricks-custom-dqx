from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple
import uuid

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from idea4.dqx.dqx_rule_registry import DQRuleRegistry
from idea4.dqx import yaml_constants as YC
from idea4.dqx.base_dqx_engine import BaseDQXEngine
from idea4.dqx.dqx_compiler import compile_rule_pack_to_dqx_rules
from idea4.dqx.custom_rules.timeliness import precompute_timeliness_common_rules

from idea4.dqx.utils.logging_utils import LoggingHandler
from idea4.dqx.runtime_context import get_runtime_context


logger = LoggingHandler(__name__).get_logger()


class Idea4DQXEngine(BaseDQXEngine):

    def __init__(self, validation_config_path: str, dqx_config_file: str = None):
        logger.debug(f"Parameters passed in: validation_config_path={validation_config_path}, dqx_config_file={dqx_config_file}")
        super().__init__(validation_config_path, dqx_config_file)

    def apply_rules_source(
        self,
        source_id: str,
        ref_dfs: Optional[Dict[str, DataFrame]] = None,
        extra_meta: Optional[Dict[str, str]] = None):
        """
        Resolve a DataFrame from a source_id and apply the corresponding rule pack.
        Returns (valid_df, quarantine_df, summary_df), same as apply_rules().
        """
        logger.debug(f"apply_rules_source called with source_id={source_id}")
        src = (self.sources_map or {}).get(source_id)
        if not src:
            logger.error(f"Unknown source_id: {source_id}")
            raise KeyError(f"Unknown source_id: {source_id}")

        rule_pack_id = src.get(YC.RULE_PACK_ID_KEY)
        if not rule_pack_id:
            raise ValueError(f"Source '{source_id}' missing required '{YC.RULE_PACK_ID_KEY}'")

        # Build a batch DataFrame from the source (table / subquery / CDF)
        df = self.table_reader.read_source_table(src)
        return self.apply_rules(rule_pack_id, df, ref_dfs=ref_dfs, extra_meta=extra_meta)

    def apply_rules(self, rule_pack_id: str, df: DataFrame,
                    ref_dfs: Optional[Dict[str, DataFrame]]=None, extra_meta: Optional[Dict[str,str]]=None):
        rule_pack_config = self._get_rule_pack_by_id(rule_pack_id)
        logger.debug(f"Input parameters: rule_pack_id={rule_pack_id}, df={df}, ref_dfs={ref_dfs}, extra_meta={extra_meta}")

        if df is None:
            logger.error(f"DataFrame not provided for {rule_pack_id}")
            return

        # If caller didn't pass any refs, use the ones loaded from idea4_dqx_config
        effective_ref_dfs = self.reference_dfs
        if ref_dfs:
            # Caller-provided refs override same keys from config
            effective_ref_dfs.update(ref_dfs)

        input_df = df
        df, technical_cols = precompute_timeliness_common_rules(df, rule_pack_config)
        dq_rules = compile_rule_pack_to_dqx_rules(rule_pack_config, self.registry, df.columns)

        logger.info(f"Applying DQX rules for {rule_pack_id}")
        valid_df, quarantine_df = self.dq_engine.apply_checks_and_split(df, dq_rules, effective_ref_dfs)

        if technical_cols:
            valid_df = valid_df.drop(*[c for c in technical_cols if c in valid_df.columns])
            quarantine_df = quarantine_df.drop(*[c for c in technical_cols if c in quarantine_df.columns])

        fast_fail = bool(rule_pack_config.get(YC.FAST_FAIL_KEY, False))
        if fast_fail:
            quarantine_df = self._enrich_errors_from_config(quarantine_df, rule_pack_config)

        logger.info(f"Completed Applying DQX rules for {rule_pack_id}")
        run_id = str(uuid.uuid4())
        run_ts = datetime.now(timezone.utc).isoformat()
        for k, vv in [(YC.DQX_RUN_ID_KEY, run_id), (YC.DQX_RUN_TS_KEY, run_ts), (YC.DQX_VALIDATION_ID_KEY, rule_pack_id)]:
            valid_df = valid_df.withColumn(k, F.lit(vv))
            quarantine_df = quarantine_df.withColumn(k, F.lit(vv))
        summary_df = self.spark.createDataFrame([(run_id, run_ts, rule_pack_id)], [YC.DQX_RUN_ID_KEY, YC.DQX_RUN_TS_KEY, YC.DQX_VALIDATION_ID_KEY])

        if YC.DQX_METRICS_PERSISTENCE_KEY in self.dqx_config and self.dqx_config[YC.DQX_METRICS_PERSISTENCE_KEY].get(YC.ENABLED_KEY, False):
            logger.debug(f"Persisting results for rule_pack_id {rule_pack_id}")
            self.persist_dqx_metrics(rule_pack_id, input_df, valid_df, quarantine_df, extra_meta)
            logger.debug(f"Completed Persisting results for rule_pack_id {rule_pack_id}")
        return valid_df, quarantine_df, summary_df

    def persist_dqx_metrics(self,rule_pack_id: str,df: DataFrame, valid_df: DataFrame, quarantine_df: DataFrame, extra_meta: Optional[Dict[str,str]]=None):
        logger.info(f"Persisting results for rule_pack_id {rule_pack_id}")
        rt = get_runtime_context(self.dbutils)
        # Check config and persist metrics
        if extra_meta:
            rt.update({YC.RUNTIME_METADATA_KEY: extra_meta})

        # build outputs for persistence
        dq_run_id = str(uuid.uuid4())
        summary_out = self._build_run_summary_df(
            YC.DQX_SUMMARY_TABLE_KEY,
            dq_run_id=dq_run_id,
            validation_id=rule_pack_id,
            runtime=rt,
            status=YC.SUCCESS_KEY,
            input_df=df,
            valid_df=valid_df,
            quarantine_df=quarantine_df,
            persist_counts=True
        )

        quarantine_out = self._build_quarantine_df(
            quarantine_df=quarantine_df,
            dq_run_id=dq_run_id,
            validation_id=rule_pack_id,
            runtime=rt
        )

        self.persistence_manager.write_to_table(YC.DQX_SUMMARY_TABLE_KEY, summary_out)
        self.persistence_manager.write_to_table(YC.DQX_QUARANTINE_TABLE_KEY, quarantine_out)
        logger.debug(f"Persistence for rule_pack_id {rule_pack_id} completed")
