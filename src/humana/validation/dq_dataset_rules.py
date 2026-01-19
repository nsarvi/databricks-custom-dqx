from __future__ import annotations
from typing import Callable, Dict, List, Optional, Tuple
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
def uniqueness_invalid(key_columns: List[str]) -> Tuple[str, Callable[[DataFrame, Optional[Dict[str, DataFrame]]], DataFrame]]:
    condition_col = "dq_condition_uniqueness_invalid"
    def _apply(df: DataFrame, ref_dfs: Optional[Dict[str, DataFrame]] = None) -> DataFrame:
        w = Window.partitionBy(*[F.col(c) for c in key_columns])
        return df.withColumn(condition_col, (F.count(F.lit(1)).over(w) > 1))
    return condition_col, _apply
def overlaps_invalid(partition_by: List[str], start_col: str, end_col: str, allow_touching: bool=True):
    condition_col = "dq_condition_overlaps_invalid"
    def _apply(df: DataFrame, ref_dfs=None):
        end_eff = F.coalesce(F.col(end_col), F.lit("3000-01-01").cast("date"))
        w = Window.partitionBy(*[F.col(c) for c in partition_by]).orderBy(F.col(start_col), end_eff)
        prev_end = F.lag(end_eff).over(w)
        violates = prev_end.isNotNull() & ((F.col(start_col) < prev_end) if allow_touching else (F.col(start_col) <= prev_end))
        return df.withColumn(condition_col, violates)
    return condition_col, _apply
def referential_invalid(ref_name: str, local_cols: List[str], ref_cols: List[str], allow_null_local: bool=True):
    condition_col = "dq_condition_referential_invalid"
    def _apply(df: DataFrame, ref_dfs=None):
        if not ref_dfs or ref_name not in ref_dfs:
            raise KeyError(f"Missing ref dataframe '{ref_name}'")
        ref = ref_dfs[ref_name].select([F.col(c) for c in ref_cols]).dropDuplicates()
        cond=None
        for lc, rc in zip(local_cols, ref_cols):
            c = (df[lc] == ref[rc])
            cond = c if cond is None else (cond & c)
        j = df.join(ref, on=cond, how="left")
        violates = j[ref_cols[0]].isNull()
        if allow_null_local:
            violates = violates & j[local_cols[0]].isNotNull()
        return j.withColumn(condition_col, violates)
    return condition_col, _apply
