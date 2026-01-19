from __future__ import annotations
from typing import Iterable
import pyspark.sql.functions as F
from pyspark.sql import Column
from databricks.labs.dqx.check_funcs import make_condition
def is_length_invalid(column: str, min_len: int, max_len: int, allow_null: bool=False) -> Column:
    c = F.col(column)
    valid = F.length(c).between(F.lit(min_len), F.lit(max_len))
    valid = (c.isNull() | valid) if allow_null else (c.isNotNull() & valid)
    return make_condition(condition=valid, msg=f"{column} length invalid", name=f"{column}_len")
def is_value_invalid(column: str, invalid_values: Iterable[str], allow_null: bool=False) -> Column:
    c = F.col(column)
    norm = F.lower(F.trim(c))
    invalid = norm.isin([str(x).lower().strip() for x in invalid_values])
    valid = ~invalid
    valid = (c.isNull() | valid) if allow_null else (c.isNotNull() & valid)
    return make_condition(condition=valid, msg=f"{column} invalid value", name=f"{column}_invalid")
def is_startswith_invalid(column: str, allowed_prefixes: Iterable[str], allow_null: bool=False) -> Column:
    c = F.col(column)
    ok = F.lit(False)
    for p in allowed_prefixes:
        ok = ok | c.startswith(str(p))
    valid = ok
    valid = (c.isNull() | valid) if allow_null else (c.isNotNull() & valid)
    return make_condition(condition=valid, msg=f"{column} bad prefix", name=f"{column}_prefix")
