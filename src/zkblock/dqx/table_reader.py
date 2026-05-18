from pyspark.sql import DataFrame
from typing import Optional
from zkblock.dqx import yaml_constants as YC
from typing import Any, Dict, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import StreamingQuery
from zkblock.dqx.utils.logging_utils import LoggingHandler
from zkblock.dqx import yaml_constants as YC


logger = LoggingHandler(__name__).get_logger()


class TableReader:

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_source_table(self, source: Dict[str, Any]) -> DataFrame:
        """
        Return a batch DataFrame for the given source.
        Expected keys:
          - YC.READ_TYPE_KEY: YC.TABLE_KEY | YC.CDF_KEY (or absent when using subquery)
          - YC.TABLE_NAME_KEY: required for YC.TABLE_KEY and YC.CDF_KEY
          - YC.SUBQUERY_KEY: SQL text for query-based reads
          - options: dict of reader options (optional)
        """
        source_id = source.get(YC.SOURCE_ID_KEY)
        read_type = (source.get(YC.READ_TYPE_KEY) or YC.TABLE_KEY).strip().lower()
        table_name = source.get(YC.TABLE_NAME_KEY)
        subquery = source.get(YC.SUBQUERY_KEY)
        sql_file = source.get(YC.SQL_FILE_KEY)
        options: Dict[str, Any] = source.get(YC.OPTIONS_KEY) or {}

        if read_type == YC.TABLE_KEY:
            # 1) Direct table read
            if table_name:
                logger.debug(f"Reading table: {table_name}")
                return self.spark.table(table_name)

        # 2) Query read (subquery wins regardless of read_type)
        if subquery:
            logger.debug("Reading via SQL subquery")
            df= self.spark.sql(subquery)
            return df
        # 2.1) File-based SQL
        if sql_file:
            logger.debug("Reading via SQL file")
            df = self.spark.sql(source.get(YC.SQL_FILE_TEXT_KEY))
            return df
        else:
            # 3) CDF read
            if read_type == YC.CDF_KEY:
                if not table_name:
                    raise ValueError(f"read_type '{YC.CDF_KEY}' requires '{YC.TABLE_NAME_KEY}'.")
                rdr = self.spark.read.option("readChangeFeed", "true")
                for k, v in options.items():
                    rdr = rdr.option(k, v)
                logger.debug(f"Reading CDF from table: {table_name} with options: {options}")
                return rdr.table(table_name)

        raise NotImplementedError(
            "Unsupported source accessor. Provide 'table_name' or 'subquery', "
            f"or set {YC.READ_TYPE_KEY} to '{YC.CDF_KEY}'."
        )
