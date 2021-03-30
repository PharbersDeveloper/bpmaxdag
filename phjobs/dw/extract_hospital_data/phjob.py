# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

import random
import string
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from pyspark.sql.functions import lit, udf
from functools import reduce
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession


def execute(**kwargs):
    """
        please input your code below
        get spark session: spark = kwargs["spark"]()
    """
    logger = phs3logger(kwargs["job_id"], LOG_DEBUG_LEVEL)
    logger.info("当前 owner 为 " + str(kwargs["owner"]))
    logger.info("当前 run_id 为 " + str(kwargs["run_id"]))
    logger.info("当前 job_id 为 " + str(kwargs["job_id"]))
    spark = kwargs["spark"]()
    _input = kwargs["input"]
    _output = kwargs["output"].replace("${run_id}", str(kwargs["run_id"]))

    # TODO 提出参数化
    custom_columns = {
        "综合专科": "MARKET",
        "专科综合": "MARKET"
    }
    drop_matchers = ["__", "_GYC", "LEVEL", "综合专科", "专科综合"]

    def convert_upper_columns(column):
        # if column in custom_columns.keys():
        #     return custom_columns[column]
        # else:
        return column.upper()

    def drop_other_columns(df):
        columns = [item for item in df.columns if any(xitem in item for xitem in drop_matchers)]
        return df.drop(*columns)

    reading = spark.read.parquet(_input)
    old_columns = reading.schema.names
    new_columns = list(map(convert_upper_columns, old_columns))
    df = drop_other_columns(reduce(lambda reading, idx: reading.withColumnRenamed(old_columns[idx], new_columns[idx]), range(len(old_columns)), reading))
    df.show()
    return {}
