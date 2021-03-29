# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

import random
import string
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from pyspark.sql.functions import lit, udf
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

    _inputs = kwargs["inputs"].replace(" ", "").split(',')
    _output = kwargs["output"]

    spark = kwargs["spark"]()
    # spark = SparkSession.builder.getOrCreate()
    all_df = spark.read.parquet(*_inputs)
    all_df.repartition(1).write.mode("overwrite").parquet(_output)

    return {}

