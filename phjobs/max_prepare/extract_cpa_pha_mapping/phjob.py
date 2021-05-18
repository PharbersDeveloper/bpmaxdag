# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

import random
import string
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from pyspark.sql.functions import lit, udf, col
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
    
    _time = str(kwargs["time"])
    _company = str(kwargs["company"])
    _input = str(kwargs["input_path"]).replace("${company}", _company)
    _output = str(kwargs["cpa_pha_mapping_output"])
    _version = str(kwargs["version"])
    
    spark = kwargs["spark"]()
    
    format_num_to_str = udf(lambda x: str(x).replace(".0", "").zfill(6), StringType())
    
    df = spark.read.parquet(_input).filter(col("推荐版本") == 1)
    df.selectExpr("ID", "PHA") \
        .withColumn("ID", format_num_to_str(col("ID"))) \
        .withColumn("TIME", lit(_time)) \
        .withColumn("COMPANY", lit(_company)) \
        .withColumn("VERSION", lit(_version)) \
        .write \
        .partitionBy("TIME", "COMPANY") \
        .mode("append") \
        .parquet(_output)
    
    return {}
