# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

import random
import string
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from pyspark.sql.functions import lit, udf, col, when
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
    
    
    _input = str(kwargs["input_path"])
    _time = str(kwargs["time"])
    _company = str(kwargs["company"])
    _version = str(kwargs["version"])
    _output = str(kwargs["city_normalize_mapping_output"])
    
    
    df = spark.read.csv(_input, header=True) \
        .withColumnRenamed("Province", "MAPPING_PROVINCE") \
        .withColumnRenamed("City", "MAPPING_CITY") \
        .withColumnRenamed("标准省份名称", "PROVINCE") \
        .withColumnRenamed("标准城市名称", "CITY") \
        .withColumn("TIME", lit(_time)) \
        .withColumn("COMPANY", lit(_company)) \
        .withColumn("VERSION", lit(_version))
        
    df.write \
        .partitionBy("TIME", "COMPANY") \
        .mode("append") \
        .parquet(_output)
    
    return {}
