# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

import random
import string
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from pyspark.sql.functions import lit, udf, col, array_contains
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
    
    def general_id():
        charset = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ' + \
                  'abcdefghijklmnopqrstuvwxyz' + \
                  '0123456789-_'

        charsetLength = len(charset)

        keyLength = 3 * 5

        result = ["MO"]
        for _ in range(keyLength):
            result.append(charset[random.randint(0, charsetLength - 1)])

        return "".join(result)
    
    _id = udf(general_id, StringType())
    _time = str(kwargs["time"])
    _input = str(kwargs["clean_input"]) + _time
    _company = str(kwargs["company"])
    _output = str(kwargs["plmd_output"])
    _version = str(kwargs["version"])
    
    
    clean_df = spark.read.parquet(_input).filter(col("COMPANY") == _company)
    
    df = clean_df.selectExpr("COMMON_NAME AS VALUE", "COMPANY", "TIME").distinct() \
        .withColumn("ID", _id()) \
        .withColumn("CATEGORY", lit("KEYWORD")) \
        .withColumn("TYPE", lit("MOLE")) \
        .withColumn("VERSION", lit(_version))
        
    df.selectExpr("ID", "CATEGORY", "TYPE", "VALUE", "VERSION", "TIME","COMPANY") \
        .write \
        .partitionBy("TIME", "COMPANY") \
        .mode("append") \
        .parquet(_output)
    
    return {}
