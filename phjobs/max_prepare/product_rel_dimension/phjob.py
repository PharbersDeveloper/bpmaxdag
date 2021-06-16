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

        result = []
        for _ in range(keyLength):
            result.append(charset[random.randint(0, charsetLength - 1)])

        return "".join(result)
    
    _id = udf(general_id, StringType())
    _time = str(kwargs["time"])
    _input = str(kwargs["clean_input"]) + _time
    _company = str(kwargs["company"])
    _output = str(kwargs["preld_output"])
    _version = str(kwargs["version"])
    
    # clean_df = spark.read.parquet(_input).filter(col("COMPANY") == _company)
    clean_df = spark.read.parquet(_input)
    
    # df = clean_df.selectExpr("PACK_ID AS VALUE", "COMPANY", "TIME").distinct().filter("VALUE is not null") \
    #     .withColumn("ID", _id()) \
    #     .withColumn("CATEGORY", lit("IMS PACKID")) \
    #     .withColumn("TYPE", lit("null")) \
    #     .withColumn("VERSION", lit(_version))
    
    df = clean_df.selectExpr("PACK_ID AS VALUE").distinct() \
        .withColumn("ID", _id()) \
        .withColumn("COMPANY", lit(_company)) \
        .withColumn("TIME", lit(_time)) \
        .withColumn("CATEGORY", lit("IMS PACKID")) \
        .withColumn("TYPE", lit("null")) \
        .withColumn("VERSION", lit(_version))
        
    df.selectExpr("ID", "CATEGORY", "TYPE", "VALUE", "VERSION", "TIME", "COMPANY") \
        .write \
        .partitionBy("TIME", "COMPANY") \
        .mode("append") \
        .parquet(_output)

    return {}
