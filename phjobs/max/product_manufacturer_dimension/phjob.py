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

        result = ["M"]
        for _ in range(keyLength):
            result.append(charset[random.randint(0, charsetLength - 1)])

        return "".join(result)
    
    _id = udf(general_id, StringType())
    _time = str(kwargs["time"])
    _input = str(kwargs["clean_input"]) + _time
    _company = str(kwargs["company"])
    _output = str(kwargs["pmd_output"])
    _version = str(kwargs["version"])
    
    # clean_df = spark.read.parquet(_input).filter(col("COMPANY") == _company)
    clean_df = spark.read.parquet(_input)
    
    # df = clean_df.selectExpr("MANUFACTURER AS MNF_NAME", "COMPANY", "TIME").distinct() \
    #     .withColumn("ID", _id()) \
    #     .withColumn("MNF_TYPE", lit("null")) \
    #     .withColumn("MNF_TYPE_NAME", lit("null")) \
    #     .withColumn("MNF_TYPE_NAME_CH", lit("null")) \
    #     .withColumn("CORP_ID", lit("null")) \
    #     .withColumn("CORP_NAME_EN", lit("null")) \
    #     .withColumn("CORP_NAME_CH", lit("null")) \
    #     .withColumn("LOCATION", lit("null")) \
    #     .withColumn("VERSION", lit(_version))
        
    df = clean_df.selectExpr("CORP_ID", "CORP_NAME_CH", "CORP_NAME_EN", "MNF_NAME_CH AS MNF_NAME", 
            "MNF_TYPE_NAME AS MNF_TYPE", "MNF_TYPE_NAME", "MNF_TYPE_NAME_CH").distinct() \
        .withColumn("ID", _id()) \
        .withColumn("COMPANY", lit(_company)) \
        .withColumn("TIME", lit(_time)) \
        .withColumn("LOCATION", lit("null")) \
        .withColumn("VERSION", lit(_version))
        
        
    df.selectExpr("ID", "MNF_NAME", "MNF_TYPE", "MNF_TYPE_NAME","MNF_TYPE_NAME_CH", "CORP_ID", "CORP_NAME_CH", "CORP_NAME_EN", "LOCATION", "VERSION","TIME","COMPANY") \
        .write \
        .partitionBy("TIME", "COMPANY") \
        .mode("append") \
        .parquet(_output)
    
    return {}
