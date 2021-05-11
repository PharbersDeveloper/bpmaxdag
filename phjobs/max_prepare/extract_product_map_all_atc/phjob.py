# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

import random
import string
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from pyspark.sql.functions import lit, udf, col, concat_ws
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
    
    _input = str(kwargs["input"])
    _time = str(kwargs["time"])
    _company = str(kwargs["company"])
    _output = str(kwargs["output"])
    
    def general_id():
        charset = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ' + \
                  'abcdefghijklmnopqrstuvwxyz' + \
                  '0123456789-_'

        charsetLength = len(charset)

        keyLength = 3 * 5

        result = [""]
        for _ in range(keyLength):
            result.append(charset[random.randint(0, charsetLength - 1)])

        return "".join(result)


    gid = udf(general_id, StringType())
    
    df = spark.read.csv(_input, header=True) \
        .select("min2", "通用名", "pfc", "标准商品名", "标准剂型", "标准规格", 
            "标准包装数量", "标准生产企业", "project", "MOLE_NAME_CH",
            "ATC1_CODE", "ATC1_DESC", "ATC1", 
            "ATC2_CODE", "ATC2_DESC", "ATC2",
            "ATC3_CODE", "ATC3_DESC", "ATC3",
            "ATC4_CODE", "ATC4_DESC", "ATC4") \
        .withColumnRenamed("min2", "MIN") \
        .withColumnRenamed("通用名", "MAPPING_MAMOLE_NAME") \
        .withColumnRenamed("pfc", "PACK_ID") \
        .withColumnRenamed("标准商品名", "PRODUCT_NAME") \
        .withColumnRenamed("标准剂型", "DOSAGE") \
        .withColumnRenamed("标准规格", "SPEC") \
        .withColumnRenamed("标准包装数量", "PACK") \
        .withColumnRenamed("标准生产企业", "MNF_NAME") \
        .withColumnRenamed("project", "PROJECT") \
        .withColumnRenamed("MOLE_NAME_CH", "MOLE_NAME") \
        .withColumn("COMPANY", lit(_company)) \
        .withColumn("TIME", lit(_time)) \
        .withColumn("ID", gid())
        
        
    df.write \
        .partitionBy("TIME", "COMPANY") \
        .mode("append") \
        .parquet(_output)
    
    
    return {}
