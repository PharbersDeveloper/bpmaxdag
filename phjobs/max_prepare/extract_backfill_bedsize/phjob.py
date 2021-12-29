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
    _output = str(kwargs["backfill_bedsize_output"])
    
    
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
    
    
    format_num_to_str = udf(lambda x: str(x).replace(".0", "").zfill(6), StringType())
    
    df = spark.read.parquet(_input)
    df.selectExpr("ID", "Bedsize AS BEDSIZE") \
        .withColumn("ID", format_num_to_str(col("ID"))) \
        .withColumn("TIME", lit(_time)) \
        .withColumn("COMPANY", lit(_company)) \
        .withColumn("VERSION", lit(_version)) \
        .write \
        .partitionBy("TIME", "COMPANY") \
        .mode("append") \
        .parquet(_output)
    
    
    return {}
