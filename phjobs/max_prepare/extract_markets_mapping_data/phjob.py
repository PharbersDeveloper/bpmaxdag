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
    
    _companies = str(kwargs["companies"]).replace(" ", "").split(",")
    _base_path = str(kwargs["base_path"]).replace(" ", "")
    _time = str(kwargs["time"])
    _output = str(kwargs["mapping_market_output"])
    
    _column_mapping = {
        "mkt": "MARKET",
        "model": "MARKET",
        "标准通用名": "COMMON_NAME"
    }
    
    
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
    
    def get_df(company):
        path = _base_path.replace("${company}", company)
        reading = spark.read.parquet(path)
        cols = list(filter(lambda x: x != "", list(map(lambda x: x if x in _column_mapping.keys() else "", reading.columns))))
        df = reading.select([col(c).alias(_column_mapping[c]) for c in cols]) \
            .withColumn("ID", gid()) \
            .withColumn("TIME", lit(_time)) \
            .withColumn("COMPANY", lit(company)) \
            .select("ID", "MARKET", "COMMON_NAME", "TIME", "COMPANY") \
            .write \
            .partitionBy("TIME", "COMPANY") \
            .mode("append") \
            .parquet(_output)
        return company   
    
    list(map(get_df, _companies))
    
    return {}
