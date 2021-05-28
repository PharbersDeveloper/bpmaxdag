# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max 

Job 流程
extract_city_normalize (由于没有地理维度统计标准，只能单独做Mapping表)

"""

import random
import string
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from pyspark.sql.functions import lit, udf, col
from functools import reduce
from pyspark.sql.types import StringType


def execute(**kwargs):
    logger = phs3logger(kwargs["job_id"], LOG_DEBUG_LEVEL)
    spark = kwargs["spark"]()
    result_path_prefix = kwargs["result_path_prefix"]
    depends_path = kwargs["depends_path"]
    
    
    _city_normalize_input = kwargs["city_normalize_input"]
    _version = kwargs["version"]
    _company = kwargs["company"]
    _label = kwargs["label"]
    _city_normalize_output = kwargs["city_normalize_output"]
    
    
    def general_id():
        charset = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ' + \
                  'abcdefghijklmnopqrstuvwxyz' + \
                  '0123456789-_'

        charsetLength = len(charset)

        keyLength = 3 * 5

        result = ["MP"]
        for _ in range(keyLength):
            result.append(charset[random.randint(0, charsetLength - 1)])

        return "".join(result)
    
    
    gid = udf(general_id, StringType())
    
    
    df = spark.read.csv(_city_normalize_input, header=True) \
        .withColumnRenamed("Province", "PROVINCE_ORIGINAL") \
        .withColumnRenamed("City", "CITY_ORIFINAL") \
        .withColumnRenamed("标准省份名称", "PROVINCE") \
        .withColumnRenamed("标准城市名称", "CITY") \
        .withColumn("ID", gid()) \
        .withColumn("COMPANY", lit(_company)) \
        .withColumn("VERSION", lit(_version)) \
        .selectExpr("ID", "PROVINCE_ORIGINAL", "PROVINCE", "CITY_ORIFINAL", "CITY", "COMPANY", "VERSION")
    
    df.write \
        .partitionBy("COMPANY", "VERSION") \
        .parquet(_city_normalize_output, "append")
    
    return {}
