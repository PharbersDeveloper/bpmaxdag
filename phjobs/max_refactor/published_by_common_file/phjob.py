# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max 

Job 流程
published_by_common_file 

"""

import random
import string
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from pyspark.sql.functions import lit, udf, col, array, explode
from functools import reduce
from pyspark.sql.types import StringType


def execute(**kwargs):
    logger = phs3logger(kwargs["job_id"], LOG_DEBUG_LEVEL)
    spark = kwargs["spark"]()
    result_path_prefix = kwargs["result_path_prefix"]
    depends_path = kwargs["depends_path"]
    
    _published_input = kwargs["published_input"]
    _version = kwargs["version"]
    _company = kwargs["company"]
    _label = kwargs["label"]
    _date = kwargs["date"]
    _published_output = kwargs["published_output"]
    
    
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
    
    format_num_to_str = udf(lambda x: str(x).replace(".0", "").zfill(6), StringType())
    
    d = list(map(lambda x: lit(_date + "%02d" % (x + 1)), range(12)))
    
    df = spark.read.csv(_published_input, header=True).selectExpr("ID AS CODE", "Source AS SOURCE") \
        .withColumn("CODE", format_num_to_str(col("CODE"))) \
        .withColumn("DATES", array(d)) \
        .withColumn("DATE", explode(col("DATES"))) \
        .selectExpr("CODE", "SOURCE", "DATE")
        
    
    df = df.selectExpr("CODE", "SOURCE AS VALUE", "DATE") \
        .withColumn("ID", gid()) \
        .withColumn("VALUE", col("VALUE").cast(StringType())) \
        .withColumn("COMPANY", lit(_company)) \
        .withColumn("CATEGORY", lit(_label)) \
        .withColumn("VERSION", lit(_version)) \
        .selectExpr("ID", "CODE", "CATEGORY", "VALUE", "DATE", "VERSION", "COMPANY")
    df.write \
        .partitionBy("COMPANY", "VERSION") \
        .parquet(_published_output, "append")
        
    
    return {}
