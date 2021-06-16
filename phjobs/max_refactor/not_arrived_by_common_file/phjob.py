# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max 

Job 流程
not_arrived_by_common_file 

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
    
    _not_arrived_input = kwargs["not_arrived_input"]
    _version = kwargs["version"]
    _company = kwargs["company"]
    _label = kwargs["label"]
    _not_arrived_output = kwargs["not_arrived_output"]
    
    
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
    
    
    df = spark.read.csv(_not_arrived_input, header=True).selectExpr("ID AS CODE", "Date AS DATE") \
        .withColumn("DATE", col("DATE").cast(StringType())) \
        .withColumn("ID", gid()) \
        .withColumn("CODE", format_num_to_str(col("CODE"))) \
        .withColumn("CATEGORY", lit(_label)) \
        .withColumn("COMPANY", lit(_company)) \
        .withColumn("VALUE", lit("null")) \
        .withColumn("VERSION", lit(_version)) \
        .selectExpr("ID", "CODE", "CATEGORY", "VALUE", "DATE", "VERSION", "COMPANY")
    
    df.write \
        .partitionBy("COMPANY", "VERSION") \
        .parquet(_not_arrived_output, "append")
    
    return {}
