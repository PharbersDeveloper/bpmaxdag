# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

import random
import string
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from pyspark.sql.functions import lit, udf, col, array
from functools import reduce
from pyspark.sql.types import StringType


def execute(**kwargs):
    logger = phs3logger(kwargs["job_id"], LOG_DEBUG_LEVEL)
    spark = kwargs["spark"]()
    result_path_prefix = kwargs["result_path_prefix"]
    depends_path = kwargs["depends_path"]
    
    _extract_product_input = kwargs["extract_product_input"]
    _version = kwargs["version"]
    _company = kwargs["company"]
    _label = kwargs["label"]
    _product_mnf_output = kwargs["product_mnf_output"]
    
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
    
    
    gid = udf(general_id, StringType())
    
    
    extract_df = spark.read.parquet(_extract_product_input)
    
    
    df = extract_df.selectExpr("CORP_ID", "CORP_NAME_CH", "CORP_NAME_EN", "MNF_NAME_CH AS MNF_NAME", 
            "MNF_TYPE_NAME AS MNF_TYPE", "MNF_TYPE_NAME", "MNF_TYPE_NAME_CH").distinct() \
        .withColumn("ID", gid()) \
        .withColumn("COMPANY", lit(_company)) \
        .withColumn("LOCATION", array(lit(0), lit(0))) \
        .withColumn("VERSION", lit(_version))
        
        
    df.selectExpr("ID", "MNF_NAME", "MNF_TYPE", "MNF_TYPE_NAME","MNF_TYPE_NAME_CH", "CORP_ID", "CORP_NAME_CH", "CORP_NAME_EN", "LOCATION", "VERSION", "COMPANY") \
        .write \
        .partitionBy("COMPANY", "VERSION") \
        .mode("append") \
        .parquet(_product_mnf_output)
    
    return {}
