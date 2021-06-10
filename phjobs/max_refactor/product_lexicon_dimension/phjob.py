# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
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
    
    _extract_product_input = kwargs["extract_product_input"]
    _version = kwargs["version"]
    _company = kwargs["company"]
    _label = kwargs["label"]
    _product_lexicon_output = kwargs["product_lexicon_output"]
    
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
    
    
    gid = udf(general_id, StringType())
    
    
    extract_df = spark.read.parquet(_extract_product_input)
    
    mole_df = extract_df.selectExpr("MOLE_NAME_CH AS VALUE").distinct() \
        .withColumn("ID", gid()) \
        .withColumn("COMPANY", lit(_company)) \
        .withColumn("CATEGORY", lit("KEYWORD")) \
        .withColumn("TYPE", lit("MOLE")) \
        .withColumn("VERSION", lit(_version))
    
    mnf_df = extract_df.selectExpr("MNF_NAME_CH AS VALUE").distinct() \
        .withColumn("ID", gid()) \
        .withColumn("COMPANY", lit(_company)) \
        .withColumn("CATEGORY", lit("KEYWORD")) \
        .withColumn("TYPE", lit("MNF")) \
        .withColumn("VERSION", lit(_version))
    
    df = mole_df.union(mnf_df)
    
    df.selectExpr("ID", "CATEGORY", "TYPE", "VALUE", "VERSION","COMPANY") \
        .write \
        .partitionBy("COMPANY", "VERSION") \
        .mode("append") \
        .parquet(_product_lexicon_output)
        
    return {}
