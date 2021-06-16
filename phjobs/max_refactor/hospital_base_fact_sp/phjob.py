# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job

Job 流程

extract_base_universe -> hospital_base_fact （只包含SEG与PANEL）

"""

import random
import string
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from pyspark.sql.functions import lit, udf, col, array
from functools import reduce
from pyspark.sql.types import StringType


def execute(**kwargs):
    logger = phs3logger(kwargs["job_id"], LOG_DEBUG_LEVEL)
    logger.info("Start")
    spark = kwargs["spark"]()
    logger.info("Get Spark Ins")
    result_path_prefix = kwargs["result_path_prefix"]
    depends_path = kwargs["depends_path"]
    
    _hosp_dim_input = kwargs["hosp_dim_input"]
    _original_clean_input = kwargs["original_clean_input"]
    _company = kwargs["company"]
    _version = kwargs["version"]
    _label = kwargs["label"]
    _hosp_base_fact_output = kwargs["hosp_base_fact_output"]
    
    
    base_fact_mapping = [
        {
            "CATEGORY": _label,
            "TAG": "SEG",
            "COLUMN": "SEG"
        },
        {
            "CATEGORY": _label,
            "TAG": "PANEL",
            "COLUMN": "PANEL"
        },
    ]
    
    
    def general_id():
        charset = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ' + \
                  'abcdefghijklmnopqrstuvwxyz' + \
                  '0123456789-_'

        charsetLength = len(charset)

        keyLength = 3 * 5

        result = ["H"]
        for _ in range(keyLength):
            result.append(charset[random.randint(0, charsetLength - 1)])

        return "".join(result)

    gid = udf(general_id, StringType())
    
    
    dim = spark.read.parquet(_hosp_dim_input)
    original_clean = spark.read.parquet(_original_clean_input)
    
    def fact_table(item):
        fact = dim.selectExpr("ID as HOSPITAL_ID", "PHA_ID AS DIM_PHA_ID") \
            .join(original_clean, [col("DIM_PHA_ID") == col("PHA_ID")], "left_outer") \
            .selectExpr("HOSPITAL_ID", item["COLUMN"]) \
            .withColumn("ID", gid()) \
            .withColumn("CATEGORY", lit(item["CATEGORY"])) \
            .withColumn("TAG", lit(item["TAG"])) \
            .withColumn("VALUE", col(item["COLUMN"]).cast(StringType())) \
            .withColumn("COMPANY", lit(_company)) \
            .withColumn("VERSION", lit(_version)) \
            .drop(item["COLUMN"])
        return fact

    fact_un_all = reduce(lambda x, y: x.union(y), list(map(fact_table, base_fact_mapping)))
    fact_un_all \
        .selectExpr("ID", "HOSPITAL_ID", "CATEGORY", "TAG", "VALUE", "COMPANY", "VERSION") \
        .write \
        .partitionBy("COMPANY", "VERSION") \
        .parquet(_hosp_base_fact_output, "append")
    
    
    return {}
