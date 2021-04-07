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

    _time = str(kwargs["time"])
    _company = str(kwargs["company"])
    _input = str(kwargs["input_dim_path"]) + "TIME=" + _time + "/COMPANY=" + _company
    _original_mapping_path = str(kwargs["original_mapping_path"])
    _input_mapping_path = str(kwargs["input_mapping_path"]).replace("${company}", _company)
    _output = str(kwargs["output_mapping_path"])
    _version = str(kwargs["version"])

    format_num_to_str = udf(lambda x: str(x).replace(".0", "").zfill(6), StringType())

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

    dim = spark.read.parquet(_input)
    input_mapping = spark.read.parquet(_input_mapping_path) \
        .filter(col("推荐版本") == 1) \
        .selectExpr("ID AS INPUT_ID", "PHA") \
        .withColumn("INPUT_ID", format_num_to_str(col("INPUT_ID")))
    original_mapping = spark.read.parquet(_original_mapping_path)
    mapping = input_mapping.join(original_mapping, [col("HOSP_CODE") == col("INPUT_ID"), col("PHA") == col("PHA_ID")],
                                 "left_outer") \
        .selectExpr("HOSP_CODE", "TYPE", "PHA_ID") \
        .join(dim, [col("PANEL_ID") == col("PHA_ID")], "left_outer") \
        .selectExpr("ID AS HOSPITAL_ID", "HOSP_CODE AS VALUE", "TYPE AS TAG") \
        .withColumn("ID", gid()) \
        .withColumn("CATEGORY", lit("TYPE")) \
        .withColumn("COMPANY", lit(_company)) \
        .withColumn("TIME", lit(_time)) \
        .withColumn("VERSION", lit(_version))

    logger.info("input_mapping ==> " + str(input_mapping.count()))
    logger.info("mapping ==> " + str(mapping.count()))

    mapping.write \
        .partitionBy("TIME", "COMPANY") \
        .mode("append") \
        .parquet(_output)

    return {}
