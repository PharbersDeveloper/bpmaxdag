# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job

Job流程
id_bedsize_by_common_file (不论是不是Common File现在全部跟公司走)
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
    
    _id_bedsize_input = kwargs["id_bedsize_input"]
    _version = kwargs["version"]
    _company = kwargs["company"]
    _label = kwargs["label"]
    _id_bedsize_output = kwargs["id_bedsize_output"]
    
    
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
    
    df = spark.read.parquet(_id_bedsize_input) \
        .selectExpr("ID AS CODE", "Bedsize AS VALUE") \
        .withColumn("CODE", format_num_to_str(col("CODE"))) \
        .withColumn("VALUE", col("VALUE").cast(StringType())) \
        .withColumn("ID", lit(gid())) \
        .withColumn("CATEGORY",  lit("COMMONFILE")) \
        .withColumn("TAG", lit("BEDSIZE")) \
        .withColumn("COMPANY", lit(_company)) \
        .withColumn("VERSION", lit(_version)) \
        .selectExpr("ID", "CODE", "CATEGORY", "TAG", "VALUE", "VERSION", "COMPANY")
    
    df.write \
        .partitionBy("COMPANY", "VERSION") \
        .parquet(_id_bedsize_output, "append")
    
    return {}
