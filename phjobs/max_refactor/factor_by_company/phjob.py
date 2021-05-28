"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job

Job流程
factor_by_company (处理每个项目或公司的的Factor存入 Balance Mapping表中)

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
    
    _factor_input = kwargs["factor_input"]
    _version = kwargs["version"]
    _company = kwargs["company"]
    _label = kwargs["label"]
    _factor_output = kwargs["factor_output"]
    
    
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
    
    df = spark.read.parquet(_factor_input) \
        .selectExpr("City AS CITY", "factor_new AS VALUE") \
        .withColumn("VALUE", col("VALUE").cast(StringType())) \
        .withColumn("ID", gid()) \
        .withColumn("PROVINCE", lit("null")) \
        .withColumn("CATEGORY", lit("FACTOR")) \
        .withColumn("TAG", lit(_label)) \
        .withColumn("COMPANY", lit(_company)) \
        .withColumn("VERSION", lit(_version)) \
        .selectExpr("ID", "PROVINCE", "CITY", "CATEGORY", "TAG", "VALUE", "VERSION",  "COMPANY")
    
    df.write \
        .partitionBy("COMPANY", "VERSION") \
        .parquet(_factor_output, "append")
    
    return {}
