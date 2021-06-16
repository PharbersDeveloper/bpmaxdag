# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job

Job流程
province_city_mapping_by_company (处理公司级别的Province City Mapping)

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
    
    _province_city_mapping_input = kwargs["province_city_mapping_input"]
    _version = kwargs["version"]
    _company = kwargs["company"]
    _label = kwargs["label"]
    _province_city_mapping_output = kwargs["province_city_mapping_output"]
    
    
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
    
    df = spark.read.parquet(_province_city_mapping_input)
    upper_columns = list(map(lambda str: str + " AS " + str.upper(), df.columns))
    
    
    def get_df(key):
        return df.selectExpr(*upper_columns) \
            .withColumnRenamed("ID", "CODE") \
            .withColumnRenamed(key, "VALUE") \
            .withColumn("ID", lit(gid())) \
            .withColumn("CODE", format_num_to_str(col("CODE"))) \
            .withColumn("CATEGORY",  lit("COMPANY")) \
            .withColumn("TAG", lit(key)) \
            .withColumn("COMPANY", lit(_company)) \
            .withColumn("VERSION", lit(_version)) \
            .selectExpr("ID", "CODE", "CATEGORY", "TAG", "VALUE", "VERSION", "COMPANY")
        
        
    un_df = reduce(lambda x, y: x.union(y), list(map(get_df, ["PROVINCE", "CITY"])) )
    # un_df.filter(col("TAG") == "PROVINCE").show()
    # un_df.filter(col("TAG") == "CITY").show()
    un_df.write \
        .partitionBy("COMPANY", "VERSION") \
        .parquet(_province_city_mapping_output, "append")
    return {}
