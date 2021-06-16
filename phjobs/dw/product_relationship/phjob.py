# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

import random
import string
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from pyspark.sql.functions import lit, udf
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession


def execute(**kwargs):
    def generate_random_str(random_length):
        '''
        string.digits = 0123456789 string.ascii_letters = 26个小写,26个大写
        '''
        str_list = random.sample(string.digits + string.ascii_letters, random_length)
        random_str = ''.join(str_list)
        return random_str

    """
        please input your code below
        get spark session: spark = kwargs["spark"]()
    """
    logger = phs3logger(kwargs["job_id"], LOG_DEBUG_LEVEL)
    logger.info("当前 owner 为 " + str(kwargs["owner"]))
    logger.info("当前 run_id 为 " + str(kwargs["run_id"]))
    logger.info("当前 job_id 为 " + str(kwargs["job_id"]))
    spark = kwargs["spark"]()
    # spark = SparkSession.builder.master("").getOrCreate()

    _id = udf(generate_random_str, StringType())
    _version = kwargs["version"]
    _input_path = kwargs["input_path"]
    _table_type = kwargs["table_type"]
    _table_name = kwargs["table_name"]
    _out_put_path = kwargs["out_put"] \
                        .replace("#table_type#", _table_type) \
                        .replace("#table_name#", _table_name) + _version

    product_re_df = spark.read.parquet(_input_path) \
        .selectExpr("PACK_ID as VALUE") \
        .distinct() \
        .withColumn("CATEGORY", lit("PACKID")) \
        .withColumn("TYPE", lit("nan")) \
        .withColumn("VERSION", lit(_version)) \
        .withColumn("ID", _id(lit(13))) \
        .selectExpr("ID", "CATEGORY", "TYPE", "VALUE", "VERSION")

    product_re_df.show()

    # product_re_df.repartition(1).write.mode("overwrite").parquet(_out_put_path)
    return {}
