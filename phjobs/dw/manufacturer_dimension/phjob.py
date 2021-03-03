# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

import random
import string
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from pyspark.sql.functions import lit, array, udf
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession


def generate_random_str(random_length):
    '''
    string.digits = 0123456789 string.ascii_letters = 26个小写,26个大写
    '''
    str_list = random.sample(string.digits + string.ascii_letters, random_length)
    random_str = ''.join(str_list)
    return random_str


def generate_id():
    return bytes("M" + generate_random_str(7), "UTF-8").hex()


def execute(**kwargs):
    """
        please input your code below
        get spark session: spark = kwargs["spark"]()
    """
    logger = phs3logger(kwargs["job_id"], LOG_DEBUG_LEVEL)
    logger.info("当前 owner 为 " + str(kwargs["owner"]))
    logger.info("当前 run_id 为 " + str(kwargs["run_id"]))
    logger.info("当前 job_id 为 " + str(kwargs["job_id"]))
    # TODO: 测试&部署时解开此注释
    spark = kwargs["spark"]()
    # spark = SparkSession.builder.master("yarn").getOrCreate()

    _id = udf(generate_id, StringType())
    _version = kwargs["version"]
    _input_path = kwargs["input_path"]
    _table_name = kwargs["table_name"]
    _table_type = kwargs["table_type"]
    _out_put_path = kwargs["out_put"] \
                        .replace("#table_type#", _table_type) \
                        .replace("#table_name#", _table_name) + _version

    reading = spark.read.parquet(_input_path)
    pd = reading \
        .withColumn("LOCATION", array(lit("0"), lit("0"))) \
        .withColumn("VERSION", lit(_version)) \
        .select("MNF_NAME_CH", "MNF_TYPE",
                "MNF_TYPE_NAME", "MNF_TYPE_NAME_CH",
                "CORP_ID", "CORP_NAME_EN",
                "CORP_NAME_CH", "LOCATION", "VERSION") \
        .distinct() \
        .withColumn("MNF_ID", _id()) \
        .select("MNF_ID", "MNF_NAME_CH", "MNF_TYPE",
                "MNF_TYPE_NAME", "MNF_TYPE_NAME_CH",
                "CORP_ID", "CORP_NAME_EN",
                "CORP_NAME_CH", "LOCATION", "VERSION")

    # pd.show()
    pd.repartition(1).write.mode("overwrite").parquet(_out_put_path)
    return {}
