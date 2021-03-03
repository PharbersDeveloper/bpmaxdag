# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

import random
import string
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from pyspark.sql.functions import lit, col, udf, monotonically_increasing_id
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

    def unpivot(data_frame, keys):
        # 参数说明 df  dataframe   keys 待转换表中需要保留的主键key，以list[]类型传入
        # 转换是为了避免字段类不匹配，统一将数据转换为string类型，如果保证数据类型完全一致，可以省略该句
        data_frame = data_frame.select(*[col(_).astype("string") for _ in df.columns])
        # cols = [_ for _ in df.columns if _ not in keys and "DESC" not in _]
        cols = ["ATC", "NFC"]
        stack_str = ','.join(map(lambda x: "'%s', %s" % (x, x), cols))
        # CATEGORY, VALUE 转换后的列名，可自定义
        data_frame = data_frame.selectExpr(*keys, "stack(%s, %s) as (CATEGORY, VALUE)" % (len(cols), stack_str))
        return data_frame

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

    df = spark.read.parquet(_input_path) \
        .selectExpr("NFC123 as NFC", "NFC123_NAME as NFC_DESC", "ATC4_CODE as ATC",
                    "ATC4_DESC as ATC_DESC") \
        .distinct() \
        .withColumn("RANK", monotonically_increasing_id())

    prod_category_df = unpivot(df, ["RANK"]).withColumnRenamed("RANK", "DCRANK")

    atc_cate_df = prod_category_df.filter(col("CATEGORY") == "ATC") \
        .join(df, [col("ATC") == col("VALUE"), col("RANK") == col("DCRANK")], "left_outer") \
        .withColumn("DESCRIPTION", col("ATC_DESC")) \
        .selectExpr("DCRANK", "CATEGORY", "VALUE", "DESCRIPTION")

    nfc_cate_df = prod_category_df.filter(col("CATEGORY") == "NFC") \
        .join(df, [col("ATC") == col("VALUE"), col("RANK") == col("DCRANK")], "left_outer") \
        .withColumn("DESCRIPTION", col("NFC_DESC")) \
        .selectExpr("DCRANK", "CATEGORY", "VALUE", "DESCRIPTION")

    union_df = atc_cate_df.unionAll(nfc_cate_df) \
        .withColumn("TYPE", lit("nan")) \
        .withColumn("LEVEL", lit("nan")) \
        .withColumn("VERSION", lit(_version)) \
        .withColumn("ID", _id()) \
        .select("ID", "CATEGORY", "TYPE", "LEVEL", "VALUE", "DESCRIPTION", "VERSION")
    union_df.show()
    # union_df.repartition(1).write.mode("overwrite").parquet(_out_put_path)
    return {}
