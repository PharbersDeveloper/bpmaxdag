# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

import random
import string
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from pyspark.sql.functions import lit, col, udf, create_map, concat
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession


def execute(**kwargs):
    def generate_random_str(random_length):
        str_list = random.sample(string.digits + string.ascii_letters, random_length)
        random_str = ''.join(str_list)
        return random_str

    def generate_id(atc, mnf_id, pack):
        return "P" + bytes(atc + mnf_id + generate_random_str(5) + pack, "UTF-8").hex()

    def replace_null(value):
        if value is None:
            return "nan"
        else:
            return value

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
    _id = udf(generate_id, StringType())
    _rn = udf(replace_null, StringType())
    _version = kwargs["version"]
    _mole_spec_dim_version = kwargs["mole_spec_dim_version"]
    _mnf_dim_version = kwargs["mnf_dim_version"]
    _product_category_version = kwargs["product_category_version"]  #
    _product_rel_version = kwargs["product_rel_version"] #

    _input_path = kwargs["input_path"]
    _mole_spec_dim_path = kwargs["mole_spec_dim_path"] + _mole_spec_dim_version
    _mnf_dim_path = kwargs["mnf_dim_path"] + _mnf_dim_version
    _product_category_path = kwargs["product_category_path"] + _product_category_version  #
    _product_rel_path = kwargs["product_rel_path"] + _product_rel_version  #

    _table_type = kwargs["table_type"]
    _table_name = kwargs["table_name"]

    _out_put_path = kwargs["out_put"] \
                        .replace("#table_type#", _table_type) \
                        .replace("#table_name#", _table_name) + _version

    product_df = spark.read.parquet(_input_path)
    mole_spec_dim_df = spark.read.parquet(_mole_spec_dim_path)
    mnf_dim_df = spark.read.parquet(_mnf_dim_path)
    product_category_df = spark.read.parquet(_product_category_path)
    product_rel_df = spark.read.parquet(_product_rel_path).selectExpr("ID as PACK_ID", "VALUE as PACK_ID_VALUE")

    df = product_df.selectExpr("PACK_ID as IMS_PACK_ID", "PROD_DESC", "PROD_NAME_CH", "PACK", "PCK_DESC",
                               "DOSAGE", "ATC4_CODE", "NFC123",
                               "SPEC_valid_digit", "SPEC_valid_unit", "MOLE_NAME_CH",
                               "MNF_NAME_CH", "CORP_ID", "MNF_TYPE", "MNF_TYPE_NAME", "CORP_NAME_EN")

    mole_df = mole_spec_dim_df.filter(col("TYPE") == "MOLE") \
        .selectExpr("MOLE_NAME", "ID as MOLE_ID", "TYPE as MOLE_TYPE")
    spec_df = mole_spec_dim_df.filter(col("TYPE") == "SPEC") \
        .selectExpr("QUANTITY", "UNIT", "ID as SPEC_ID", "TYPE as SPEC_TYPE")

    atc_df = product_category_df.filter(col("CATEGORY") == "ATC") \
        .selectExpr("ID as ATC_ID", "VALUE as ATC_VALUE")
    nfc_df = product_category_df.filter(col("CATEGORY") == "NFC") \
        .selectExpr("ID as NFC_ID", "VALUE as NFC_VALUE")

    mole_join = df \
        .join(mole_df, [df.MOLE_NAME_CH == mole_spec_dim_df.MOLE_NAME], "left_outer") \
        .join(spec_df, [df.SPEC_valid_digit == mole_spec_dim_df.QUANTITY, df.SPEC_valid_unit == mole_spec_dim_df.UNIT],
              "left_outer") \
        .join(mnf_dim_df,
              [df.MNF_NAME_CH == mnf_dim_df.MNF_NAME_CH, df.CORP_ID == mnf_dim_df.CORP_ID,
               df.MNF_TYPE == mnf_dim_df.MNF_TYPE, df.CORP_NAME_EN == mnf_dim_df.CORP_NAME_EN], "left_outer") \
        .join(product_rel_df, [df.IMS_PACK_ID == product_rel_df.PACK_ID_VALUE], "left_outer") \
        .join(atc_df, [df.ATC4_CODE == atc_df.ATC_VALUE], "left_outer") \
        .join(nfc_df, [df.NFC123 == nfc_df.NFC_VALUE], "left_outer")
        
    mole_join = mole_join \
        .select("PACK_ID", "ATC_ID", "NFC_ID", "PROD_DESC", "PROD_NAME_CH", "PACK", "PCK_DESC", "DOSAGE", "ATC4_CODE", "NFC123", "MNF_ID",
                create_map(lit('MOLE_ID'), col("MOLE_ID"),
                          lit('MOLE_NAME'), col("MOLE_NAME"),
                          lit("SPEC_ID"), _rn(col("SPEC_ID")),
                          lit("QUANTITY"), _rn(col("QUANTITY")),
                          lit("UNIT"), _rn(col("UNIT"))).alias("CONTAINS")) \
        .withColumn("MOLE_NAME", col("CONTAINS.MOLE_NAME")) \
        .withColumn("SPEC", concat(col("CONTAINS.QUANTITY"), col("CONTAINS.UNIT"))) \
        .withColumn("ID", _id(col("ATC4_CODE"), col("NFC123"), col("PACK"))) \
        .withColumn("MNF_ID", col("MNF_ID")) \
        .withColumn("ATC_ID", col("ATC_ID")) \
        .withColumn("NFC_ID", col("NFC_ID")) \
        .withColumn("EVENTS", lit("nan")) \
        .select("ID", "MOLE_NAME", "PROD_DESC", "PROD_NAME_CH", "PACK", "PCK_DESC",
                "DOSAGE", "CONTAINS", "SPEC", "MNF_ID", "PACK_ID","ATC_ID", "NFC_ID", "EVENTS")
    mole_join.show()
    mole_join.repartition(1) \
        .write.mode("overwrite") \
        .parquet(_out_put_path)

    return {}
