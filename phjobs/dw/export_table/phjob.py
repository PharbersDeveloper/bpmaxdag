# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from pyspark.sql.functions import col
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
    # spark = SparkSession.builder.getOrCreate()

    _owner = kwargs["owner_id"]
    _version = kwargs["version"]
    _product = kwargs["product"] + _version
    _product_rel = kwargs["product_rel"] + _version
    _manufacturer = kwargs["manufacturer"] + _version
    _product_cgy = kwargs["product_cgy"] + _version
    _out_put_path = kwargs["out_put"].replace("#run_id#", kwargs["run_id"]).replace("#owner#", _owner)

    product_df = spark.read.parquet(_product).drop("ID").drop("CONTAINS")
    product_rel_df = spark.read.parquet(_product_rel).selectExpr("ID as REL_ID", "VALUE as IMS_VALUE")
    manufacturer_df = spark.read.parquet(_manufacturer).selectExpr("MNF_ID as PMNF_ID", "MNF_NAME_CH", "CORP_ID",
                                                                   "CORP_NAME_CH")
    product_cgy = spark.read.parquet(_product_cgy).selectExpr("ID", "CATEGORY", "VALUE")

    atc_cgy_df = product_cgy.filter(col("CATEGORY") == "ATC").withColumnRenamed("ID",
                                                                                "ATC_CATEGORY_ID").withColumnRenamed(
        "VALUE", "ATC_VALUE")
    nfc_cgy_df = product_cgy.filter(col("CATEGORY") == "NFC").withColumnRenamed("ID",
                                                                                "NFC_CATEGORY_ID").withColumnRenamed(
        "VALUE", "NFC_VALUE")

    product_df.join(manufacturer_df, [product_df.MNF_ID == manufacturer_df.PMNF_ID], "left_outer") \
        .join(product_rel_df, [product_df.PACK_ID == product_rel_df.REL_ID], "left_outer") \
        .join(atc_cgy_df, [product_df.ATC_ID == atc_cgy_df.ATC_CATEGORY_ID], "left_outer") \
        .join(nfc_cgy_df, [product_df.NFC_ID == nfc_cgy_df.NFC_CATEGORY_ID], "left_outer") \
        .selectExpr("MOLE_NAME", "PROD_DESC", "PROD_NAME_CH", "PACK", "PCK_DESC", "DOSAGE", "SPEC", "MNF_NAME_CH",
                    "CORP_ID", "CORP_NAME_CH", "IMS_VALUE as PACK_ID", "ATC_VALUE as ATC_CODE", "NFC_VALUE as NFC_CODE") \
        .repartition(1) \
        .write.mode("overwrite").csv(_out_put_path, header=True)

    return {}
