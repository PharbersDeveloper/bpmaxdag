# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

import random
import string
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from pyspark.sql.functions import lit, udf, col, create_map
from functools import reduce
from pyspark.sql.types import StringType


def execute(**kwargs):
    logger = phs3logger(kwargs["job_id"], LOG_DEBUG_LEVEL)
    spark = kwargs["spark"]()
    result_path_prefix = kwargs["result_path_prefix"]
    depends_path = kwargs["depends_path"]
    
    
    _extract_product_input = kwargs["extract_product_input"]
    _version = kwargs["version"]
    _company = kwargs["company"]
    _label = kwargs["label"]
    _product_drug_output = kwargs["product_drug_output"]
    
    def general_id():
        charset = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ' + \
                  'abcdefghijklmnopqrstuvwxyz' + \
                  '0123456789-_'

        charsetLength = len(charset)

        keyLength = 3 * 5

        result = ["P"]
        for _ in range(keyLength):
            result.append(charset[random.randint(0, charsetLength - 1)])

        return "".join(result)
    
    
    gid = udf(general_id, StringType())
    
    
    extract_df = spark.read.parquet(_extract_product_input)
    
    
    def dim_path_format(categroy):
        return "/".join(_product_drug_output.replace("//", "$").split("/")[:-1]).replace("$", "//") + "/" + categroy + "/COMPANY=" + _company + "/VERSION=" + _version
    
    
    categroy_df = spark.read.parquet(dim_path_format("CATEGORY"))
    categroy_df.persist()
    
    lexicon_df = spark.read.parquet(dim_path_format("LEXICON")).filter("TYPE == 'MOLE'") \
        .withColumnRenamed("ID", "LM_ID") \
        .withColumnRenamed("VALUE", "LM_VALUE")
    # lexicon_df.persist()
    
    manufacturer_df = spark.read.parquet(dim_path_format("MANUFACTURER")) \
        .withColumnRenamed("ID", "M_ID") \
        .withColumnRenamed("CORP_ID", "M_CORP_ID") \
        .withColumnRenamed("MNF_TYPE_NAME", "M_MNF_TYPE_NAME") \
        .withColumnRenamed("MNF_TYPE_NAME_CH", "M_MNF_TYPE_NAME_CH") \
        .withColumnRenamed("CORP_NAME_CH", "M_CORP_NAME_CH") \
        .withColumnRenamed("CORP_NAME_EN", "M_CORP_NAME_EN")
    # manufacturer_df.persist()
    
    drug_map_df = spark.read.parquet(dim_path_format("DRUGMAP")) \
        .withColumnRenamed("ID", "R_ID") \
        .withColumnRenamed("VALUE", "R_VALUE")
    # drug_map_df.persist()
    
    categroy_atc_df = categroy_df.filter("CATEGORY == 'ATC'") \
        .withColumnRenamed("ID", "PC_ATC_ID") \
        .withColumnRenamed("VALUE", "PC_ATC_VALUE")
    categroy_nfc_df = categroy_df.filter("CATEGORY == 'NFC'") \
        .withColumnRenamed("ID", "PC_NFC_ID") \
        .withColumnRenamed("VALUE", "PC_NFC_VALUE")
    
    
    df = extract_df \
        .join(lexicon_df, [col("MOLE_NAME_CH") == col("LM_VALUE")], "left_outer") \
        .join(drug_map_df, [col("PACK_ID") == col("R_VALUE")], "left_outer") \
        .join(categroy_atc_df, [col("ATC4_CODE") == col("PC_ATC_VALUE")], "left_outer") \
        .join(categroy_nfc_df, [col("NFC123") == col("PC_NFC_VALUE")], "left_outer") \
        .join(manufacturer_df, [
            col("CORP_ID") == col("M_CORP_ID"), 
            col("MNF_NAME_CH") == col("MNF_NAME"), 
            col("CORP_NAME_CH") == col("M_CORP_NAME_CH"),
            col("CORP_NAME_EN") == col("M_CORP_NAME_EN"),
            col("MNF_TYPE_NAME") == col("M_MNF_TYPE_NAME")
        ], "left_outer") \
        .show()
    #     .selectExpr("LM_VALUE AS MOLE_NAME", "PROD_NAME_CH", "PACK", "DOSAGE", 
    #     "SPEC", "M_ID AS MNF_ID", "R_ID AS PACK_ID", "LM_ID AS MOLE_ID", 
    #     "PC_ATC_ID AS ATC_ID", "PC_NFC_ID AS NFC_ID") \
    #     .withColumn("PROD_DESC", lit("null")) \
    #     .withColumn("PCK_DESC", lit("null")) \
    #     .withColumn("EVENTS", lit("null")) \
    #     .withColumn("ID", gid()) \
    #     .withColumn("COMPANY", lit(_company)) \
    #     .withColumn("VERSION", lit(_version)) \
    #     .withColumn("CONTAINS", create_map(
    #         lit('MOLE_ID'), col("MOLE_ID"),
    #         lit('MOLE_NAME'), col("MOLE_NAME"),
    #     ))
    
    
    # df.selectExpr("ID", "MOLE_NAME", "PROD_DESC", 
    #     "PROD_NAME_CH", "PACK", "PCK_DESC", 
    #     "DOSAGE", "SPEC", "CONTAINS", "MNF_ID", 
    #     "PACK_ID", "ATC_ID", "NFC_ID", "EVENTS",
    #     "COMPANY", "VERSION") \
    #     .show()
    
    
    
    return {}
