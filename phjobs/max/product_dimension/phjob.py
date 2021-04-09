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
    
    _id = udf(general_id, StringType())
    _time = str(kwargs["time"])
    _input = str(kwargs["clean_input"]) + _time
    _company = str(kwargs["company"])
    _output = str(kwargs["pd_output"])
    _version = str(kwargs["version"])
    
    _base_path = str(kwargs["base_path"])
    
    _definite_path = "{base_path}/{model}/TIME={time}/COMPANY={company}"
    _lexicon_manufacturer_path = _definite_path.format(
        base_path = _base_path,
        model = "DIMENSION/LEXICON",
        time = _time,
        company = _company
    )
    _manufacturer_path = _definite_path.format(
        base_path = _base_path,
        model = "DIMENSION/MNF_DIMENSION",
        time = _time,
        company = _company
    )
    _relationship_path = _definite_path.format(
        base_path = _base_path,
        model = "DIMENSION/PRODUCT_RELATIONSHIP_DIMENSION",
        time = _time,
        company = _company
    )
    
    
    lexicon_manufacturer_df = spark.read.parquet(_lexicon_manufacturer_path) \
        .withColumnRenamed("ID", "LM_ID") \
        .withColumnRenamed("VALUE", "LM_VALUE")
    manufacturer_df = spark.read.parquet(_manufacturer_path) \
        .withColumnRenamed("ID", "M_ID")
    relationship_df = spark.read.parquet(_relationship_path) \
        .withColumnRenamed("ID", "R_ID") \
        .withColumnRenamed("VALUE", "R_VALUE")
    clean_df = spark.read.parquet(_input).filter(col("COMPANY") == _company)
    
    df = clean_df \
        .join(lexicon_manufacturer_df, [col("COMMON_NAME") == col("LM_VALUE")], "left_outer") \
        .join(manufacturer_df, [col("MANUFACTURER") == col("MNF_NAME")], "left_outer") \
        .join(relationship_df, [col("PACK_ID") == col("R_VALUE")], "left_outer")
    
    
    df = df.selectExpr("ORIGINAL_MIN", "LM_VALUE AS MOLE_NAME", "PRODUCT_NAME AS PROD_NAME_CH", "PACK_NUMBER AS PACK", "DOSAGE", "SPECIFICATIONS AS SPEC", "M_ID AS MNF_ID", "R_ID AS PACK_ID", "LM_ID AS MOLE_ID") \
        .withColumn("PROD_DESC", lit("null")) \
        .withColumn("PCK_DESC", lit("null")) \
        .withColumn("ATC_ID", lit("null")) \
        .withColumn("NFC_ID", lit("null")) \
        .withColumn("EVENTS", lit("null")) \
        .withColumn("ID", _id()) \
        .withColumn("TIME", lit(_time)) \
        .withColumn("COMPANY", lit(_company)) \
        .withColumn("VERSION", lit(_version)) \
        .withColumn("CONTAINS", create_map(
            lit('MOLE_ID'), col("MOLE_ID"),
            lit('MOLE_NAME'), col("MOLE_NAME"),
        ))
        
    df.selectExpr("ID", "ORIGINAL_MIN" ,"MOLE_NAME", "PROD_DESC", "PROD_NAME_CH", "PACK", "PCK_DESC", "DOSAGE", "SPEC", "CONTAINS", "MNF_ID", "PACK_ID", "ATC_ID", "NFC_ID", "EVENTS", "TIME", "COMPANY", "VERSION") \
        .write \
        .partitionBy("TIME", "COMPANY") \
        .mode("append") \
        .parquet(_output)
    
    return {}
