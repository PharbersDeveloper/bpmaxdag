# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
import logging
import os

def execute(**kwargs):
    
    logging.basicConfig(format='%(asctime)s %(filename)s %(funcName)s %(lineno)d %(message)s')
    logger = logging.getLogger('driver_logger')
    logger.setLevel(logging.INFO)
    logger.info("Origin kwargs = {}.".format(str(kwargs)))

    # input required
    input_path = kwargs['input_path']
    if input_path == 'not set':
        raise Exception("Invalid input_path!", input_path)
        
    chc_prod_path = kwargs['chc_prod_path']
    if chc_prod_path == 'not set':
        raise Exception("Invalid chc_prod_path!", chc_prod_path)
    
    chc_hosp_path = kwargs['chc_hosp_path']
    if chc_hosp_path == 'not set':
        raise Exception("Invalid chc_hosp_path!", chc_hosp_path)
        
    chc_date_path = kwargs['chc_date_path']
    if chc_date_path == 'not set':
        raise Exception("Invalid chc_date_path!", chc_date_path)
    
    chc_etc_path = kwargs['chc_etc_path']
    if chc_etc_path == 'not set':
        raise Exception("Invalid chc_etc_path!", chc_etc_path)
        
    # output required
    output_path = kwargs['output_path']
    if output_path == 'not set':
        raise Exception("Invalid output_path!", output_path)

    spark = SparkSession.builder \
        .master("yarn") \
        .appName("datamart original fact_table chc job") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.instance", "1") \
        .config("spark.executor.memory", "1g") \
        .config('spark.sql.codegen.wholeStage', False) \
        .getOrCreate()

    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    
    if access_key is not None:
        spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
        spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
        spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
        spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
        spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.cn-northwest-1.amazonaws.com.cn")

    logger.info("preparing data")
    
    reading = spark.read.parquet(input_path)    # 23355096
#     root                                                                            
#  |-- CITY_NAME: string (nullable = true)
#  |-- COMPANY: string (nullable = true)
#  |-- DELIVERY_WAY: string (nullable = true)
#  |-- DOSAGE: string (nullable = true)
#  |-- HOSP_CODE: string (nullable = true)
#  |-- HOSP_LEVEL: string (nullable = true)
#  |-- HOSP_LIBRARY_TYPE: string (nullable = true)
#  |-- HOSP_NAME: string (nullable = true)
#  |-- HOSP_REGION_TYPE: string (nullable = true)
#  |-- HOSP_TYPE: string (nullable = true)
#  |-- MANUFACTURER_NAME: string (nullable = true)
#  |-- MKT: string (nullable = true)
#  |-- MOLE_NAME: string (nullable = true)
#  |-- MONTH: long (nullable = true)
#  |-- PACK_QTY: long (nullable = true)
#  |-- PACK_UNIT: string (nullable = true)
#  |-- PREFECTURE_NAME: string (nullable = true)
#  |-- PRICE: string (nullable = true)
#  |-- PRODUCT_NAME: string (nullable = true)
#  |-- PROVINCE_NAME: string (nullable = true)
#  |-- QUARTER: string (nullable = true)
#  |-- SALES_QTY_BOX: string (nullable = true)
#  |-- SALES_QTY_GRAIN: string (nullable = true)
#  |-- SALES_QTY_TAG: string (nullable = true)
#  |-- SALES_VALUE: string (nullable = true)
#  |-- SOURCE: string (nullable = true)
#  |-- SPEC: string (nullable = true)
#  |-- TAG: string (nullable = true)
#  |-- YEAR: long (nullable = true)
#  |-- version: string (nullable = true)
    
    # hosp
    chc_hosp = spark.read.parquet(chc_hosp_path) \
        .withColumnRenamed("_ID", "CHC_HOSP_ID")
    chc_hosp_keys_lst = [
        "HOSP_CODE", "HOSP_LEVEL", "HOSP_LIBRARY_TYPE", "HOSP_NAME", "HOSP_REGION_TYPE", "HOSP_TYPE", 
        "PROVINCE_NAME", "CITY_NAME", "PREFECTURE_NAME",
        ]
    chc_hosp_cond = [
        reading.HOSP_CODE == chc_hosp.HOSP_CODE,
        reading.HOSP_LEVEL == chc_hosp.HOSP_LEVEL,
        reading.HOSP_LIBRARY_TYPE == chc_hosp.HOSP_LIBRARY_TYPE,
        reading.HOSP_NAME == chc_hosp.HOSP_NAME,
        reading.HOSP_REGION_TYPE == chc_hosp.HOSP_REGION_TYPE,
        reading.HOSP_TYPE == chc_hosp.HOSP_TYPE,
        reading.PROVINCE_NAME == chc_hosp.PROVINCE_NAME,
        reading.CITY_NAME == chc_hosp.CITY_NAME,
        reading.PREFECTURE_NAME == chc_hosp.PREFECTURE_NAME,
        ]
    
    # prod
    chc_prod = spark.read.parquet(chc_prod_path) \
        .withColumnRenamed("_ID", "CHC_PROD_ID")
    chc_prod_keys_lst = [
        "PRODUCT_NAME", "MOLE_NAME", "SPEC", "DOSAGE", "PACK_QTY", "PACK_UNIT", 
        "MANUFACTURER_NAME", "DELIVERY_WAY",
        ]
    chc_prod_cond = [
        reading.PRODUCT_NAME == chc_prod.PRODUCT_NAME,
        reading.MOLE_NAME == chc_prod.MOLE_NAME,
        reading.SPEC == chc_prod.SPEC,
        reading.DOSAGE == chc_prod.DOSAGE,
        reading.PACK_QTY == chc_prod.PACK_QTY,
        reading.PACK_UNIT == chc_prod.PACK_UNIT,
        reading.MANUFACTURER_NAME == chc_prod.MANUFACTURER_NAME,
        reading.DELIVERY_WAY == chc_prod.DELIVERY_WAY,
        ]
        
    # date
    chc_date = spark.read.parquet(chc_date_path) \
        .withColumnRenamed("_ID", "CHC_DATE_ID")
    chc_date_keys_lst = [
        "YEAR", "QUARTER", "MONTH",
        ]
    chc_date_cond = [
        reading.YEAR == chc_date.YEAR,
        reading.QUARTER == chc_date.QUARTER,
        reading.MONTH == chc_date.MONTH,
        ]
        
    # etc
    chc_etc = spark.read.parquet(chc_etc_path) \
        .withColumnRenamed("_ID", "CHC_ETC_ID")
    chc_etc_keys_lst = [
        "COMPANY", "MKT", "SOURCE", "TAG",
        ]
    chc_etc_cond = [
        reading.COMPANY == chc_etc.COMPANY,
        reading.MKT == chc_etc.MKT,
        reading.SOURCE == chc_etc.SOURCE,
        reading.TAG == chc_etc.TAG,
        ]
    
    dest = reading \
        .join(chc_hosp, chc_hosp_cond, how='left') \
        .drop(*chc_hosp_keys_lst) \
        .join(chc_prod, chc_prod_cond, how='left') \
        .drop(*chc_prod_keys_lst) \
        .join(chc_date, chc_date_cond, how='left') \
        .drop(*chc_date_keys_lst) \
        .join(chc_etc, chc_etc_cond, how='left') \
        .drop(*chc_etc_keys_lst)
    
    # dest.show(100)
    # print(dest.count()) # 23355096
    dest.write.mode("overwrite").parquet(output_path)
