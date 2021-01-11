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
        
    # output required
    output_path = kwargs['output_path']
    if output_path == 'not set':
        raise Exception("Invalid output_path!", output_path)

    spark = SparkSession.builder \
        .master("yarn") \
        .appName("datamart original dim chc_hosps job") \
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
    
    reading = spark.read.parquet(input_path)
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
    
    chc_hosp_keys_lst = [
        "HOSP_CODE", "HOSP_LEVEL", "HOSP_LIBRARY_TYPE", "HOSP_NAME", "HOSP_REGION_TYPE", "HOSP_TYPE", 
        "PROVINCE_NAME", "CITY_NAME", "PREFECTURE_NAME",
        ]
    dest = reading.select(chc_hosp_keys_lst).distinct().withColumn("_ID", monotonically_increasing_id())
    # dest.show(100)
    # logger.info(dest.count()) # 81446
    dest.write.mode("overwrite").parquet(output_path)
    
    