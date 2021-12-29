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
    prod_input_path = kwargs['prod_input_path']
    if prod_input_path == 'not set':
        raise Exception("Invalid prod_input_path!", prod_input_path)
        
    # input required
    mnfs_input_path = kwargs['mnfs_input_path']
    if mnfs_input_path == 'not set':
        raise Exception("Invalid mnfs_input_path!", mnfs_input_path)
        
    # output required
    prod_output_path = kwargs['prod_output_path']
    if prod_output_path == 'not set':
        raise Exception("Invalid prod_output_path!", prod_output_path)

    spark = SparkSession.builder \
        .master("yarn") \
        .appName("datamart dimension prod job") \
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
    
    product_data_df = spark.read.parquet(prod_input_path)
    mnfs_data_df = spark.read.parquet(mnfs_input_path)
    
    mnfs_data_df = mnfs_data_df.withColumnRenamed('_ID', 'PH_MNF_ID')
    cond = [
        product_data_df.MNF_ID == mnfs_data_df.MNF_ID,
        product_data_df.MNF_TYPE == mnfs_data_df.MNF_TYPE,
        product_data_df.CORP_NAME_EN == mnfs_data_df.CORP_NAME_EN,
        product_data_df.CORP_NAME_CH == mnfs_data_df.CORP_NAME_CH,
        product_data_df.MNF_TYPE_NAME == mnfs_data_df.MNF_TYPE_NAME_EN,
        product_data_df.MNF_TYPE_NAME_CH == mnfs_data_df.MNF_TYPE_NAME_CH,
        product_data_df.MNF_NAME_EN == mnfs_data_df.MNF_NAME_EN,
        product_data_df.MNF_NAME_CH == mnfs_data_df.MNF_NAME_CH,
    ]
    product_data_df = product_data_df.join(mnfs_data_df, cond, "left_outer") \
                        .drop('MNF_ID', 'MNF_TYPE', 'CORP_ID', 'CORP_NAME_EN', 'CORP_NAME_CH', 'MNF_TYPE_NAME', 'MNF_TYPE_NAME_EN', 'MNF_TYPE_NAME_CH', 'MNF_NAME_EN', 'MNF_NAME_CH') \
                        .drop('NFC1', 'NFC1_NAME', 'NFC1_NAME_CH', 'NFC12', 'NFC12_NAME', 'NFC12_NAME_CH') \
                        .drop('ATC1', 'ATC1_CODE', 'ATC1_DESC', 'ATC2', 'ATC2_CODE', 'ATC2_DESC', "ATC3", "ATC3_CODE", "ATC3_DESC") \
                        .drop('COMPANY', 'SOURCE', 'TAG', 'PARENT_ID') \
                        .withColumn('EXT', lit('{}')) \
                        .withColumn('STANDARD', lit('COMMON')) \
                        .repartition(1).withColumn("_ID", monotonically_increasing_id()).cache()
	
    product_data_df.repartition("STANDARD").write.format("parquet").mode("overwrite").partitionBy("STANDARD").save(prod_output_path)
