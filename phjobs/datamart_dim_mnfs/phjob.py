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
        
    # output required
    mnfs_output_path = kwargs['mnfs_output_path']
    if mnfs_output_path == 'not set':
        raise Exception("Invalid mnfs_output_path!", mnfs_output_path)

    spark = SparkSession.builder \
        .master("yarn") \
        .appName("datamart dimension mnfs job") \
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
    
    reading = spark.read.parquet(prod_input_path)
    
    reading = reading.select(
			"MNF_ID", 
			"MNF_TYPE", 
			"MNF_TYPE_NAME", 
			"MNF_TYPE_NAME_CH", 
			"MNF_NAME_EN", 
			"MNF_NAME_CH"
		).distinct().withColumn("_ID", F.monotonically_increasing_id()) \
		.withColumnRenamed("MNF_TYPE_NAME", "MNF_TYPE_NAME_EN") \
		.withColumn("TAG", F.lit("")) \
		.withColumn("PARENT_ID", F.lit(""))
    
    reading.write.format("parquet").mode("overwrite").save(mnfs_output_path)
