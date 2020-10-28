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
        .appName("datamart dimension alias job") \
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
    
    keys = reading.schema.names
    
    dest = reading \
        .withColumn("P_KEY", lit(keys[0])) \
        .withColumn("T_KEY", lit(keys[1])) \
        .withColumnRenamed(keys[0], "P_VALUE") \
        .withColumnRenamed(keys[1], "T_VALUE")
    
    dest.filter(col("T_VALUE").isNotNull() & (col("T_VALUE") != "#N/A")) \
        .write.mode("overwrite").parquet(output_path)
