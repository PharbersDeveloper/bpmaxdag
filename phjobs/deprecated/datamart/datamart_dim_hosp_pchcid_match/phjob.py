# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, udf, monotonically_increasing_id
import logging
import os
import pandas as pd
import boto3
import io


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

    os.environ["PYSPARK_PYTHON"] = "python3"
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("datamart dimension hosp pha_id match") \
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
    
    source_bucket = input_path.split('/')[2]
    source_path = "/".join(input_path.split('/')[3:])
    
    s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    object_file = s3_client.get_object(Bucket=source_bucket, Key=source_path)
    data = object_file['Body'].read()
    pd_df = pd.read_excel(io.BytesIO(data), encoding='utf-8')
    pd_df = pd_df[['旧版PCHC_Code', '重复']]
    
    df = spark.createDataFrame(pd_df.astype(str)) \
            .withColumnRenamed('旧版PCHC_Code', 'OLD_ID') \
            .withColumnRenamed('重复', 'NEW_ID')
            
    df = df.filter(df.NEW_ID != '0')
    df.write.format("parquet").mode("overwrite").save(output_path)