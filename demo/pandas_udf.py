# -*- coding: utf-8 -*-

import os
import pandas as pd
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, pandas_udf, udf
from pyspark.sql.types import StringType

os.environ["PYSPARK_PYTHON"] = "python3"
spark = SparkSession.builder \
    .master("yarn") \
    .appName("pandas_udf_demo") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.instance", "1") \
    .config("spark.executor.memory", "1g") \
    .config('spark.sql.codegen.wholeStage', False) \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

# access_key = os.getenv("AWS_ACCESS_KEY_ID")
# secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
access_key = "AKIAWPBDTVEAJ6CCFVCP"
secret_key = "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"
if access_key is not None:
    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
    spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
    # spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
    spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.cn-northwest-1.amazonaws.com.cn")


test_data_path = 's3a://ph-max-auto/2020-08-11/BPBatchDAG/cpa_match/'
test_data_df = spark.read.parquet(test_data_path)

replace_udf = pandas_udf(lambda x: x.str.replace('g', 'GGG'), returnType=StringType())
test_data_df = test_data_df.select('PRODUCT_NAME', replace_udf('PRODUCT_NAME').alias('udf'))
test_data_df.show(4)
