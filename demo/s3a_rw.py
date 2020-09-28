import io
import os
import boto3
import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf

access_key = "AKIAWPBDTVEAJ6CCFVCP"
secret_key = "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"

SOURCE_BUCKET = 'ph-origin-files'
SOURCE_PATH = 'prod/Product standardization master data-A-S-0827.xlsx'
TARGET_BUCKET = 'ph-stream'
TARGET_PATH = 'common/public/prod/0.0.15'


s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
object_file = s3_client.get_object(Bucket=SOURCE_BUCKET, Key=SOURCE_PATH)
data = object_file['Body'].read()
pd_df = pd.read_excel(io.BytesIO(data), encoding='utf-8')

os.environ["PYSPARK_PYTHON"] = "python3"
spark = SparkSession.builder \
    .master("yarn") \
    .appName("data cube cal measures") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.instance", "4") \
    .config("spark.executor.memory", "2g") \
    .config('spark.sql.codegen.wholeStage', False) \
    .getOrCreate()

if access_key is not None:
    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
    spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
    # spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
    spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.cn-northwest-1.amazonaws.com.cn")

sdf = spark.createDataFrame(pd_df.astype(str)).drop('Unnamed: 39') \
    .withColumn('COMPANY', sf.lit('product')) \
    .withColumn('SOURCE', sf.lit('product')) \
    .withColumn('TAG', sf.lit(SOURCE_PATH.split('/')[-1])) \
    .withColumn('version', sf.lit('0.0.15'))
sdf.show()
save_path = "s3a://%s/%s" % (TARGET_BUCKET, TARGET_PATH)
sdf.write.format("parquet").mode("overwrite").save(save_path)
