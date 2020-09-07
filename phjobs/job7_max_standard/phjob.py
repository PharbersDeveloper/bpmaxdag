# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""

from phlogs.phlogs import phlogger
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql import functions as func
import os

#def execute(a, b):

spark = SparkSession.builder \
    .master("yarn") \
    .appName("data from s3") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.instance", "1") \
    .config("spark.executor.memory", "1g") \
    .config('spark.sql.codegen.wholeStage', False) \
    .getOrCreate()

access_key = "AKIAWPBDTVEAJ6CCFVCP"
secret_key = "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"
if access_key is not None:
    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
    spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
    # spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
    spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.cn-northwest-1.amazonaws.com.cn")


max_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/"
project_name = "Astellas"

max_result_path_list_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/Astellas/max_result_path_list.csv"
max_result_path_list = spark.read.csv(max_result_path_list_path, header=True)
max_result_path_list = max_result_path_list.toPandas()

for i in  range(len(max_result_path_list)):
    


# max_result_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/Astellas/202006/MAX_result/MAX_result_201401_202006_city_level"
product_map_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/Common_files/product_map_all"
MAX_city_normalize_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/Common_files/MAX_city_normalize.csv"
packID_ACT_map_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/Common_files/packID_ACT_map.csv"

max_result = spark.read.parquet(max_result_path)
product_map = spark.read.parquet(product_map_path)
MAX_city_normalize = spark.read.csv(MAX_city_normalize_path, header=True)
packID_ACT_map = spark.read.csv(packID_ACT_map_path, header=True)

# product
product_map = product_map.where(product_map.project == project_name) \
                .drop("project") \
                .withColumnRenamed("pfc", "PACK_ID")
                
max_standard = max_result.join(product_map, max_city_result["Prod_Name"] == product_map["min2"], how="left") \
                        .drop("min2")

# city
max_standard = max_standard.join(MAX_city_normalize, on=["Province", "City"], how="left")

# ACT
max_standard = max_standard.join(packID_ACT_map, on=["PACK_ID"], how="left")







    
    