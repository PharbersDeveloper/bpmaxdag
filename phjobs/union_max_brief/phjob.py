# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from ph_logs.ph_logs import phlogger
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql import functions as func
import os
from pyspark.sql.functions import pandas_udf, PandasUDFType

def execute(max_path, path_for_extract_path):
    os.environ["PYSPARK_PYTHON"] = "python3"
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("data from s3") \
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
        # spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
        spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.cn-northwest-1.amazonaws.com.cn")
    
    
    # max_path = "s3a://ph-stream/common/public/max_result/0.0.5/"
    max_standard_brief_all_path = max_path + "/max_standard_brief_all"
    if path_for_extract_path == "Empty":
        path_for_extract_path = max_path + "/path_for_extract.csv"
    
    '''
    只有path_for_extract发生更新，才需要运行该job
    '''
    
    # 根据path_for_extract， 合并项目brief，生成max_standard_brief_all
    
    path_for_extract = spark.read.csv(path_for_extract_path, header=True)
    
    path_all = path_for_extract.toPandas()["path"].tolist()
    path_all_brief = [i + "_brief" for i in path_all]
    
    # 合并项目brief
    # "project", "Date", "标准通用名", "ATC", "DOI"
    index = 0
    for eachpath in path_all_brief:
        df = spark.read.parquet(eachpath)
        if index ==0:
            max_standard_brief_all = df
        else:
            max_standard_brief_all = max_standard_brief_all.union(df)
        index += 1
    
    max_standard_brief_all = max_standard_brief_all.repartition(2)
    max_standard_brief_all.write.format("parquet") \
        .mode("overwrite").save(max_standard_brief_all_path)
