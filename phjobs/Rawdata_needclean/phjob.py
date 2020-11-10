# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""
from ph_logs.ph_logs import phlogger
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql import functions as func
import os
from pyspark.sql.functions import pandas_udf, PandasUDFType
import time

def execute(max_path, project_name, outdir, minimum_product_sep, minimum_product_columns, test):
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
        
    # 输入
    '''
    minimum_product_sep = '|'
    minimum_product_columns = "Brand, Form, Specifications, Pack_Number, Manufacturer"
    outdir = '202008'
    project_name = 'Gilead'
    '''
    if minimum_product_sep == "kong":
        minimum_product_sep = ""
    minimum_product_columns = minimum_product_columns.replace(" ","").split(",")
    product_map_path = max_path + '/' + project_name + '/' + outdir + '/prod_mapping'
    
    if test == 'True':
        all_raw_data_path = max_path + '/' + project_name + '/' + outdir + '/raw_data_check/raw_data'
    else:
        all_raw_data_path = max_path + '/' + project_name + '/' + outdir + '/raw_data'
    
    # 输出
    need_clean_path = max_path + '/' + project_name + '/' + outdir + '/raw_data_check/need_cleaning_raw.csv'
    
    # =========  数据执行  =============
    all_raw_data = spark.read.parquet(all_raw_data_path)
    
    # 待清洗
    clean = all_raw_data.select('Brand','Form','Specifications','Pack_Number','Manufacturer','Molecule','Corp','Route','Path','Sheet').distinct()
    clean = clean.withColumn('Brand', func.when((clean.Brand.isNull()) | (clean.Brand == 'NA'), clean.Molecule).otherwise(clean.Brand))
    
    clean = clean.withColumn("min1", func.when(clean[minimum_product_columns[0]].isNull(), func.lit("NA")).
                                       otherwise(clean[minimum_product_columns[0]]))
    for col in minimum_product_columns[1:]:
        clean = clean.withColumn(col, clean[col].cast(StringType()))
        clean = clean.withColumn("min1", func.concat(
            clean["min1"],
            func.lit(minimum_product_sep),
            func.when(func.isnull(clean[col]), func.lit("NA")).otherwise(clean[col])))
    
    
    product_map = spark.read.parquet(product_map_path)
    product_map = product_map.distinct() \
                        .withColumn("min1", func.regexp_replace("min1", "&amp;", "&")) \
                        .withColumn("min1", func.regexp_replace("min1", "&lt;", "<")) \
                        .withColumn("min1", func.regexp_replace("min1", "&gt;", ">"))
                        
    need_clean = clean.join(product_map.select('min1').distinct(), on='min1', how='left_anti')
    if need_clean.count() > 0:
        need_clean = need_clean.repartition(1)
        need_clean.write.format("csv").option("header", "true") \
            .mode("overwrite").save(need_clean_path) 
