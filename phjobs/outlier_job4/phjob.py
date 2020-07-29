# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as func
from phlogs.phlogs import phlogger
import os
from pyspark.sql.functions import udf, from_json
import json

from pyspark.sql.functions import udf
import numpy as np
import pandas as pd

def execute(a, b):
    
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
    doi = "AZ16"
    df_ims_shr_path = u"s3a://ph-max-auto/v0.0.1-2020-06-08/AZ/outlier/"+doi+"/df_ims_shr"
    prd_input = [u"普米克令舒", u"Others-Pulmicort", u"益索"]

    # 输出
    df_ims_shr_res_path = u"s3a://ph-max-auto/v0.0.1-2020-06-08/AZ/outlier/" + doi + "/df_ims_shr_res"
    df_cities_path = u"s3a://ph-max-auto/v0.0.1-2020-06-08/AZ/outlier/" + doi + "/df_cities"
    
    # ============== 函数定义 ================
    
    def gen_poi_with_input(prds):
        result = []
        for it in prds:
            result.append((1, it))
        return result
    
    # ============== 数据执行 ================ 
    # 数据读取
    df_ims_shr = spark.read.parquet(df_ims_shr_path)
    
    # ims 个城市产品市场份额：max_outlier_ims_shr_job
    df_ims_shr = df_ims_shr.where(
        (df_ims_shr.city != "CHPA")
        & (df_ims_shr.city != 'KEY-54')
        & (df_ims_shr.city != 'ROC-54')
        # & (df_ims_shr.city == "珠三角市")
    )
    df_cities = df_ims_shr.select("city").distinct().withColumn("key", func.lit(1))

    # prd_input = [u"加罗宁", u"凯纷", u"诺扬"]
    schema = StructType([StructField("key", IntegerType(), True), StructField("poi", StringType(), True)])
    df_tmp_poi = spark.createDataFrame(gen_poi_with_input(prd_input), schema)

    df_ct_pd = df_cities.join(df_tmp_poi, on="key", how="outer") \
        .withColumn("ims_share", func.lit(0)) \
        .withColumn("ims_poi_vol", func.lit(0)) \
        .drop("key")
    # df_ct_pd.show()

    df_ims_shr_mkt = df_ims_shr.groupBy("city").sum("ims_poi_vol").withColumnRenamed("sum(ims_poi_vol)", "ims_mkt_vol")
    df_ims_shr_poi = df_ims_shr.union(df_ct_pd)
    df_ims_shr_res = df_ims_shr_mkt.join(df_ims_shr_poi, on="city").groupby(["city", "poi"]) \
        .agg({"ims_share": "sum", "ims_poi_vol": "sum", "ims_mkt_vol": "first"}) \
        .withColumnRenamed("sum(ims_poi_vol)", "ims_poi_vol") \
        .withColumnRenamed("sum(ims_share)", "ims_share") \
        .withColumnRenamed("first(ims_mkt_vol)", "ims_mkt_vol")

    # 输出结果
    df_ims_shr_res = df_ims_shr_res.repartition(2)
    df_ims_shr_res.write.format("parquet") \
        .mode("overwrite").save(df_ims_shr_res_path)
    
    df_cities = df_cities.repartition(2)
    df_cities.write.format("parquet") \
        .mode("overwrite").save(df_cities_path)
        
    return [df_ims_shr_res, df_cities]
    