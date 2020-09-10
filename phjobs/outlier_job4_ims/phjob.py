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

def execute(max_path, project_name, out_path, out_dir, doi, product_input):
    os.environ["PYSPARK_PYTHON"] = "python2"    
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

    out_path_dir = out_path + "/" + project_name + '/' + out_dir + '/' + doi
    
    # 输入
    df_ims_share_path = out_path_dir + "/df_ims_share"
    product_input = product_input.replace(" ","").split(',')

    # 输出
    df_ims_share_res_path = out_path_dir + "/df_ims_share_res"
    df_cities_path = out_path_dir + "/df_cities"
    
    # ============== 函数定义 ================
    
    def gen_poi_with_input(prds):
        result = []
        for it in prds:
            result.append((1, it))
        return result
    
    # ============== 数据执行 ================ 
    
    phlogger.info('数据执行-start')
    
    # 数据读取
    df_ims_share = spark.read.parquet(df_ims_share_path)
    
    # ims 个城市产品市场份额：max_outlier_ims_shr_job
    df_ims_share = df_ims_share.where(
        (df_ims_share.city != "CHPA")
        & (df_ims_share.city != 'KEY-54')
        & (df_ims_share.city != 'ROC-54')
        # & (df_ims_share.city == "珠三角市")
    )
    df_cities = df_ims_share.select("city").distinct().withColumn("key", func.lit(1))

    # product_input = [u"加罗宁", u"凯纷", u"诺扬"]
    schema = StructType([StructField("key", IntegerType(), True), StructField("poi", StringType(), True)])
    df_tmp_poi = spark.createDataFrame(gen_poi_with_input(product_input), schema)

    df_ct_pd = df_cities.join(df_tmp_poi, on="key", how="outer") \
        .withColumn("ims_share", func.lit(0)) \
        .withColumn("ims_poi_vol", func.lit(0)) \
        .drop("key")
    # df_ct_pd.show()

    df_ims_share_mkt = df_ims_share.groupBy("city").sum("ims_poi_vol").withColumnRenamed("sum(ims_poi_vol)", "ims_mkt_vol")
    df_ims_share_poi = df_ims_share.union(df_ct_pd)
    df_ims_share_res = df_ims_share_mkt.join(df_ims_share_poi, on="city").groupby(["city", "poi"]) \
        .agg({"ims_share": "sum", "ims_poi_vol": "sum", "ims_mkt_vol": "first"}) \
        .withColumnRenamed("sum(ims_poi_vol)", "ims_poi_vol") \
        .withColumnRenamed("sum(ims_share)", "ims_share") \
        .withColumnRenamed("first(ims_mkt_vol)", "ims_mkt_vol")

    # 输出结果
    df_ims_share_res = df_ims_share_res.repartition(2)
    df_ims_share_res.write.format("parquet") \
        .mode("overwrite").save(df_ims_share_res_path)
    
    df_cities = df_cities.repartition(2)
    df_cities.write.format("parquet") \
        .mode("overwrite").save(df_cities_path)
        
    phlogger.info('数据执行-Finish')
        
    return [df_ims_share_res, df_cities]
    