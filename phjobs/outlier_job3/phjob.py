# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as func
from phlogs.phlogs import phlogger
import os


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
    df_EIA_path =  u"s3a://ph-max-auto/v0.0.1-2020-06-08/AZ/outlier/" + doi + "/df_EIA"
    df_universe_path = u"s3a://ph-max-auto/v0.0.1-2020-06-08/AZ/outlier/"+doi+"/df_universe"
    df_PHA_city_path = u"s3a://ph-max-auto/v0.0.1-2020-06-08/AZ/outlier/"+doi+"/df_PHA_city"
    
    # 输出
    df_pnl_path = u"s3a://ph-max-auto/v0.0.1-2020-06-08/AZ/outlier/" + doi + "/df_pnl"
    df_pnl_mkt_path = u"s3a://ph-max-auto/v0.0.1-2020-06-08/AZ/outlier/" + doi + "/df_pnl_mkt"
    
    # 数据读取
    df_EIA = spark.read.parquet(df_EIA_path)
    df_uni = spark.read.parquet(df_universe_path)
    df_PHA_city = spark.read.parquet(df_PHA_city_path)
    
    # 对Panel医院的数据整理， 用户factor的计算：max_outlier_pnl_job
    df_panel_hos = df_uni.where(df_uni.PANEL == 1).select("HOSP_ID").withColumnRenamed("HOSP_ID", "HOSP_ID1")
    if("City" in df_EIA.columns):
        df_EIA = df_EIA.drop("City")
    df_pnl = df_EIA.join(df_PHA_city, df_EIA.HOSP_ID == df_PHA_city.Panel_ID, how="left").\
        join(df_panel_hos, df_EIA.HOSP_ID == df_panel_hos.HOSP_ID1, how="right")

    df_pnl = df_pnl.groupBy(["City", "POI"]).sum("Sales").withColumnRenamed("sum(Sales)", "Sales")
    df_pnl.persist()

    df_pnl_mkt = df_pnl.groupBy("City").agg({"Sales": "sum"}) \
        .withColumnRenamed("sum(Sales)", "Sales_pnl_mkt")
        
    df_pnl = df_pnl.withColumnRenamed("Sales", "Sales_pnl")

    df_pnl = df_pnl.withColumnRenamed("City", "city") \
        .withColumnRenamed("Sales_pnl_mkt", "sales_pnl_mkt") \
        .withColumnRenamed("POI", "poi") \
        .withColumnRenamed("Sales_pnl", "sales_pnl")

    df_pnl_mkt = df_pnl_mkt.withColumnRenamed("City", "city") \
        .withColumnRenamed("Sales_pnl_mkt", "sales_pnl_mkt")
    
    # 输出结果
    df_pnl = df_pnl.repartition(2)
    df_pnl.write.format("parquet") \
        .mode("overwrite").save(df_pnl_path)
        
    df_pnl_mkt = df_pnl_mkt.repartition(2)
    df_pnl_mkt.write.format("parquet") \
        .mode("overwrite").save(df_pnl_mkt_path)
    
    return [df_pnl, df_pnl_mkt]