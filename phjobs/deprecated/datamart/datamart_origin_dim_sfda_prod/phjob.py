# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""
import pandas as pd
import boto3
import io
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, monotonically_increasing_id
import logging
import os


def s3excel2df(spark, source_bucket, source_path):
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        
    s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    object_file = s3_client.get_object(Bucket=source_bucket, Key=source_path)
    data = object_file['Body'].read()
    pd_df = pd.read_excel(io.BytesIO(data), encoding='utf-8')
        
    return spark.createDataFrame(pd_df.astype(str))
    
    
def execute(**kwargs):
    """
        please input your code below
    """
    logging.basicConfig(format='%(asctime)s %(filename)s %(funcName)s %(lineno)d %(message)s')
    logger = logging.getLogger('driver_logger')
    logger.setLevel(logging.INFO)
    logger.info("Origin kwargs = {}.".format(str(kwargs)))
    
    # input required
    origin_prod_path = kwargs.get('origin_prod_path', '')
    if origin_prod_path == 'not set' or origin_prod_path == '':
        raise Exception("Invalid origin_prod_path!", origin_prod_path)
        
    national_drug_code_domestically_path = kwargs.get('national_drug_code_domestically_path', '')
    if national_drug_code_domestically_path == 'not set' or national_drug_code_domestically_path == '':
        raise Exception("Invalid national_drug_code_domestically_path!", national_drug_code_domestically_path)
        
    national_drug_code_imported_path = kwargs.get('national_drug_code_imported_path', '')
    if national_drug_code_imported_path == 'not set' or national_drug_code_imported_path == '':
        raise Exception("Invalid national_drug_code_imported_path!", national_drug_code_imported_path)
    
    # output    
    origin_dim_sfda_prods_path = kwargs.get('origin_dim_sfda_prods_path', '')
    if origin_dim_sfda_prods_path == 'not set' or origin_dim_sfda_prods_path == '':
        raise Exception("Invalid origin_dim_sfda_prods_path!", origin_dim_sfda_prods_path)
    
    flat_table_prod_path = kwargs.get('flat_table_prod_path', '')
    if flat_table_prod_path == 'not set' or flat_table_prod_path == '':
        raise Exception("Invalid flat_table_prod_path!", flat_table_prod_path)
    
    os.environ["PYSPARK_PYTHON"] = "python3"
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("datamart dimension input max hosp job") \
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
        
    # root
#  |-- PACK_ID: string (nullable = true)
#  |-- MOLE_NAME_EN: string (nullable = true)
#  |-- MOLE_NAME_CH: string (nullable = true)
#  |-- PROD_DESC: string (nullable = true)
#  |-- PROD_NAME_CH: string (nullable = true)
#  |-- CORP_NAME_EN: string (nullable = true)
#  |-- CORP_NAME_CH: string (nullable = true)
#  |-- MNF_NAME_EN: string (nullable = true)
#  |-- MNF_NAME_CH: string (nullable = true)
#  |-- PCK_DESC: string (nullable = true)
#  |-- DOSAGE: string (nullable = true)
#  |-- SPEC: string (nullable = true)
#  |-- PACK: string (nullable = true)
#  |-- NFC123: string (nullable = true)
#  |-- NFC123_NAME: string (nullable = true)
#  |-- CORP_ID: string (nullable = true)
#  |-- MNF_TYPE: string (nullable = true)
#  |-- MNF_TYPE_NAME: string (nullable = true)
#  |-- MNF_TYPE_NAME_CH: string (nullable = true)
#  |-- MNF_ID: string (nullable = true)
#  |-- ATC4_CODE: string (nullable = true)
#  |-- ATC4_DESC: string (nullable = true)
#  |-- ATC4: string (nullable = true)
    # origin_prod_df = spark.read.parquet(origin_prod_path) \
    #     .select("PACK_ID","MOLE_NAME_EN","MOLE_NAME_CH","PROD_DESC","PROD_NAME_CH","CORP_NAME_EN","CORP_NAME_CH","MNF_NAME_EN","MNF_NAME_CH", \
    #     "PCK_DESC","DOSAGE","SPEC","PACK","NFC123","NFC123_NAME","CORP_ID","MNF_TYPE","MNF_TYPE_NAME","MNF_TYPE_NAME_CH","MNF_ID","ATC4_CODE", \
    #     "ATC4_DESC","ATC4").distinct()
    # origin_prod_df.show()
    # logger.info("origin_prod_df.count() = {}.".format(origin_prod_df.count())) # 44161
    # logger.info("join_key_df.count() = {}.".format(origin_prod_df.select("PROD_NAME_CH", "MNF_NAME_CH", "MNF_NAME_EN", "DOSAGE", "SPEC").distinct().count())) # 33932
    
    domestically_path_lst = national_drug_code_domestically_path.split('/')
    domestically_bucket = domestically_path_lst[2]
    domestically_path = "/".join(domestically_path_lst[3:])
    domestically_df = s3excel2df(spark, source_bucket=domestically_bucket, source_path=domestically_path).drop("ID")
    
    # domestically_df.printSchema()
    # domestically_df.show()
    # logger.info("domestically_df.count() = {}.".format(domestically_df.count())) # 163407
    
    imported_path_lst = national_drug_code_imported_path.split('/')
    imported_bucket = imported_path_lst[2]
    imported_path = "/".join(imported_path_lst[3:])
    imported_df = s3excel2df(spark, source_bucket=imported_bucket, source_path=imported_path).drop("ID")
    
    # imported_df.printSchema()
    # imported_df.show()
    # logger.info("imported_df.count() = {}.".format(imported_df.count())) # 3589
    
    national_drug_code_df = domestically_df.unionByName(imported_df).distinct()
    # national_drug_code_df.show()
    # logger.info("national_drug_code_df.count() = {}.".format(national_drug_code_df.count())) # 166987
    # logger.info("join_key_df.count() = {}.".format(national_drug_code_df.select("PRODUCT_NAME", "MANUFACTURER_CN", "MANUFACTURER_EN", "DOSAGE", "SPEC").distinct().count())) # 164681
    
    # 使用国家药品编码大全与数据仓储中的产品按照共有Key进行匹配，目前没有交集。
    # cond = [
    #     national_drug_code_df.PRODUCT_NAME == origin_prod_df.PROD_NAME_CH,      # 4741743
    #     national_drug_code_df.MANUFACTURER_CN == origin_prod_df.MNF_NAME_CH,    # 30142
    #     national_drug_code_df.MANUFACTURER_EN == origin_prod_df.MNF_NAME_EN,    # 0
    #     national_drug_code_df.DOSAGE == origin_prod_df.DOSAGE,                  # 6337
    #     national_drug_code_df.SPEC == origin_prod_df.SPEC,                      # 0
    #     ]
    # flat_table_df = national_drug_code_df.join(origin_prod_df, cond, "full")
    # logger.info("flat_table_df.count() = {}.".format(flat_table_df.count())) # 211148
    
    origin_dim_sfda_prods_df = national_drug_code_df.withColumn("_ID", monotonically_increasing_id())
    origin_dim_sfda_prods_df.write.format("parquet").mode('overwrite').save(origin_dim_sfda_prods_path)
    