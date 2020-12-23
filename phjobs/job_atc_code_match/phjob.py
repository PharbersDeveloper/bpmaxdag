# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from ph_logs.ph_logs import phs3logger
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import os
from pyspark.sql.functions import  when
from pyspark.sql.functions import col
import logging
from pyspark.sql.functions import lit

def prepare():
    sparkClassPath = os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.postgresql:postgresql:42.2.14 pyspark-shell'
    os.environ["PYSPARK_PYTHON"] = "python3"
    # 读取s3桶中的数据
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("sample data 2 postgresql") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.cores", "2") \
        .config("spark.executor.instances", "2") \
        .config("spark.executor.memory", "2g") \
        .config('spark.sql.codegen.wholeStage', False) \
        .config("spark.driver.extraClassPath", sparkClassPath) \
        .getOrCreate()

    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    if access_key is not None:
        spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
        spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
        spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
        spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.cn-northwest-1.amazonaws.com.cn")

    return spark
'''
写入操作
'''
def write2postgres(df, pgTable):
    logging.info("write postgresql start")
    url = "jdbc:postgresql://ph-db-lambda.cngk1jeurmnv.rds.cn-northwest-1.amazonaws.com.cn/phreports"
    # spark.read.csv(s3Path, header=True) \
    df.write.format("jdbc") \
        .option("url", url) \
        .option("dbtable", pgTable) \
        .option("user", "pharbers") \
        .option("password", "Abcde196125") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()
    logging.info("write postgresql end")

def execute(**kwargs):
    """
        please input your code below
        get spark session: spark = kwargs["spark"]()
    """
    
    logger = phs3logger(kwargs["job_id"])
    logger.info("当前 owner 为 " + str(kwargs["owner"]))
    logger.info("当前 run_id 为 " + str(kwargs["run_id"]))
    logger.info("当前 job_id 为 " + str(kwargs["job_id"]))
   
    spark = prepare()
    '''
    读取文件
    '''
    df=spark.read.parquet("s3a://ph-stream/common/public/max_result/0.0.5/max_standard/all_report_a")
    df_csv=spark.read.csv("s3a://ph-max-auto/2020-08-11/cube/metadata/Prod_Ref_202010.csv", header=True)
    '''
    创建视图
    '''
    df_csv.createOrReplaceTempView("df_csv")
    df.createOrReplaceTempView("df_report")
    '''
    执行sql逻辑查询
    '''
    result=spark.sql(" select distinct  a.*,case when a.ACT5=b.ATC then 1 else 0 end as flag from  (select distinct  left(ATC4_Code,3) as act3, ATC4_Code as act5 from df_csv ) as a ,(select  distinct  ATC  from df_report) as b order by act3 ")

    '''
    列名操作
    '''
    result.createOrReplaceTempView("result")
    result=result.withColumn('act3CN', lit(None).cast(StringType())).withColumn('act5CN', lit(None).cast(StringType()))
    result=result.select("act3","act5","act3CN","act5CN","flag")
    '''
    写入数据库
    '''
    # write2postgres(result,"powerbi.atcCode")

    return {}
