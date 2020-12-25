# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from ph_logs.ph_logs import phs3logger
from pyspark.sql.functions import array
from pyspark.sql.functions import explode
import os
import logging

'''
写入操作
'''    
def write2postgres(df,pgTable):
    logging.info("write postgresql start")
    url = "jdbc:postgresql://ph-db-lambda.cngk1jeurmnv.rds.cn-northwest-1.amazonaws.com.cn/phreports"
    df.write.format("jdbc") \
    .option("url", url) \
    .option("dbtable", pgTable) \
    .option("user", "pharbers") \
    .option("password", "Abcde196125") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()
    logging.info("write postgresql end")


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
        spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
        spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
        spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.cn-northwest-1.amazonaws.com.cn")

    return spark

def execute(**kwargs):
    """
        please input your code below
        get spark session: spark = kwargs["spark"]()
    """
    logger = phs3logger(kwargs["job_id"])
    logger.info("当前 owner 为 " + str(kwargs["owner"]))
    logger.info("当前 run_id 为 " + str(kwargs["run_id"]))
    logger.info("当前 job_id 为 " + str(kwargs["job_id"]))

    spark = kwargs["spark"]()
    # logger.info(kwargs["a"])
    # logger.info(kwargs["b"])
    # logger.info(kwargs["c"])
    # logger.info(kwargs["d"])
    
    #读取文件
    df = spark.read.parquet(kwargs['input_report'])
    df_csv = spark.read.csv(kwargs['input_csv'], header=True)
    #创建视图
    df_csv.createOrReplaceTempView("company")
    df.createOrReplaceTempView("all_report")
    #读取服务时间
    result_time = spark.sql("select project,min(left(time_left,4)) as startTime,max(left(time_right,4)) as currentTime  from  all_report where project is not null  group by project ")
    
    result_time.createOrReplaceTempView("result_time")
    #生成结果
    result = spark.sql(" select  result_time.project, company.Rank, company.CN, result_time.startTime, result_time.currentTime from  result_time  left join  company on result_time.project=company.Alias")
    #字段处理
    result.createOrReplaceTempView("result")
    result = spark.sql("select project , case when CN is not null then CN else project  end as company, case when Rank is null then cast((random()*100) as int)+50 else cast(Rank as int ) end as rank, startTime , currentTime from result order by rank asc")
    
    result = result.withColumn("DATE", array(result.startTime, result.currentTime)).drop("startTime", "currentTime")
    result = result.withColumn("DATE", explode(result.DATE))
    result.show(100)
    
    #写入数据库
    write2postgres(result,kwargs['output'])
    
    return {}
