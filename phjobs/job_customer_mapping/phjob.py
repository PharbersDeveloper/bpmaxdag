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
    df = spark.read.parquet("s3a://ph-stream/common/public/max_result/0.0.5/max_standard/all_report_a")
    df_csv = spark.read.csv("s3a://ph-max-auto/2020-08-11/cube/metadata/2019_TOP50_Companys.csv", header=True)
    #创建视图
    df_csv.createOrReplaceTempView("company")
    df.createOrReplaceTempView("all_report")
    #读取服务时间
    result_time = spark.sql("select project,min(left(time_left,4)) as startTime,max(left(time_right,4)) as currentTime  from  all_report where project is not null  group by project ")
    result_time.show(100)
    spark.sql("select * from company").show(100)
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
    write2postgres(result,"temp.customer")
    
    return {}
