# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from pyspark.sql.functions import col,count


def execute(**kwargs):
    """
        please input your code below
        get spark session: spark = kwargs["spark"]()
    """
    logger = phs3logger(kwargs["job_id"], LOG_DEBUG_LEVEL)
    logger.info("当前 owner 为 " + str(kwargs["owner"]))
    logger.info("当前 run_id 为 " + str(kwargs["run_id"]))
    logger.info("当前 job_id 为 " + str(kwargs["job_id"]))
    spark = kwargs["spark"]()
    
#     az = spark.read.parquet("s3a://ph-max-auto/2020-08-11/BPBatchDAG/refactor/zyyin/azsanofi/raw_data")

#     az.show(300)
#     print(cpa_az_split.count())
#     az_result = spark.read.csv("s3a://ph-max-auto/2020-08-11/data_matching/refactor/results/2021-03-08_09-09-15/Report",header=True)
#     qilu = spark.read.parquet("s3a://ph-max-auto/2020-08-11/BPBatchDAG/refactor/zyyin/qilu/raw_data2")
#     weicai = spark.read.parquet("s3a://ph-max-auto/2020-08-11/BPBatchDAG/refactor/zyyin/eia/raw_data_2")
#     pf = spark.read.parquet("s3a://ph-max-auto/2020-08-11/BPBatchDAG/refactor/zyyin/pfizer_model/0.0.2/raw_data")
#     sh = spark.read.csv("s3a://ph-max-auto/2020-08-11/data_matching/temp/mzhang/sh_PCHC_sku",header=True,encoding="gbk")
#     sh.write.mode("overwrite").parquet("s3a://ph-max-auto/2020-08-11/data_matching/temp/mzhang/sh_PCHC_sku")
#     az_result = spark.read.csv('s3a://ph-max-auto/2020-08-11/data_matching/refactor/results/2021-03-10_03-17-32/Predictions/',header=True)

    qilu_result = spark.read.csv('s3a://ph-max-auto/2020-08-11/data_matching/refactor/results/2021-03-11_08-16-33/Predictions/',header=True)
    qilu_result.select("label","prediction").show()
    print(qilu_result.printSchema())
    print(qilu_result.count())
    a = qilu_result.filter(col("label")==col("prediction")).count()
    all_count = qilu_result.count()
    rate = round(int(a)/int(all_count) * 100 ,2)  
    print(rate)
    



    return {}
