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
    
    #cpa_az = spark.read.parquet("s3a://ph-max-auto/2020-08-11/BPBatchDAG/refactor/zyyin/azsanofi/raw_data")
    cpa_az_split = spark.read.parquet("s3a://ph-max-auto/2020-08-11/BPBatchDAG/refactor/zyyin/azsanofi/0.0.15/splitdata")
    #dosage_mapping = spark.read.parquet("s3a://ph-max-auto/2020-08-11/data_matching/refactor/data/DOSAGE_MAPPING/CPA/V0.0.2")
    cpa_az_split.show(500)
    print(cpa_az_split.count())
    #cpa_az_split.select("SPEC").distinct().show(200)
    return {}
