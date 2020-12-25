# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from ph_logs.ph_logs import phs3logger


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
    logger.info(kwargs["atc_input"])
    logger.info(kwargs["atc_output"])
    
    
    df = spark.read.csv("s3a://ph-max-auto/2020-08-11/cube/metadata/atcCode1223_alfred.csv", header=True)
    df = df.withColumn("COUNT", lit(1)).withColumn("TMP", lit(1))
    
    # 1. 构建图表的整体长度
    df_act_3 = df.groupBy("act3").agg(sum(df.COUNT).alias("COUNT"))
    df_act_3_factor = df_act_3.select("COUNT").distinct().withColumn("TMP", lit(1))
    df_act_3_factor = df_act_3_factor.groupBy("TMP").agg(pudf_base_factor(df_act_3_factor.COUNT).alias("FACTOR"))
    df_act_3 = df_act_3.crossJoin(df_act_3_factor.drop("TMP"))
    df_act_3 = df_act_3.withColumn("STEP", df_act_3.FACTOR / df_act_3.COUNT)
    return {}
