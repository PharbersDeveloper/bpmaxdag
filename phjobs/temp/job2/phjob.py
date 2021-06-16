# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from pyspark.sql.functions import col,count
from pyspark.sql.types import DoubleType


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
    

    path_negative = r"s3a://ph-max-auto/2020-08-11/data_matching/refactor/results/2021-04-08_00-56-39/Negative_Predictions/"
#     path_negative = r"s3a://ph-max-auto/2020-08-11/data_matching/refactor/results/2021-04-08_12-11-26/Negative_Predictions/"
    path_az = r"s3a://ph-max-auto/2020-08-11/BPBatchDAG/refactor/zyyin/azsanofi/raw_data"
#     path_az = r"s3a://ph-max-auto/2020-08-11/data_matching/refactor/data/CHC/*"

    df_negative = spark.read.csv(path_negative,header=True)
    df_negative = df_negative.withColumn("PACK_ID_CHECK",col("PACK_ID_CHECK").cast("double"))
    print(df_negative.printSchema())
    df_az = spark.read.parquet(path_az)
    
    df_az = df_az.withColumnRenamed("MOLE_NAME","MOLE_NAME_ORIGINAL")\
                .withColumnRenamed("PACK_QTY","PACK_QTY_ORIGINAL")\
                .withColumnRenamed("PACK_ID_CHECK","PACK_ID_CHECK_ORIGINAL")\
                .withColumnRenamed("SPEC","SPEC_ORIGINAL")
                
    
    df_az = df_az.withColumn("PACK_ID_CHECK_ORIGINAL",col("PACK_ID_CHECK_ORIGINAL").cast("double"))
    df_az = df_az.select("MOLE_NAME_ORIGINAL","PACK_QTY_ORIGINAL","PACK_ID_CHECK_ORIGINAL","SPEC_ORIGINAL")
    print(df_az.printSchema())
    df = df_negative.join(df_az,df_negative.PACK_ID_CHECK == df_az.PACK_ID_CHECK_ORIGINAL,"left")
    df = df.filter(col("PACK_ID_STANDARD")==col("PACK_ID_CHECK_ORIGINAL"))
    print(df.printSchema())
    print(df.count())
    final_path = r"s3a://ph-max-auto/2020-08-11/data_matching/temp/mzhang/0408_az"
#     df.repartition(1).write.mode("overwrite").csv(final_path,header=True)
    

    return {}
