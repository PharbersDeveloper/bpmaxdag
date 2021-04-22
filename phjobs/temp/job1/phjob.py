# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
import time
from pyspark.sql.functions import regexp_extract , regexp_replace, upper ,concat_ws ,count , max ,col
from nltk.tokenize import sent_tokenize
from nltk.tokenize import word_tokenize

def execute(**kwargs):
    """
        please input your code below
        get spark session: spark = kwargs["spark"]()
    """
###############=======configure=======================############
    logger = phs3logger(kwargs["job_id"], LOG_DEBUG_LEVEL)
    logger.info("当前 owner 为 " + str(kwargs["owner"]))
    logger.info("当前 run_id 为 " + str(kwargs["run_id"]))
    logger.info("当前 job_id 为 " + str(kwargs["job_id"]))
    spark = kwargs["spark"]()
################=======configure=====================##############  

##############---------input-----------------------###############

#############----------input----------------------################


#############------loading files-------------------#################

############------loading files--------------------#################


#######################-------test--------------##################
#     url_pos = r"s3a://ph-max-auto/2020-08-11/data_matching/refactor/results/2021-04-21_10-18-15/Positive_Predictions/"
#     df_pos = spark.read.csv(url_pos,header=True)
    
#     url_raw = r"s3a://ph-max-auto/2020-08-11/data_matching/refactor/runs/manual__2021-04-21T09_34_29.804083+00_00/cleaning_data_normalization/cleaning_origin/"
#     df_raw = spark.read.parquet(url_raw)
#     print(df_pos.printSchema())
#     print(df_raw.printSchema())
#     df_raw = df_raw.withColumnRenamed("MOLE_NAME","MOLE_ORIGINAL")\
#                     .withColumnRenamed("PRODUCT_NAME","PRODUCT_ORIGINAL")\
#                     .withColumnRenamed("SPEC","SPEC_ORIGINAL")\
#                     .withColumnRenamed("DOSAGE","DOSAGE_ORIGINAL")\
#                     .withColumnRenamed("MANUFACTURER_NAME","MANUFACTURER_NAME_ORIGINAL")\
#                     .withColumnRenamed("PACK_ID_CHECK","PACK_ID_CHECK_ORIGINAL_")\
#                     .withColumnRenamed("ID","ID_ORIGINAL")\
#                     .withColumnRenamed("pack_qty","PACK_QTY_ORIGINAL_")
#     cols = ["MOLE_ORIGINAL",\
#            "PRODUCT_ORIGINAL",\
#            "SPEC_ORIGINAL",\
#            "DOSAGE_ORIGINAL",\
#            "MANUFACTURER_NAME_ORIGINAL",\
#            "PACK_ID_CHECK_ORIGINAL_",\
#            "ID_ORIGINAL",\
#            "PACK_QTY_ORIGINAL_"]
#     df_raw = df_raw.select(cols)
#     df_raw.show(100)
#     print(df_raw.printSchema())
#     df = df_pos.join(df_raw,df_pos.ID == df_raw.ID_ORIGINAL,'left')
#     df.repartition(1).write.mode("overwrite").csv("s3a://ph-max-auto/2020-08-11/data_matching/temp/mzhang/0421_pos_test",header=True)
    
    url = r"s3a://ph-max-auto/2020-08-11/data_matching/refactor/runs/manual__2021-04-21T09_34_29.804083+00_00/cross_join_cutting/cross_result/"
    df = spark.read.parquet(url)
    df = df.select("ID").distinct()
    print(df.count())
    
#####################-------test---------------################

    
    return {}
