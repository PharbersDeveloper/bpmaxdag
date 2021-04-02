# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
import uuid
import numpy as np
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.functions import col, when, split,count
from pyspark.sql.functions import array_distinct, array
from pyspark.ml.linalg import Vectors ,VectorUDT
from pyspark.ml.feature import StopWordsRemover


def execute(**kwargs):
    """
        please input your code below
        get spark session: spark = kwargs["spark"]()
    """
###################=======configure==========#################
    logger = phs3logger(kwargs["job_id"], LOG_DEBUG_LEVEL)
    spark = kwargs["spark"]()
    logger.info(kwargs)
###################=======configure==========#################
    
###################=======input==========#################
    depends = get_depends_path(kwargs)
    path_cross_result = depends["input_cross_result"]
    path_mapping_path = kwargs["dosage_mapping_path"]
    g_repartition_shared = int(kwargs["g_repartition_shared"])
    
###################=======input==========#################

###################=======output==========################# 
    job_id = get_job_id(kwargs)
    run_id = get_run_id(kwargs)
    result_path_prefix = get_result_path(kwargs, run_id, job_id)
    result_path = result_path_prefix + kwargs["mapping_dosage_result"]
###################=======output==========#################

###################=======loading files==========#################
    df_cross_dosage = load_cross_result(spark, path_cross_result)
    df_mapping_dosage = loading_files(spark, path_mapping_path)
    
####################=======loading files==========#################

####################=======main functions==========#################

    df_dosage = join_maping_table(df_cross_dosage=df_cross_dosage,\
                                  df_mapping_dosage=df_mapping_dosage,\
                                 left_key="DOSAGE_STANDARD",
                                 right_key="DOSAGE_STANDARD")
    
# ####################=======main functions==========#################

# ####################### == RESULT == #####################

    df_dosage.repartition(g_repartition_shared).write.mode("overwrite").parquet(result_path)
    
####################### == RESULT == #####################
    return {}


########### === FUNCTIONS === ###########

###################  中间文件与结果文件路径  ######################
def get_run_id(kwargs):
    run_id = kwargs["run_id"]
    if not run_id:
        run_id = "runid_" + "alfred_runner_test"
    return run_id

def get_job_id(kwargs):
    job_name = kwargs["job_name"]
    job_id = kwargs["job_id"]
    if not job_id:
        job_id = "jobid_" + uuid.uuid4().hex
    return job_name # + "_" + job_id 

def get_result_path(kwargs, run_id, job_id):
    path_prefix = kwargs["path_prefix"]
    return path_prefix + "/" + run_id + "/" + job_id + "/"

def get_depends_file_path(kwargs, job_name, job_key):
    run_id = get_run_id(kwargs)
    return get_result_path(kwargs, run_id, job_name) + job_key

def get_depends_path(kwargs):
    depends_lst = eval(kwargs["depend_job_names_keys"])
    result = {}
    for item in depends_lst:
        tmp_lst = item.split("#")
        depends_job = tmp_lst[0]
        depends_key = tmp_lst[1]
        depends_name = tmp_lst[2]
        result[depends_name] = get_depends_file_path(kwargs, depends_job, depends_key)
    return result
##################  中间文件与结果文件路径  ######################

def load_cross_result(spark,path_cross_result):
    df_seg_dosage = spark.read.parquet(path_cross_result)
    df_seg_dosage = df_seg_dosage.select("ID","INDEX","DOSAGE","DOSAGE_STANDARD","PACK_ID_CHECK","PACK_ID_STANDARD")
    
    return df_seg_dosage 


######## == 下载文件 == ########
def loading_files(spark, input_path):
    files = spark.read.parquet(input_path)
    return files


##### == mapping == #####
def join_maping_table(df_cross_dosage, df_mapping_dosage,left_key,right_key):

    
    df_cross_dosage = df_cross_dosage.withColumnRenamed(left_key,"left_col")
    df_mapping_dosage = df_mapping_dosage.withColumnRenamed(right_key,"right_col")

    df_dosage = df_cross_dosage.join(df_mapping_dosage,df_cross_dosage.left_col == df_mapping_dosage.right_col, how="left")

    df_dosage = df_dosage.withColumnRenamed("left_col",left_key).drop("right_col")

    
    return df_dosage

