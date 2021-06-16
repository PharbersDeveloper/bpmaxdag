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
    path_mapping_path = confirm_mapping_path(spark,kwargs)  
    g_repartition_shared = int(kwargs["g_repartition_shared"])
    
###################=======input==========#################

###################=======output==========################# 
    job_id = get_job_id(kwargs)
    run_id = get_run_id(kwargs)
    result_path_prefix = get_result_path(kwargs, run_id, job_id)
    result_path = result_path_prefix + kwargs["mapping_mnf_result"]
    original_mnf_mapping_path = result_path_prefix + kwargs["original_mnf_mapping_table"]
###################=======output==========#################

###################=======loading files==========#################
    df_cross_mnf = load_cross_result(spark, path_cross_result)
    df_mapping_mnf = loading_files(spark, path_mapping_path)
    
####################=======loading files==========#################

####################=======main functions==========#################
    df_mnf = join_maping_table(df_cross_mnf=df_cross_mnf,\
                               df_mapping_mnf=df_mapping_mnf,\
                               left_key="MANUFACTURER_NAME_STANDARD",\
                               right_key="MANUFACTURER_NAME_STANDARD")
# # ####################=======main functions==========#################

# ####################### == RESULT == #####################
    #写入原mapping表
    df_mapping_mnf.write.mode("overwrite").parquet(original_mnf_mapping_path)
    
    #写入结果
    df_mnf.repartition(g_repartition_shared).write.mode("overwrite").parquet(result_path)
    
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
    
    df_seg_mnf = spark.read.parquet(path_cross_result)
    df_seg_mnf = df_seg_mnf.select("ID","INDEX","MANUFACTURER_NAME","MANUFACTURER_NAME_STANDARD")
    
    return df_seg_mnf 


######## == 下载文件 == ########
def loading_files(spark, input_path):
    files = spark.read.parquet(input_path)
    return files

####### == 确认正确mapping路径状态  == #####
def confirm_mapping_route_state(spark,kwargs):
    path_correct_mapping_path = get_depends_path(kwargs)["input_correct_mapping_table"]
    
    try:
        df = spark.read.parquet(path_correct_mapping_path)
        state = "success"
    except:
        state = "fail"
    return state
        

####### == 确认mapping表路径 == #######
def confirm_mapping_path(spark,kwargs):
    
    state = confirm_mapping_route_state(spark,kwargs)
    if state == "success":
        path_mapping_table =get_depends_path(kwargs)["input_correct_mapping_table"]
    else:
        path_mapping_table = kwargs["mnf_mapping_path"]
    
    return path_mapping_table


##### == mapping == #####
def join_maping_table(df_cross_mnf,df_mapping_mnf,left_key,right_key):
    
    df_cross_mnf = df_cross_mnf.withColumnRenamed(left_key,"left_col")
    df_mapping_mnf = df_mapping_mnf.withColumnRenamed(right_key,"right_col")
    
    df_mnf = df_cross_mnf.join(df_mapping_mnf,df_cross_mnf.left_col == df_mapping_mnf.right_col, how="left")
    
    df_mnf = df_mnf.withColumnRenamed("left_col",left_key).drop("right_col")
    
    
    return df_mnf

