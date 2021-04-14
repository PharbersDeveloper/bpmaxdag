# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

import uuid
import pandas as pd
from pyspark.sql.types import DoubleType
from phcli.ph_logs.ph_logs import phs3logger
from pyspark.sql.types import *
from pyspark.sql.functions import broadcast, monotonically_increasing_id, col
from pyspark.sql.functions import udf, pandas_udf, PandasUDFType


def execute(**kwargs):
    """
        please input your code below
        get spark session: spark = kwargs["spark"]()
    """
#####################============configure================#################
    logger = phs3logger(kwargs["job_id"])
    spark = kwargs["spark"]()
    logger.info(kwargs)
#####################=============configure===============#################


#################-----------input---------------################
    depends = get_depends_path(kwargs)
    g_partitions_num = int(kwargs["g_partitions_num"])
    path_cleanning_table = depends["cleaning"]
    path_standard_table = depends["standard"]
################------------input----------------################


###############----------------output-------------################
    job_id = get_job_id(kwargs)
    run_id = get_run_id(kwargs)
    result_path_prefix = get_result_path(kwargs, run_id, job_id)
    result_path = result_path_prefix + kwargs["output_correct_result"]
###############----------------output--------------##################

    
###########--------------load file------------------################
    df_standard = loading_flies(spark, path_standard_table)
    df_cleanning = loading_flies(spark, path_cleanning_table)
###########--------------load file-------------------################


#############--------------main function--------------################
    df_correct = merge_table(left_table=df_cleanning,\
                             right_table=df_standard,\
                             left_key="PACK_ID_CHECK",\
                             right_key="PACK_ID_STANDARD")
    

#########--------------main function--------------------#################   


############# ------------  RESUST ----------- ######################
    df_correct.repartition(g_partitions_num).write.mode("overwrite").parquet(result_path)
############# ------------  RESUST ----------- ######################

    return {}

################--------------------- functions ---------------------################
"""
中间文件与结果文件路径
"""
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

######################## == functions == ###################################

########　下载文件
def loading_flies(spark,input_path):
    df = spark.read.parquet(input_path)
    return df

######## 统一pack_check_id数据类型
def define_data_types_as_double(input_dataframe,input_col):
    
    df = input_dataframe.withColumn(input_col,col(input_col).cast(DoubleType()))
    
    return df

#######  左连接
def merge_table(left_table,right_table,left_key,right_key):
    
    left_table = define_data_types_as_double(input_dataframe=left_table,\
                                             input_col=left_key)
    right_table = define_data_types_as_double(input_dataframe=right_table,\
                                             input_col=right_key)
    
    left_table = left_table.withColumnRenamed(left_key,"left_col")
    right_table = right_table.withColumnRenamed(right_key,"right_col")
    df = left_table.join(right_table,left_table.left_col==right_table.right_col,"left")
    df = df.withColumnRenamed("left_col",left_key)\
            .withColumnRenamed("right_col",right_key)
    
    return df




################-----------------------------------------------------################