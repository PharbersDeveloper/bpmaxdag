# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
import re ,os
import uuid
from pyspark.sql.types import DoubleType
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from pyspark.sql.functions import array, array_join, col,\
                                split, array_union,array_remove,\
                                collect_set, explode, array_distinct


def execute(**kwargs):
    
    ########### == configure == ###########
    """
        please input your code below
        get spark session: spark = kwargs["spark"]()
    """
    logger = phs3logger(kwargs["job_id"], LOG_DEBUG_LEVEL)
    logger.info("当前 owner 为 " + str(kwargs["owner"]))
    logger.info("当前 run_id 为 " + str(kwargs["run_id"]))
    logger.info("当前 job_id 为 " + str(kwargs["job_id"]))
    spark = kwargs["spark"]()
    ########### == configure == ###########
    
    
    ########### == input == ###########
    depends = get_depends_path(kwargs)

    path_correct_table = depends["input_correct_table"]
    ########### == input == ###########
    
    
    ########### == output == ###########
    jod_name = kwargs["job_name"]
    run_id = get_run_id(kwargs)
    result_path_prefix = get_result_path(kwargs,run_id,jod_name)
    path_table_of_on_check_id_in_standard_csv = result_path_prefix + kwargs["table_of_no_check_id_in_standard_csv"]
    ########### == output == ###########
    
    ########## == loading files == #############
    
    df_correct = loading_parquet_files(spark, path_correct_table)
    
    ######### == loading files == #############
    
    
    ########### == main function == #########
    
    df_of_no_exist_pack_id_standard = get_dosage_not_exist_in_standard(input_df=df_correct,\
                                                           input_col="PACK_ID_STANDARD")
    ########## === RESULT === #############
    #### 标准表缺失数据
    write_files(input_df_info=df_of_no_exist_pack_id_standard,\
                path_output=path_table_of_on_check_id_in_standard_csv)
    
    ########### == main function == #########
    
    return {}
    

##############  中间文件与结果文件路径  #####################
def get_run_id(kwargs):
    run_id = kwargs["run_id"]
    if not run_id:
        run_id = "runid_" + "alfred_runner_test"
    return run_id

# def get_job_id(kwargs):
#     job_name = kwargs["job_name"]
#     job_id = kwargs["job_id"]
#     if not job_id:
#         job_id = "jobid_" + uuid.uuid4().hex
#     return job_name # + "_" + job_id 

def get_result_path(kwargs,run_id,job_name):
    path_prefix = kwargs["path_prefix"]
    result_path = path_prefix + "/" + run_id + "/" + job_name + "/"
    return result_path

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
    
###################  中间文件与结果文件路径  ######################


#####  == 下载文件 == ########
def loading_parquet_files(spark, input_path):
    try:
        df = spark.read.parquet(input_path)
        return df
    except:
        print("parquet文件不存在，请检查文件格式及路径！")
        return None 

##### == 写入路径 == #########
def write_files(input_df_info, path_output):
    
    try:
        if input_df_info[-1].lower() == "parquet":
            input_df_info[0].repartition(10).write.mode("overwrite").parquet(path_output)
        else:
            input_df_info[0].repartition(1).write.mode("overwrite").csv(path_output,header=True)
        status_info = fr"{input_df_info[-1]} Write Success"
    except:
        status_info = fr"{input_df_info[-1]} Write Failed"
        
    print(status_info)
    
    return status_info

###### == 获取不在标准表中的数据 == ########
def get_dosage_not_exist_in_standard(input_df,input_col):
    
    df = input_df.filter(col(input_col).isNull())
    df_type = "csv"
    
    return df,df_type

