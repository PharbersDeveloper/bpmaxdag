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
    path_raw_table = depends["input_raw_table"]
    path_prediction_table = depends["input_prediction_table"]
    ########### == input == ###########
    
    
    ########### == output == ###########
    jod_name = kwargs["job_name"]
    run_id = get_run_id(kwargs)
    result_path_prefix = get_result_path(kwargs,run_id,jod_name)
    path_be_cutting_data = result_path_prefix + kwargs["output_be_cutting_data"]
    ########### == output == ###########
    
    ########## == loading files == #############
    
    df_raw_table = loading_parquet_files(spark, path_raw_table)
    
    df_prediction = loading_parquet_files(spark, path_prediction_table)
    
    ######### == loading files == #############
    
    ########### == main function == #########
    
    df_info_of_be_cutting = find_be_cutting_data(dataframe_of_raw=df_raw_table,\
                                            dataframe_of_prediction=df_prediction)
    
    wirte_files(input_info_of_dataframe=df_info_of_be_cutting,\
                path_output=path_be_cutting_data)
    
   
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
def loading_csv_files(spark, input_path):

    df = spark.read.csv(input_path, header=True) 
    
    return df

def loading_parquet_files(spark, input_path):
    
    try:
        df = spark.read.parquet(input_path)
        message = fr"{input_path} loading success !"
        data = df
    except:
        message = fr"{input_path} path does not exist"
        data = None
    print(message)
    return data
    
        

##### == 写入路径 == #########
def wirte_files(input_info_of_dataframe, path_output):
    
    try:
        if input_info_of_dataframe[-1].lower() == 'csv':
            input_info_of_dataframe[0].repartition(1).write.mode("overwrite").csv(path_output,header=True)
        else:
            input_info_of_dataframe[0].repartition(16).write.mode("overwrite").parquet(path_output)
        message = fr'{path_output} {input_info_of_dataframe[-1]} Write Success !'
    except:
        message = fr'{path_output} {input_info_of_dataframe[-1]} Write Failed !'
        
    print(message)
    
    return message


def find_be_cutting_data(dataframe_of_raw,dataframe_of_prediction):
    
    
    dataframe_of_prediction_id = dataframe_of_prediction.select("ID").distinct().withColumnRenamed("ID","PRE_ID")
    
    
    df = dataframe_of_raw.join(dataframe_of_prediction_id,\
                                    dataframe_of_raw.ID == dataframe_of_prediction_id.PRE_ID,\
                                    'left')
    dataframe_of_output = df.filter(col("PRE_ID").isNull()).drop(col("PRE_ID"))
    
    file_type = "csv"
    
    return dataframe_of_output,file_type