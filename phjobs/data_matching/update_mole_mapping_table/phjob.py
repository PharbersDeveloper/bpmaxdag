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

    path_original_mole_mapping_table = depends["input_original_mole_table"]
    path_negative_table = depends["input_negative_table"]
    ########### == input == ###########
    
    
    ########### == output == ###########
    jod_name = kwargs["job_name"]
    run_id = get_run_id(kwargs)
    result_path_prefix = get_result_path(kwargs,run_id,jod_name)
    output_mole_mapping_table = result_path_prefix + kwargs["output_mole_mapping_table"]
    ########### == output == ###########
    
    ########## == loading files == #############
    
    df_mole_mapping_original = loading_parquet_files(spark, path_original_mole_mapping_table)
    
    df_negative = loading_parquet_files(spark, path_negative_table)
    
    ######### == loading files == #############
    
    
    
    ########### == main function == #########
     
    
    df_mole_mapping_original = get_mole_mapping_elements(input_dataframe=df_mole_mapping_original,\
                                                             input_mole="MOLE_NAME_STANDARD",\
                                                             input_master="MASTER_MOLE")
    
    df_negative =  get_negative_mole_mapping_elements(input_dataframe=df_negative\
                                     ,input_mole="MOLE_NAME"\
                                     ,input_standard_mole="MOLE_NAME_STANDARD"\
                                     ,input_master="MASTER_MOLE",\
                                     similarity=0.9)


    df = get_available_mole_mapping_elements(input_dataframe_oringal=df_mole_mapping_original,\
                                               input_dataframe_negative=df_negative)

    
    wirte_files(df,output_mole_mapping_table)
   
    
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
        return df
    except:
        print("parquet文件不存在，请检查文件格式及路径！")
        return None 

##### == 写入路径 == #########
def wirte_files(input_df, path_output):
    
    try:
        input_df.write.mode("overwrite").parquet(path_output)
        status_info = r"Write Success"
        
    except:
        status_info = r"Write Failed"
    print(status_info)
    
    return status_info

def get_mole_mapping_elements(input_dataframe,input_mole,input_master):
    
    if input_dataframe == None:
        return None
    else:

        input_dataframe = input_dataframe.na.fill('',subset=[input_mole])

        input_dataframe = input_dataframe.withColumn(input_master,explode(col(input_master)))

        data_frame = input_dataframe.groupBy(col(input_mole)).agg(collect_set(col(input_master)).alias(input_master))

        return data_frame


def get_negative_mole_mapping_elements(input_dataframe,input_mole,input_standard_mole,input_master,similarity):
    
    input_dataframe = input_dataframe.withColumn("PACK_ID_CHECK",col("PACK_ID_CHECK").cast(DoubleType()))\
                                    .withColumn("PACK_ID_STANDARD",col("PACK_ID_STANDARD").cast(DoubleType()))
        
    input_dataframe = input_dataframe.filter((col("EFFTIVENESS_MOLE_NAME") < float(similarity)) &\
                                             (col("PACK_ID_CHECK")==col("PACK_ID_STANDARD")))


    if input_master in  input_dataframe.columns:
     
        input_dataframe = input_dataframe.na.fill('',subset=[input_master])
    
        input_dataframe = input_dataframe.withColumn(input_master, split(col(input_master), pattern=' '))
        
        input_dataframe= input_dataframe.withColumn(input_mole,array(col(input_mole),col(input_standard_mole)))\
                    .withColumn(input_mole,array_distinct(array_union(col(input_mole),col(input_master))))\
                    .withColumn(input_mole, array_remove(col(input_mole),""))
    else:
        input_dataframe= input_dataframe.withColumn(input_mole,array(col(input_mole),col(input_standard_mole)))\
                .withColumn(input_mole,array_distinct(col(input_mole)))\
                .withColumn(input_mole, array_remove(col(input_mole),""))
        
    input_dataframe = input_dataframe.withColumn(input_mole,explode(col(input_mole)))
    data_frame =  input_dataframe.groupBy(col(input_standard_mole)).agg(collect_set(col(input_mole)).alias(input_master))
    data_frame.show(500)
    
    return data_frame


def get_available_mole_mapping_elements(input_dataframe_oringal, input_dataframe_negative):
    
    if input_dataframe_oringal == None:
        data_frame = input_dataframe_negative
    else:
        data_frame = input_dataframe_oringal.union(input_dataframe_negative)
        data_frame = get_mole_mapping_elements(input_dataframe=data_frame\
                                                 ,input_mole="MOLE_NAME_STANDARD",\
                                                 input_master="MASTER_MOLE")
    return data_frame