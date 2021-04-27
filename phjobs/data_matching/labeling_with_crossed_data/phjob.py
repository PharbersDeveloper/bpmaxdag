# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger
import uuid
from pyspark.sql.functions import when , col 


def execute(**kwargs):
    """
        please input your code below
        get spark session: spark = kwargs["spark"]()
    """
################========configure=============############
    logger = phs3logger(kwargs["job_id"])
    spark = kwargs["spark"]()
    logger.info(kwargs)
############========configure==============#############

##############-----------input------------##############
    depends = get_depends_path(kwargs)
    path_combine_Cols = depends["input_prod"]
#############------------input-------------##############

#############------------output-----------##############
    job_id = get_job_id(kwargs)
    run_id = get_run_id(kwargs)
    result_path_prefix = get_result_path(kwargs, run_id, job_id)
    result_path = result_path_prefix + kwargs["label_result"]
############-------------output------------#############

###########----------loading files-------------############
    df_labeling = loading_files(spark, path_combine_Cols)
##########-----------loading files--------############
    

###########---------mian functions ----------######################
    
    signal_of_data = Judge_TrainingData_OrNot(input_dataframe=df_labeling,\
                                           inputCheckCol="PACK_ID_CHECK")
    
    df_labeling = Make_label_OrNot(input_signal_of_data=signal_of_data,\
                                  input_dataframe=df_labeling)
    
    
    df_labeling = make_label(df_labeling)

    df_labeling.repartition(10).write.mode("overwrite").parquet(result_path)
    logger.info("第二轮完成，写入完成")
###########---------mian functions ----------######################
   

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

#### == LOADING FILES == #####
def loading_files(spark, input_path):
    
    df = spark.read.parquet(input_path)
    print(df.printSchema())
    
    return df

def make_label(df_result):
    df_result = df_result.withColumn("PACK_ID_CHECK_NUM", df_result.PACK_ID_CHECK.cast("int")).na.fill({"PACK_ID_CHECK_NUM": -1})
    df_result = df_result.withColumn("PACK_ID_STANDARD_NUM", df_result.PACK_ID_STANDARD.cast("int")).na.fill({"PACK_ID_STANDARD_NUM": -1})
    df_result = df_result.withColumn("label",
                                     when((df_result.PACK_ID_CHECK_NUM > 0) & (df_result.PACK_ID_STANDARD_NUM > 0) & (df_result.PACK_ID_CHECK_NUM == df_result.PACK_ID_STANDARD_NUM), 1.0).otherwise(0.0)) \
    .drop("PACK_ID_CHECK_NUM", "PACK_ID_STANDARD_NUM")
    return df_result


def Judge_TrainingData_OrNot(input_dataframe,inputCheckCol):
    
    Cols_of_data = list(map(lambda x: x.upper(),input_dataframe.columns))
    
    Check_col = inputCheckCol.upper()
    
    if Check_col in Cols_of_data:
        signal = True
    else:
        signal = None
    
    print(signal)
    
    return signal


def Make_label_OrNot(input_signal_of_data,input_dataframe):
    
    if input_signal_of_data == True:
        
        message = r" need make lable!"
        output_dataframe = make_label(input_dataframe)
    else:
        message = r" not need make lable!"
        output_dataframe = input_dataframe
    print(message)
        
    return output_dataframe


################-----------------------------------------------------################