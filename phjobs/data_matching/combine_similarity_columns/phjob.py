# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
import os
import pandas as pd
from functools import reduce
from phcli.ph_logs.ph_logs import phs3logger
from pyspark.sql.types import StringType
from pyspark.sql.functions import pandas_udf, PandasUDFType, array_join
from pyspark.sql import functions as F 
import uuid


def execute(**kwargs):
    """
        please input your code below
        get spark session: spark = kwargs["spark"]()
    """
##############========configure============##############
    logger = phs3logger(kwargs["job_id"])
    spark = kwargs["spark"]()
    logger.info(kwargs)
############==========configure============###############

#######################---------------input-------------#######################	
    depends = get_depends_path(kwargs)
    path_sim_dosage = depends["input_sim_dosage"]
    path_sim_mnf = depends["input_sim_mnf"]
    path_sim_mole = depends["input_sim_mole"]
    path_sim_pack = depends["input_sim_pack"]
    path_sim_spec = depends["input_sim_spec"]
    g_repartition_shared = int(kwargs["g_repartition_shared"])
#######################---------------input-------------#######################	

#######################--------------output-------------######################## 
    job_id = get_job_id(kwargs)
    run_id = get_run_id(kwargs)
    result_path_prefix = get_result_path(kwargs, run_id, job_id)
    result_path = result_path_prefix + kwargs["combine_cols_job_result"]
#######################--------------output--------------########################


###################--------loading files--------------########################
    df_sim_dosage = loading_files(spark, path_sim_dosage)
    df_sim_mnf = loading_files(spark, path_sim_mnf)
    df_sim_mole = loading_files(spark, path_sim_mole)
    df_sim_pack = loading_files(spark, path_sim_pack)
    df_sim_spec = loading_files(spark, path_sim_spec)
##################--------loading files----------------########################

########################--------------main function--------------------#################
    df_max_effectiveness = collect_similarity_data(df_sim_dosage,df_sim_mnf,df_sim_mole,df_sim_pack,df_sim_spec)
    
#     df_max_effectiveness = Array_tranform_into_string(df_max_effectiveness)
    
######################--------------main function--------------------#################   

############# == RESULT == ####################

#     df_max_effectiveness.repartition(g_repartition_shared).write.mode("overwrite").parquet(result_path)
    
############ == RESULT == #####################

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


##### == LOADING FILES == ######
def loading_files(spark,input_path):
    
    df = spark.read.parquet(input_path)
        
    return df

def collect_similarity_data(df_sim_dosage,df_sim_mnf,df_sim_mole,df_sim_pack,df_sim_spec):
    
    df_max_effectiveness = df_sim_dosage.join(df_sim_mnf,df_sim_dosage.ID == df_sim_mnf.ID,"left")\
                                        .join(df_sim_mole,df_sim_dosage.ID == df_sim_mole.ID,"left")\
                                        .join(df_sim_pack,df_sim_dosage.ID == df_sim_pack.ID,"left")\
                                        .join(df_sim_spec,df_sim_dosage.ID == df_sim_spec.ID,"left")\
                                        .drop(df_sim_mnf.ID)\
                                        .drop(df_sim_mole.ID)\
                                        .drop(df_sim_pack.ID)\
                                        .drop(df_sim_spec.ID)
    df_max_effectiveness.show(200)
    
    return df_sim_dosage  

#####  == array transform string == ###
def Array_tranform_into_string(df_max_effectiveness):
    
    df_max_effectiveness = df_max_effectiveness.withColumn("DOSAGE_CUT_WORDS",array_join(df_max_effectiveness.DOSAGE_CUT_WORDS,delimiter=''))
    df_max_effectiveness = df_max_effectiveness.withColumn("DOSAGE_CUT_STANDARD_WORDS",array_transform_string(df_max_effectiveness.DOSAGE_CUT_STANDARD_WORDS))
    df_max_effectiveness = df_max_effectiveness.withColumn("MANUFACTURER_NAME_CUT_WORDS",array_transform_string(df_max_effectiveness.MANUFACTURER_NAME_CUT_WORDS))
    df_max_effectiveness = df_max_effectiveness.withColumn("MANUFACTURER_NAME_STANDARD_CUT_STANDARD_WORDS",array_transform_string(df_max_effectiveness.MANUFACTURER_NAME_STANDARD_CUT_STANDARD_WORDS))
    df_max_effectiveness = df_max_effectiveness.withColumn("MOLE_CUT_WORDS",array_transform_string(df_max_effectiveness.MOLE_CUT_WORDS))
    df_max_effectiveness = df_max_effectiveness.withColumn("MOLE_CUT_STANDARD_WORDS",array_transform_string(df_max_effectiveness.MOLE_CUT_STANDARD_WORDS))
    df_max_effectiveness = df_max_effectiveness.withColumn("PRODUCT_NAME_CUT_WORDS",array_transform_string(df_max_effectiveness.PRODUCT_NAME_CUT_WORDS))
    df_max_effectiveness = df_max_effectiveness.withColumn("SPEC_CUT_WORDS",array_transform_string(df_max_effectiveness.SPEC_CUT_WORDS))
    df_max_effectiveness = df_max_effectiveness.withColumn("SPEC_CUT_STANDARD_WORDS",array_transform_string(df_max_effectiveness.SPEC_CUT_STANDARD_WORDS))
    df_max_effectiveness = df_max_effectiveness.withColumn("PRODUCT_CUT_STANDARD_WORDS",array_transform_string(df_max_effectiveness.PRODUCT_CUT_STANDARD_WORDS))
    
    print(df_max_effectiveness.printSchema())
    return df_max_effectiveness


################---------------functions--------------------################
