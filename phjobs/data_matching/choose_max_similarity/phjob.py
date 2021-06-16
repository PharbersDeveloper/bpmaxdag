# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
import os
import pandas as pd
from functools import reduce
import numpy as np
import pandas as pd
from pyspark.sql import Window
from phcli.ph_logs.ph_logs import phs3logger
from pyspark.sql.types import StringType, DoubleType
from pyspark.sql.functions import pandas_udf, PandasUDFType, array
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
    path_combine = depends["input_combine"]
    g_repartition_shared = int(kwargs["g_repartition_shared"])
#######################---------------input-------------#######################	

#######################--------------output-------------######################## 
    job_id = get_job_id(kwargs)
    run_id = get_run_id(kwargs)
    result_path_prefix = get_result_path(kwargs, run_id, job_id)
    result_path = result_path_prefix + kwargs["max_sim_job_result"]
#######################--------------output--------------########################


###################--------loading files--------------########################
    df_max_sim = loading_files(spark, path_combine)
##################--------loading files----------------########################

########################--------------main function--------------------#################

    df_max_sim = combine_similarity_cols(df_max_sim)

    df_max_sim = get_max_similarity(df_max_sim)
    
    df_max_sim = extract_max_similaritey(df_max_sim)
    
######################--------------main function--------------------#################   

############# == RESULT == ####################

    df_max_sim.repartition(g_repartition_shared).write.mode("overwrite").parquet(result_path)
    
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

def combine_similarity_cols(df_max_sim):
    
    df_max_sim = df_max_sim.withColumn("array_sim", array(df_max_sim.EFFECTIVENESS_DOSAGE,\
                                                          df_max_sim.EFFECTIVENESS_MANUFACTURER,\
                                                          df_max_sim.EFFECTIVENESS_MOLE,\
                                                          df_max_sim.EFFECTIVENESS_PACK_QTY,\
                                                         df_max_sim.EFFECTIVENESS_SPEC))
    
    return df_max_sim

#增加平滑系数

@pandas_udf(DoubleType(),PandasUDFType.SCALAR)
def get_array_max_value(inputCol):
    frame = {"inputCol":inputCol}
    df = pd.DataFrame(frame)
    df['output'] = df.apply(lambda x: np.sum([x.inputCol[0]*0.2,\
                                             x.inputCol[1]*0.3,\
                                             x.inputCol[2]*0.3,\
                                             x.inputCol[3]*0.1,\
                                             x.inputCol[4]*0.1]), axis=1)
    return df['output']

def get_max_similarity(df_max_sim):
    
    df_max_sim = df_max_sim.withColumn("array_sim", get_array_max_value(df_max_sim.array_sim))
    
    return df_max_sim


def extract_max_similaritey(df_max_sim):
    
    window_max = Window.partitionBy("ID")
    
    df_max_sim = df_max_sim.withColumn("max_eff",F.max("array_sim").over(window_max))\
                                .where(F.col("array_sim") >= F.col("max_eff")*0.7)\
                                .drop("max_eff")
    
    return df_max_sim

################---------------functions--------------------################
