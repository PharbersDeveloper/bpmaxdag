# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
import uuid
import pandas as pd
import numpy as np
from pyspark.sql import Window
from pyspark.sql.types import DoubleType 
from pyspark.sql.functions import pandas_udf, PandasUDFType, array_join
from pyspark.sql import functions as F
from itertools import product
from nltk.metrics.distance import jaro_winkler_similarity


def execute(**kwargs):
    """
        please input your code below
        get spark session: spark = kwargs["spark"]()
    """
############# == configure == #####################
    logger = phs3logger(kwargs["job_id"], LOG_DEBUG_LEVEL)
    spark = kwargs["spark"]()
############# == configure == #####################
    
############# ------- input ----------- #####################
    depends = get_depends_path(kwargs)
    path_mapping_mole = depends["input_mapping_mole"]
    
############# ------- input ------------ ####################

############# == output == #####################
    job_id = get_job_id(kwargs)
    run_id = get_run_id(kwargs)
    result_path_prefix = get_result_path(kwargs, run_id, job_id)
    result_path = result_path_prefix + kwargs["similarity_mole_result"]
    g_repartition_shared = int(kwargs["g_repartition_shared"])
############# == output == #####################

############# == loading files == #####################

    df_mapping_mole = load_mapping_mole_result(spark, path_mapping_mole)
    
############# == loading files == #####################

############# == main functions == #####################

    df_sim_mole = calculate_mole_similarity(df_mapping_mole)
    
    df_sim_mole = let_array_become_string(df_sim_mole)

############# == main functions == #####################

    df_sim_mole.repartition(g_repartition_shared).write.mode("overwrite").parquet(result_path)

    return {}


########### == FUNCTIONS == #########

############### === 中间文件与结果文件路径 === ##############
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

############### === 中间文件与结果文件路径 === ##############


#### == loding files == ###
def load_mapping_mole_result(spark, path_mapping_mole):
    df_mapping_mole = spark.read.parquet(path_mapping_mole)
    return df_mapping_mole  


@pandas_udf(DoubleType(),PandasUDFType.SCALAR)
def calculate_mole_similarity_after_mapping(mole_name,master_mole,mole_name_standard):
    frame = {"mole_name":mole_name,
            "master_mole":master_mole,
            "mole_name_standard":mole_name_standard}
    df = pd.DataFrame(frame)
    def calculate_similarity(s1,s2,s3):
        try:
            if float(jaro_winkler_similarity(s1,s3)) >= 0.95:
                sim_value = float(jaro_winkler_similarity(s1,s3))
            elif s1 in s2:
                sim_value = float(0.995)
            else:
                sim_value = float(0.0) 
        except:
            sim_value = float(0.0)
        return sim_value
    
    df['mole_sim'] = df.apply(lambda x: calculate_similarity(x.mole_name, x.master_mole,x.mole_name_standard), axis=1)
    return df['mole_sim']

##### == calulate_similarity == #######
def calculate_mole_similarity(df_mapping_mole):
    
    df_sim_mole = df_mapping_mole.withColumn("eff_mole",calculate_mole_similarity_after_mapping(df_mapping_mole.MOLE_NAME,\
                                                                                        df_mapping_mole.MASTER_MOLE,\
                                                                                       df_mapping_mole.MOLE_NAME_STANDARD))
    return df_sim_mole

def let_array_become_string(df_sim_mole):
    
    df_sim_mole = df_sim_mole.withColumn("MASTER_MOLE",array_join(df_sim_mole.MASTER_MOLE,delimiter=' '))
    
    return df_sim_mole