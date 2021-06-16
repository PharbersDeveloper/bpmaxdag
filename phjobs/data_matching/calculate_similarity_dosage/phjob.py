# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
import uuid
import pandas as pd
import numpy as np
from pyspark.sql import Window
from pyspark.sql.types import DoubleType ,StringType
from pyspark.sql.functions import pandas_udf, PandasUDFType 
from pyspark.sql.functions import array_join 
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
    path_mapping_dosage = depends["input_mapping_dosage"]
    
############# ------- input ------------ ####################

############# == output == #####################
    job_id = get_job_id(kwargs)
    run_id = get_run_id(kwargs)
    result_path_prefix = get_result_path(kwargs, run_id, job_id)
    result_path = result_path_prefix + kwargs["similarity_dosage_result"]
    g_repartition_shared = int(kwargs["g_repartition_shared"])
############# == output == #####################

############# == loading files == #####################

    df_mapping_dosage = load_mapping_dosage_result(spark, path_mapping_dosage)
    
############# == loading files == #####################

############# == main functions == #####################

    df_sim_dosage = calculate_dosage_similarity(df_mapping_dosage)
    
    
    df_sim_dosage = let_array_become_string(df_sim_dosage)

############# == main functions == #####################

########## === RESULT === ##############
    df_sim_dosage.repartition(g_repartition_shared).write.mode("overwrite").parquet(result_path)
########## === RESULT === ##############

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
def load_mapping_dosage_result(spark, path_mapping_dosage):
    df_mapping_dosage = spark.read.parquet(path_mapping_dosage)
    return df_mapping_dosage  


#### 相似性计算 ########
@pandas_udf(DoubleType(),PandasUDFType.SCALAR)
def execute_calculate_dosage_similarity(dosage,master_dosage,dosage_standard):
    frame = {"dosage":dosage,
            "master_dosage":master_dosage,
            "dosage_standard":dosage_standard}
    df = pd.DataFrame(frame)
    def calculate_similarity(s1,s2,s3):
        try:
            if jaro_winkler_similarity(s1,s3) > 0.95 :
                sim_value = float(jaro_winkler_similarity(s1,s3))
            elif s1 in s2:
                sim_value = float(1.0)
            else:
                sim_value = float(0.0)
        except:
            sim_value = float(0.0)
    return sim_value
    
    df['dosage_sim'] = df.apply(lambda x: calculate_similarity(x.dosage, x.master_dosage,x.dosage_standard), axis=1)
    return df['dosage_sim']

##### == calculate_similarity == #######
def calculate_dosage_similarity(df_mapping_dosage):
    
    df_sim_dosage = df_mapping_dosage.withColumn("eff_dosage", execute_calculate_dosage_similarity(df_mapping_dosage.DOSAGE,\
                                                                                       df_mapping_dosage.MASTER_DOSAGE,\
                                                                                      df_mapping_dosage.DOSAGE_STANDARD))
    
    return df_sim_dosage



def let_array_become_string(df_sim_dosage):
    
    df_sim_dosage = df_sim_dosage.withColumn("MASTER_DOSAGE",array_join(df_sim_dosage.MASTER_DOSAGE,delimiter=' '))
    df_sim_dosage.show(500)
    
    return df_sim_dosage
