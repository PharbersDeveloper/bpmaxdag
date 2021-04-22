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
    path_max_sim = depends["input_max_sim"]
    
############# ------- input ------------ ####################

############# == output == #####################
    job_id = get_job_id(kwargs)
    run_id = get_run_id(kwargs)
    result_path_prefix = get_result_path(kwargs, run_id, job_id)
    result_path = result_path_prefix + kwargs["similarity_prod_result"]
    g_repartition_shared = int(kwargs["g_repartition_shared"])
############# == output == #####################

############# == loading files == #####################

    df_prod = load_combine_result(spark, path_max_sim)
    
############# == loading files == #####################

############# == main functions == #####################

    df_prod = calulate_prod_similarity(df_prod)
    

############# == main functions == #####################
    df_prod.repartition(g_repartition_shared).write.mode("overwrite").parquet(result_path)

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
def load_combine_result(spark, path_combine):
    df_prod = spark.read.parquet(path_combine)
    return df_prod  

#### 相似性计算 ########
@pandas_udf(DoubleType(),PandasUDFType.SCALAR)
def get_prod_similarity(eff_mole,eff_dosage):
    frame ={"eff_mole":eff_mole,
             "eff_dosage":eff_dosage} 
    df = pd.DataFrame(frame)
    df['eff_prod'] = df.apply(lambda x: np.mean(x.eff_mole,x.eff_dosage),axis=1)
    return df['eff_prod']
    
##### == calulate_similarity == #######
def calulate_prod_similarity(df_prod):
    
    df_prod = df_prod.withColumn('eff_prod',get_prod_similarity(df_prod.eff_mole,df_prod.eff_dosage))
    
    return df_prod

