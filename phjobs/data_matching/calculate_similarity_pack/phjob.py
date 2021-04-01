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
from pyspark.sql.functions import pandas_udf, PandasUDFType
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
    path_segmentation_pack = depends["input_cross_result"]
    
############# ------- input ------------ ####################

############# == output == #####################
    job_id = get_job_id(kwargs)
    run_id = get_run_id(kwargs)
    result_path_prefix = get_result_path(kwargs, run_id, job_id)
    result_path = result_path_prefix + kwargs["similarity_pack_result"]
    g_repartition_shared = int(kwargs["g_repartition_shared"])
############# == output == #####################

############# == loading files == #####################

    df_seg_pack = load_seg_pack_result(spark, path_segmentation_pack)
    
############# == loading files == #####################

############# == main functions == #####################

    df_sim_pack = calulate_pack_similarity(df_seg_pack)
    
#     df_sim_pack = extract_max_similarity(df_sim_pack)

############# == main functions == #####################
    df_sim_pack.repartition(g_repartition_shared).write.mode("overwrite").parquet(result_path)

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
def load_seg_pack_result(spark, path_segmentation_pack):
    df_seg_pack = spark.read.parquet(path_segmentation_pack)
    df_seg_pack = df_seg_pack.select("ID","INDEX","PACK_QTY","PACK_QTY_STANDARD")
    return df_seg_pack  


#计算相似性
@pandas_udf(DoubleType(), PandasUDFType.SCALAR)
def Get_spec_similarity(raw_spec,standard_spec):
    frame = {"raw_spec": raw_spec,
            "standard_spec": standard_spec}
    df = pd.DataFrame(frame)
    def sure_pack_sim(raw_pack,standard_pack):
        if float(raw_pack) == float(standard_pack):
            sim_pack = 1.0
        else:
            sim_pack = 0.0
        return sim_pack
    df['spec_eff'] = df.apply(lambda x: sure_pack_sim(x.raw_spec,x.standard_spec), axis=1)
    
    return df['spec_eff']


#### 相似性计算 ########
@pandas_udf(DoubleType(),PandasUDFType.SCALAR)
def calulate_pack_similarity_after_seg(raw_pack,standard_pack):
    frame = {"raw_pack":raw_pack,
            "standard_pack":standard_pack}
    df = pd.DataFrame(frame)
    
    def Get_sim_value_data(input_raw, input_standard):
        all_possible_result = list(product(input_raw, input_standard))
        all_possible_sim_value = list(map(lambda x: jaro_winkler_similarity(x[0],x[-1]), all_possible_result))
        all_possible_array_value = np.array(all_possible_sim_value)
        all_possible_matrix_value = all_possible_array_value.reshape(int(len(input_raw)),int(len(input_standard)))
        max_similarity_value = list(map(lambda x: max(x,default=0.0), all_possible_matrix_value))
        return max_similarity_value
    
    def handle_sim_value_data(raw_sentence, standard_sentence):
        max_similarity_value = Get_sim_value_data(raw_sentence, standard_sentence)
        high_similarity_data = list(filter(lambda x: x >= 0.5, max_similarity_value))
        low_similarity_data = [x for x in max_similarity_value if x not in high_similarity_data]
        high_similarity_rate = len(high_similarity_data) / len(max_similarity_value)
        if high_similarity_rate >= 0.5:
            similarity_value = np.mean(high_similarity_data)
        else:
            similarity_value = np.mean(low_similarity_data)
        return similarity_value
    
    df['output_similarity_value'] = df.apply(lambda x: float(handle_sim_value_data(x.raw_pack,x.standard_pack)), axis=1)
    return df['output_similarity_value']

##### == calulate_similarity == #######
def calulate_pack_similarity(df_seg_pack):
    
    df_seg_pack = df_seg_pack.withColumn("eff_pack",Get_spec_similarity(df_seg_pack.PACK_QTY,df_seg_pack.PACK_QTY_STANDARD))
    
    return df_seg_pack


# ##### == 取最大相似性 == #########
# def extract_max_similarity(df_sim_pack):
    
#     window_pack = Window.partitionBy("ID")

#     df_sim_pack = df_sim_pack.withColumn("max_eff",F.max("eff_pack").over(window_pack))\
#                                 .where(F.col("eff_pack") == F.col("max_eff"))\
#                                 .drop("max_eff")\
#                                 .drop_duplicates(["ID"])
#     return df_sim_pack