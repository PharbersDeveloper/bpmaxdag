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
    path_segmentation_mole = depends["input_seg_mole"]
    
############# ------- input ------------ ####################

############# == output == #####################
    job_id = get_job_id(kwargs)
    run_id = get_run_id(kwargs)
    result_path_prefix = get_result_path(kwargs, run_id, job_id)
    result_path = result_path_prefix + kwargs["similarity_mole_result"]
    g_repartition_shared = int(kwargs["g_repartition_shared"])
############# == output == #####################

############# == loading files == #####################

    df_seg_mole = load_seg_mole_result(spark, path_segmentation_mole)
    
############# == loading files == #####################

############# == main functions == #####################

    df_sim_mole = calulate_mole_similarity(df_seg_mole)
    
    df_sim_mole = extract_max_similaritey(df_sim_mole)

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
def load_seg_mole_result(spark, path_segmentation_mole):
    df_seg_mole = spark.read.parquet(path_segmentation_mole)
    return df_seg_mole  



#### 相似性计算 ########
@pandas_udf(DoubleType(),PandasUDFType.SCALAR)
def calulate_mole_similarity_after_seg(raw_mole,standard_mole):
    frame = {"raw_mole":raw_mole,
            "standard_mole":standard_mole}
    df = pd.DataFrame(frame)
    
    def sure_sim(s1,s2):
        if s1 == s2:
            value = 1.0
        else:
            value = 0.0
        return value
    
    def Get_sim_value_data(input_raw, input_standard):
        all_possible_result = list(product(input_raw, input_standard))
        if len(all_possible_result) == 1:
            max_similarity_value = list(map(lambda x: sure_sim(x[0],x[-1]),all_possible_result))
        else:
            all_possible_sim_value = list(map(lambda x: jaro_winkler_similarity(x[0],x[-1]), all_possible_result))
            all_possible_array_value = np.array(all_possible_sim_value)
            all_possible_matrix_value = all_possible_array_value.reshape(int(len(input_raw)),int(len(input_standard)))
            max_similarity_value = list(map(lambda x: max(x), all_possible_matrix_value))
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
    
    df['output_similarity_value'] = df.apply(lambda x: float(handle_sim_value_data(x.raw_mole,x.standard_mole)), axis=1)
    return df['output_similarity_value']

##### == calulate_similarity == #######
def calulate_mole_similarity(df_seg_mole):
    
    df_seg_mole = df_seg_mole.withColumn("eff_mole",calulate_mole_similarity_after_seg(df_seg_mole.MOLE_CUT_WORDS,df_seg_mole.MOLE_CUT_STANDARD_WORDS))
    df_seg_mole.show(500)
    print(df_seg_mole.printSchema())
    return df_seg_mole


def extract_max_similaritey(df_sim_mole):
    
    window_mole = Window.partitionBy("ID")

    df_sim_mole = df_sim_mole.withColumn("max_eff",F.max("eff_mole").over(window_mole))\
                                .where(F.col("eff_mole") == F.col("max_eff"))\
                                .drop("max_eff")\
                                .drop_duplicates(["ID"])
    df_sim_mole.show(500)
    print(df_sim_mole.count())

    return df_sim_mole