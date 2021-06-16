# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
import uuid
from itertools import product
import pandas as pd
import numpy as np
from pyspark.sql import Window
from pyspark.sql.types import DoubleType ,StringType
from pyspark.sql.functions import pandas_udf, PandasUDFType ,col
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
    path_seg_mole = depends["input_seg_mole"]
    
############# ------- input ------------ ####################

############# == output == #####################
    job_id = get_job_id(kwargs)
    run_id = get_run_id(kwargs)
    result_path_prefix = get_result_path(kwargs, run_id, job_id)
    result_path = result_path_prefix + kwargs["similarity_mole_result"]
    g_repartition_shared = int(kwargs["g_repartition_shared"])
############# == output == #####################

############# == loading files == #####################

    df_seg_mole = loading_files(spark,\
                               path_of_file=path_seg_mole,\
                               file_type="parquet")
############# == loading files == #####################
    
############# == main functions == #####################
    
    df_sim_mole = Get_similarity_of_RawColAndStandardCol(input_dataframe=df_seg_mole,\
                                           RawColWords="MOLE_NAME_WORDS",\
                                           StandardColWords="MOLE_NAME_STANDARD_WORDS",\
                                           PrefixOfEffectiveness="Effectiveness")
    
    
    df_sim_mole = Cause_ArrayStructureCol_Become_StringStructureCol(input_dataframe=df_sim_mole,\
                                                                   inputCol="MOLE_NAME_WORDS")
    
    df_sim_mole = Cause_ArrayStructureCol_Become_StringStructureCol(input_dataframe=df_sim_mole,\
                                                                   inputCol="MOLE_NAME_STANDARD_WORDS")
    

############# == main functions == #####################

########## === RESULT === ##############
    write_files(input_dataframe=df_sim_mole,\
                path_of_write=result_path,\
                file_type="parquet",\
                repartition_num=g_repartition_shared)
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
def loading_files(spark, path_of_file, file_type):
    
    try:
        if file_type.lower() == 'parquet':
            output_dataframe = spark.read.parquet(path_of_file)
        else:
            output_dataframe = spark.read.csv(path_of_file)
        message = fr"{path_of_file} {file_type} Loading Success!"
    except:
        output_dataframe = None
        message = fr"{path_of_file} {file_type} Loading Failed!"
    print(message)
    return output_dataframe



#### 相似性计算 ########
@pandas_udf(DoubleType(),PandasUDFType.SCALAR)

def calulate_similarity_of_RawColAndStandardCol(RawCol,StandardCol):
    
    frame = {"RawCol":RawCol,
            "StandardCol":StandardCol}
    df = pd.DataFrame(frame)
    
    def Get_sim_value_data(input_raw, input_standard):
        all_possible_result = list(product(input_raw, input_standard))
        all_possible_sim_value = list(map(lambda x: jaro_winkler_similarity(x[0],x[-1]), all_possible_result))
        all_possible_array_value = np.array(all_possible_sim_value)
        all_possible_matrix_value = all_possible_array_value.reshape(int(len(input_raw)),int(len(input_standard)))
        max_similarity_value = list(map(lambda x: max(x), all_possible_matrix_value))
        return max_similarity_value
    
    def Solve_sim_value_data(raw_sentence, standard_sentence):
        max_similarity_value = Get_sim_value_data(raw_sentence, standard_sentence)
        high_similarity_data = list(filter(lambda x: x >= 0.5, max_similarity_value))
        low_similarity_data = [x for x in max_similarity_value if x not in high_similarity_data]
        high_similarity_rate = len(high_similarity_data) / len(max_similarity_value)
        if high_similarity_rate >= 0.5:
            similarity_value = np.mean(high_similarity_data)
        else:
            similarity_value = np.mean(low_similarity_data)
        return similarity_value
    
    df['output_similarity_value'] = df.apply(lambda x: float(Solve_sim_value_data(x.RawCol,x.StandardCol)), axis=1)
    return df['output_similarity_value']



##### == calculate_similarity == #######
def Get_similarity_of_RawColAndStandardCol(input_dataframe,\
                                           RawColWords,\
                                           StandardColWords,\
                                           PrefixOfEffectiveness):
    
    EffectivenessName = (PrefixOfEffectiveness + '_' + RawColWords.split('_')[0]).upper()
    
    output_dataframe = input_dataframe.withColumn(EffectivenessName,\
                                                 calulate_similarity_of_RawColAndStandardCol(col(RawColWords),col(StandardColWords)))
    
    return output_dataframe

##### == array 变 string
def Cause_ArrayStructureCol_Become_StringStructureCol(input_dataframe,inputCol):
    
    output_dataframe = input_dataframe.withColumn(inputCol,array_join(col(inputCol),delimiter=' '))
    
    
    return output_dataframe

###### 写入文件
def write_files(input_dataframe, path_of_write, file_type, repartition_num):
    
    try:
        if file_type.lower() == "parquet":
            input_dataframe.repartition(repartition_num).write.mode("overwrite").parquet(path_of_write)
        else:
            input_dataframe.repartition(1).write.mode("overwrite").csv(path_of_write,header=True)
        message = fr"{path_of_write} {file_type} Write Success!"
    except:
        message = fr"{path_of_write} {file_type} Write Failed!"
    print(message)
    return message