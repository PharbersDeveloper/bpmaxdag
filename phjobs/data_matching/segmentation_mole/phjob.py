# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
import uuid
import pkuseg
import numpy as np
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.functions import col, when, split,count
from pyspark.sql.functions import array_distinct, array
from pyspark.ml.linalg import Vectors ,VectorUDT
from pyspark.ml.feature import StopWordsRemover


def execute(**kwargs):
    """
        please input your code below
        get spark session: spark = kwargs["spark"]()
    """
###################=======configure==========#################
    logger = phs3logger(kwargs["job_id"], LOG_DEBUG_LEVEL)
    spark = kwargs["spark"]()
    logger.info(kwargs)
###################=======configure==========#################
    
###################=======input==========#################
    depends = get_depends_path(kwargs)
    path_cross_result = depends["input_cross_result"]
    path_mole_stopwords = kwargs["mole_stopwords_path"]
    path_mole_lexicon = kwargs["mole_lexicon_path"]
    g_repartition_shared = int(kwargs["g_repartition_shared"])
    
###################=======input==========#################

###################=======output==========################# 
    job_id = get_job_id(kwargs)
    run_id = get_run_id(kwargs)
    result_path_prefix = get_result_path(kwargs, run_id, job_id)
    result_path = result_path_prefix + kwargs["segmentation_mole_result"]
###################=======output==========#################

###################=======loading files==========#################
    df_seg_mole = load_cross_result(spark,path_cross_result)
    mole_stopwords = load_mole_stopwords(spark, path_mole_stopwords)
    mole_lexicon = load_mole_lexicon(spark,path_mole_lexicon)
###################=======loading files==========#################

###################=======main functions==========#################
    
    
    #### == raw_table 分词、停用词处理
    df_seg_mole = phcleanning_mole_seg(input_dataframe=df_seg_mole,\
                        input_lexicon=mole_lexicon,\
                        input_stopwords=mole_stopwords,\
                        inputCol="MOLE_NAME",\
                        outputCol="MOLE_NAME_WORDS")
    
    #### == standard_table 分词、停用词处理
    df_seg_mole = phcleanning_mole_seg(input_dataframe=df_seg_mole,\
                                     input_lexicon=mole_lexicon,\
                                     input_stopwords=mole_stopwords,\
                                     inputCol="MOLE_NAME_STANDARD",\
                                     outputCol="MOLE_NAME_STANDARD_WORDS")
    
    
###################=======main functions==========#################
    
    #写入路径
    write_files(input_dataframe=df_seg_mole,\
                path_of_write=result_path,\
                file_type="parquet",\
               repartition_num = g_repartition_shared)
   
    return {}


########### === FUNCTIONS === ###########

###################  中间文件与结果文件路径  ######################
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
##################  中间文件与结果文件路径  ######################

def load_mole_stopwords(spark, path_mole_stopwords):
    
    try:
        mole_stopwords = spark.read.csv(path_mole_stopwords,header=True)
        mole_stopwords = mole_stopwords.rdd.map(lambda x : x.STOPWORDS).collect()
    except:
        mole_stopwords = None
        
    return mole_stopwords

def load_cross_result(spark,path_cross_result):
    
    df_seg_mole = spark.read.parquet(path_cross_result)
    df_seg_mole = df_seg_mole.select("ID","INDEX","MOLE_NAME","MOLE_NAME_STANDARD")
    df_seg_mole.persist()
    
    return df_seg_mole 

def load_mole_lexicon(spark,path_mole_lexicon):
    
    try:
        df_lexicon = spark.read.csv(path_mole_lexicon,header=True)
        mole_lexicon = df_lexicon.rdd.map(lambda x: x.lexicon).distinct().collect()
    except:
        mole_lexicon = None
        
    return mole_lexicon

######## 分词逻辑 ##########
def phcleanning_mole_seg(input_dataframe,\
                        input_lexicon,\
                        input_stopwords,\
                        inputCol,\
                        outputCol):
    
    seg = pkuseg.pkuseg(user_dict=input_lexicon)
    inputColByCut = inputCol + 'ByCut'
    
    @pandas_udf(ArrayType(StringType()), PandasUDFType.SCALAR)
    def cut_words(inputCol):
        nonlocal seg ,inputColByCut
        frame = {
            "inputCol_name": inputCol,
        }
        df = pd.DataFrame(frame)
        df[inputColByCut] = df["inputCol_name"].apply(lambda x: seg.cut(x))
        return df[inputColByCut]
    
    # 3. 中文的分词
    input_dataframe = input_dataframe.withColumn(inputColByCut, cut_words(col(inputCol)))
    # 4. 分词之后构建词库编码
    
    # 4.1 stop word remover 去掉不需要的词
    try:
        remover = StopWordsRemover(stopWords=input_stopwords,\
                                   inputCol=inputColByCut, outputCol=outputCol)
        output_dataframe = remover.transform(input_dataframe).drop(inputColByCut)
    except:
        output_dataframe = input_dataframe.withColumnRenamed(inputColByCut,outputCol)
        
    return output_dataframe

########  分词逻辑  ############

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
    
    
    
    