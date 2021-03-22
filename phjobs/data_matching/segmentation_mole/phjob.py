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
    path_mole_lexicon = kwargs["mole_lexicon_path"]
    path_mole_stopwords = kwargs["mole_stopwords_path"]
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
    mole_lexicon = load_mole_lexicon(spark, path_mole_lexicon)
    mole_stopwords = load_mole_stopwords(spark, path_mole_lexicon)
###################=======loading files==========#################

###################=======main functions==========#################
    df_seg_mole = cut_mole_word(df_seg_mole,mole_lexicon,mole_stopwords)
###################=======main functions==========#################
    df_seg_mole.repartition(g_repartition_shared).write.mode("overwrite").parquet(result_path)
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

def load_mole_lexicon(spark, path_mole_lexicon):
    if path_mole_lexicon == 'None':
        return None
    else:
        mole_lexicon = spark.read.csv(path_mole_lexicon,header=True) 
        return mole_lexicon

def load_mole_stopwords(spark, path_mole_stopwords):
    if path_mole_stopwords == "None":
        return None
    else:
        mole_stopwords = spark.csv(path_mole_stopwords,header=True)
        return mole_stopwords

def load_cross_result(spark,path_cross_result):
    path_cross_result = r"s3a://ph-max-auto/2020-08-11/data_matching/refactor/runs/manual__2021-03-19T08_05_34.344972+00_00/cross_join_cutting/cross_result"
    df_seg_mole = spark.read.parquet(path_cross_result)
    df_seg_mole = df_seg_mole.select("ID","MOLE_NAME","MOLE_NAME_STANDARD")
    df_seg_mole.persist()
    
    return df_seg_mole 


######## 分词逻辑 ##########
def phcleanning_mole_seg(df, df_lexicon, stopwords, inputCol, outputCol):
    if df_lexicon is None: 
        lexicon = None 
    else:
        lexicon = df_lexicon.rdd.map(lambda x: x.lexicon).collect()
    seg = pkuseg.pkuseg(user_dict=lexicon)
    @pandas_udf(ArrayType(StringType()), PandasUDFType.SCALAR)
    def manifacture_name_pseg_cut(inputCol):
        nonlocal seg 
        frame = {
            "inputCol_name": inputCol,
        }
        df = pd.DataFrame(frame)
        df["be_cut_col"] = df["inputCol_name"].apply(lambda x: seg.cut(x))
        return df["be_cut_col"]
    
    # 3. 中文的分词
    df = df.withColumn(outputCol, manifacture_name_pseg_cut(col(inputCol)))
    # 4. 分词之后构建词库编码
    # 4.1 stop word remover 去掉不需要的词
    if stopwords is None:
        pass
    else:
        remover = StopWordsRemover(stopWords=stopwords, inputCol=inputCol, outputCol=outputCol)
        df = remover.transform(df) #.drop(inputCol)
    return df
########  分词逻辑  ############

######  进行分词 ########
def cut_mole_word(df_seg_mole,mole_lexicon,mole_stopwords):
    
    df_seg_mole =phcleanning_mole_seg(df=df_seg_mole,df_lexicon=mole_lexicon,stopwords=mole_stopwords,inputCol="MOLE_NAME",outputCol="MOLE_CUT_WORDS")
    df_seg_mole =phcleanning_mole_seg(df=df_seg_mole,df_lexicon=mole_lexicon,stopwords=mole_stopwords,inputCol="MOLE_NAME_STANDARD",outputCol="MOLE_CUT_STANDARD_WORDS")
    df_seg_mole.select("ID","MOLE_NAME","MOLE_CUT_WORDS","MOLE_NAME_STANDARD","MOLE_CUT_STANDARD_WORDS").show(100)
    return df_seg_mole

