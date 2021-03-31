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
    path_mnf_lexicon = kwargs["mnf_lexicon_path"]
    path_mnf_stopwords = kwargs["mnf_stopwords_path"]
    g_repartition_shared = int(kwargs["g_repartition_shared"])
    
###################=======input==========#################

###################=======output==========################# 
    job_id = get_job_id(kwargs)
    run_id = get_run_id(kwargs)
    result_path_prefix = get_result_path(kwargs, run_id, job_id)
    result_path = result_path_prefix + kwargs["segmentation_mnf_result"]
###################=======output==========#################

###################=======loading files==========#################
    df_seg_mnf = load_cross_result(spark,path_cross_result)
    mnf_lexicon = load_mnf_lexicon(spark, path_mnf_lexicon)
    mnf_stopwords = load_mnf_stopwords(spark, path_mnf_stopwords)
###################=======loading files==========#################

###################=======main functions==========#################
    df_seg_mnf = cut_mnf_word(df_seg_mnf,mnf_lexicon,mnf_stopwords)
###################=======main functions==========#################
    df_seg_mnf.repartition(g_repartition_shared).write.mode("overwrite").parquet(result_path)
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

def load_mnf_lexicon(spark, path_mnf_lexicon):
    mnf_lexicon = spark.read.csv(path_mnf_lexicon,header=True) 
    return mnf_lexicon

def load_mnf_stopwords(spark, path_mnf_stopwords):
    mnf_stopwords = spark.read.csv(path_mnf_stopwords,header=True)
    mnf_stopwords = mnf_stopwords.rdd.map(lambda x : x.STOPWORDS).collect()
    return mnf_stopwords

def load_cross_result(spark,path_cross_result):
    df_seg_mnf = spark.read.parquet(path_cross_result)
    df_seg_mnf = df_seg_mnf.select("ID","INDEX","MANUFACTURER_NAME","MANUFACTURER_NAME_STANDARD","MANUFACTURER_NAME_EN_STANDARD")
    return df_seg_mnf 


######## 分词逻辑 ##########
def phcleanning_mnf_seg(df, df_lexicon, stopwords, inputCol, outputCol):
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
    
    df = df.withColumn("temp_col", manifacture_name_pseg_cut(col(inputCol)))
    remover = StopWordsRemover(stopWords=stopwords, inputCol="temp_col", outputCol=outputCol)
    df = remover.transform(df).drop("temp_col")
    return df

######  进行分词 ########
def cut_mnf_word(df_seg_mnf,mnf_lexicon,mnf_stopwords):
    
    df_seg_mnf =phcleanning_mnf_seg(df=df_seg_mnf,df_lexicon=mnf_lexicon,stopwords=mnf_stopwords,inputCol="MANUFACTURER_NAME",outputCol="MANUFACTURER_NAME_CUT_WORDS")
    df_seg_mnf =phcleanning_mnf_seg(df=df_seg_mnf,df_lexicon=mnf_lexicon,stopwords=mnf_stopwords,inputCol="MANUFACTURER_NAME_STANDARD",outputCol="MANUFACTURER_NAME_STANDARD_CUT_STANDARD_WORDS")
    return df_seg_mnf

