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
    path_dosage_lexicon = kwargs["dosage_lexicon_path"]
    path_dosage_stopwords = kwargs["dosage_stopwords_path"]
    g_repartition_shared = int(kwargs["g_repartition_shared"])
    
###################=======input==========#################

###################=======output==========################# 
    job_id = get_job_id(kwargs)
    run_id = get_run_id(kwargs)
    result_path_prefix = get_result_path(kwargs, run_id, job_id)
    result_path = result_path_prefix + kwargs["segmentation_dosage_result"]
###################=======output==========#################

###################=======loading files==========#################
    df_seg_dosage = load_cross_result(spark,path_cross_result)
    dosage_lexicon = load_dosage_lexicon(spark, path_dosage_lexicon)
    dosage_stopwords = load_dosage_stopwords(spark, path_dosage_stopwords)
####################=======loading files==========#################

####################=======main functions==========#################
    df_seg_dosage = cut_dosage_word(df_seg_dosage,dosage_lexicon,dosage_stopwords)
####################=======main functions==========#################

####################### == RESULT == #####################

    df_seg_dosage.repartition(g_repartition_shared).write.mode("overwrite").parquet(result_path)
    
####################### == RESULT == #####################
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

def load_dosage_lexicon(spark, path_dosage_lexicon):
    if path_dosage_lexicon == 'None':
        return None
    else:
        dosage_lexicon = spark.read.csv(path_dosage_lexicon,header=True) 
        return dosage_lexicon

def load_dosage_stopwords(spark, path_dosage_stopwords):
    if path_dosage_stopwords == "None":
        return None
    else:
        dosage_stopwords = spark.csv(path_dosage_stopwords,header=True)
        dosage_stopwords = dosage_stopwords.rdd.map(lambda x : x.STOPWORDS).collect()
        return dosage_stopwords

def load_cross_result(spark,path_cross_result):
    path_cross_result = r"s3a://ph-max-auto/2020-08-11/data_matching/refactor/runs/manual__2021-03-19T08_05_34.344972+00_00/cross_join_cutting/cross_result"
    df_seg_dosage = spark.read.parquet(path_cross_result)
    df_seg_dosage = df_seg_dosage.select("ID","DOSAGE","DOSAGE_STANDARD","PACK_ID_CHECK","PACK_ID_STANDARD")
    
    return df_seg_dosage 


######## 分词逻辑 ##########
def phcleanning_dosage_seg(df, df_lexicon, stopwords, inputCol, outputCol):
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
        remover = StopWordsRemover(stopWords=stopwords, inputCol=outputCol, outputCol=outputCol)
        df = remover.transform(df)# .drop("temp_col")
    return df
########  分词逻辑  ############

######  进行分词 ########
def cut_dosage_word(df_seg_dosage,dosage_lexicon,dosage_stopwords):
    
    df_seg_dosage =phcleanning_dosage_seg(df=df_seg_dosage,df_lexicon=dosage_lexicon,stopwords=dosage_stopwords,inputCol="DOSAGE",outputCol="DOSAGE_CUT_WORDS")
    df_seg_dosage =phcleanning_dosage_seg(df=df_seg_dosage,df_lexicon=dosage_lexicon,stopwords=dosage_stopwords,inputCol="DOSAGE_STANDARD",outputCol="DOSAGE_CUT_STANDARD_WORDS")
    df_seg_dosage.select("ID","DOSAGE","DOSAGE_CUT_WORDS","DOSAGE_STANDARD","DOSAGE_CUT_STANDARD_WORDS").show(100)
    return df_seg_dosage

