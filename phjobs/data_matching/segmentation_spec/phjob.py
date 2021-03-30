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
    path_spec_lexicon = kwargs["spec_lexicon_path"]
    path_spec_stopwords = kwargs["spec_stopwords_path"]
    g_repartition_shared = int(kwargs["g_repartition_shared"])
    
###################=======input==========#################

###################=======output==========################# 
    job_id = get_job_id(kwargs)
    run_id = get_run_id(kwargs)
    result_path_prefix = get_result_path(kwargs, run_id, job_id)
    result_path = result_path_prefix + kwargs["segmentation_spec_result"]
###################=======output==========#################

###################=======loading files==========#################
    df_seg_spec = load_cross_result(spark,path_cross_result)
    spec_lexicon = load_spec_lexicon(spark, path_spec_lexicon)
    spec_stopwords = load_spec_stopwords(spark, path_spec_stopwords)
###################=======loading files==========#################

###################=======main functions==========#################
    df_seg_spec = cut_spec_word(df_seg_spec,spec_lexicon,spec_stopwords)
###################=======main functions==========#################

#################### == RESUTT == #####################
    df_seg_spec.repartition(g_repartition_shared).write.mode("overwrite").parquet(result_path)
#################### == RESUTT == #####################

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

def load_spec_lexicon(spark, path_spec_lexicon):
    if path_spec_lexicon == 'None':
        return None
    else:
        spec_lexicon = spark.read.csv(path_spec_lexicon,header=True) 
        return spec_lexicon

def load_spec_stopwords(spark, path_spec_stopwords):
    if path_spec_stopwords == "None":
        return None
    else:
        spec_stopwords = spark.csv(path_spec_stopwords,header=True)
        spec_stopwords = spec_stopwords.rdd.map(lambda x: x.STOPWORDS).collect()
        return spec_stopwords

def load_cross_result(spark,path_cross_result):
    df_seg_spec = spark.read.parquet(path_cross_result)
    df_seg_spec = df_seg_spec.select("ID","SPEC","SPEC_STANDARD","SPEC_VALID","SPEC_GROSS","SPEC_STANDARD_VALID","SPEC_STANDARD_GROSS")
    return df_seg_spec 


######## 分词逻辑 ##########
def phcleanning_spec_seg(df, df_lexicon, stopwords, inputCol, outputCol):
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
        df = remover.transform(df) #.drop(inputCol)
    return df
########  分词逻辑  ############

######  进行分词 ########
def cut_spec_word(df_seg_spec,spec_lexicon,spec_stopwords):
    
    df_seg_spec =phcleanning_spec_seg(df=df_seg_spec,df_lexicon=spec_lexicon,stopwords=spec_stopwords,inputCol="SPEC",outputCol="SPEC_CUT_WORDS")
    df_seg_spec =phcleanning_spec_seg(df=df_seg_spec,df_lexicon=spec_lexicon,stopwords=spec_stopwords,inputCol="SPEC_STANDARD",outputCol="SPEC_CUT_STANDARD_WORDS")
    return df_seg_spec

