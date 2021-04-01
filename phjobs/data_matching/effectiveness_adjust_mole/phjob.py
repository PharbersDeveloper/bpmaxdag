# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""
from phcli.ph_logs.ph_logs import phs3logger
import uuid
import pandas as pd
import numpy as np
import pkuseg
import math
from math import isnan, sqrt
from pyspark.sql.types import *
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.functions import col, monotonically_increasing_id, explode
from pyspark.sql.functions import when 
from pyspark.sql.functions import array_distinct, array
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.feature import StopWordsRemover
from nltk.metrics.distance import jaro_winkler_similarity


def execute(**kwargs):
    """
        please input your code below
        get spark session: spark = kwargs["spark"]()
    """
##################=========configure===========###################
    logger = phs3logger(kwargs["job_id"])
    spark = kwargs["spark"]()
    logger.info(kwargs)
##################=========configure===========###################

############-----------input-------------------------###################
    depends = get_depends_path(kwargs)
    path_effective_result = depends["input"]
    g_repartition_shared = int(kwargs["g_repartition_shared"])
    mole_word_dict_encode_path = kwargs["word_dict_encode_path"]
    mole_lexicon_path = kwargs["mnf_lexicon_path"]
    mole_stopwords_path = kwargs["mnf_stopwords_path"]
############-----------input-------------------------###################

###########------------output------------------------###################
    job_id = get_job_id(kwargs)
    run_id = get_run_id(kwargs)
    result_path_prefix = get_result_path(kwargs, run_id, job_id)
    result_path = result_path_prefix + kwargs["mole_adjust_result"]
    mid_path= result_path_prefix + kwargs["mole_adjust_mid"]
###########------------output------------------------###################

###############--------loading files --------------##################
    mole_lexicon = load_mole_lexicon(spark, mole_lexicon_path)
    mole_stopwords = load_mole_stopwords(spark, mole_stopwords_path)
    df_cleanning  = load_effective_result(spark, path_effective_result)
    df_encode = load_word_dict_encode(spark, mole_word_dict_encode_path)
    #mole硬编码字典
    mole_mapping_code_dict = make_mole_segmentation_mapping_code(df_encode)
###############--------loading files----------------#################


#########--------------main function--------------------#################  
    #进行分词处理
    df_cleanning =  deal_with_word_segmentation_mole(df_cleanning, mole_lexicon, mole_stopwords)
#     df_cleanning = mnf_encoding_index(df_cleanning, df_encode)
    
    #mnf分词映射成编码
    df_cleanning = make_mole_word_segmentation_convert_into_code(df_cleanning, mole_mapping_code_dict)
    
    df_cleanning = mole_encoding_cosine(df_cleanning)
    
    #mnf有效性计算
    df_cleanning = get_mole_efftiveness(df_cleanning)
    
    df_cleanning.repartition(g_repartition_shared).write.mode("overwrite").parquet(mid_path)
    #选取指定的列用于和adjust_spec job 进行union操作
    df_cleanning = select_specified_cols(df_cleanning)

    df_cleanning.repartition(g_repartition_shared).write.mode("overwrite").parquet(result_path)
#########--------------main function--------------------#################  
    return {}
################--------------------- functions ---------------------################
"""
中间文件与结果文件路径
"""
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

def load_effective_result(spark, path_effective_result):
    path_effective_result = r's3a://ph-max-auto/2020-08-11/data_matching/refactor/runs/manual__2021-03-11T23_02_47.745378+00_00/effectiveness_with_jws/effective_result'
    df_cleanning = spark.read.parquet(path_effective_result)
    return df_cleanning

def load_mole_stopwords(spark, mole_stopwords_path):
    mole_stopwords = spark.read.csv(mole_stopwords_path, header=True)
    mole_stopwords = mole_stopwords.rdd.map(lambda x : x.STOPWORDS).collect()
    return mnf_stopwords
    
"""
读取分子名配置表
"""
def load_mole_word_dict_encode(spark, mole_word_dict_encode_path):
    df_encode = spark.read.parquet(mole_word_dict_encode_path)
    return df_encode

def load_mole_lexicon(spark, mole_lexicon_path):
    mole_lexicon = spark.read.csv(mole_lexicon_path, header=True)
    return mole_lexicon
    
'''
mole分词逻辑
'''
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
    df = df.withColumn(inputCol, manifacture_name_pseg_cut(col(inputCol)))
    # 4. 分词之后构建词库编码
    # 4.1 stop word remover 去掉不需要的词
    if stopwords is None:
        pass
    else:
        remover = StopWordsRemover(stopWords=stopwords, inputCol=inputCol, outputCol=outputCol)
        df = remover.transform(df) #.drop(inputCol)
    return df

def deal_with_word_segmentation_mole(df_cleanning, mole_lexicon, mole_stopwords):
    
    df_cleanning = phcleanning_mole_seg(df=df_cleanning, df_lexicon=mole_lexicon, stopwords=mole_stopwords, inputCol="MOLE_NAME_STANDARD", outputCol="MOLE_NAME_STANDARD_WORDS")
    df_cleanning = phcleanning_mole_seg(df=df_cleanning, df_lexicon=mole_lexicon, stopwords=mole_stopwords, inputCol="MOLE_NAME", outputCol="MOLE_NAME_CLEANNING_WORDS")

    return df_cleanning

#分词余弦处理优化逻辑
def make_mole_segmentation_mapping_code(df):
    list_word = df.rdd.map(lambda x: x.WORD).collect()
    list_encode = df.rdd.map(lambda x: x.ENCODE).collect()
    mole_mapping_code_dict = dict(zip(list_word,list_encode))
    return mole_mapping_code_dict 

#mole分词映射成编码
def make_mole_word_segmentation_convert_into_code(df_cleanning,mole_mapping_code_dict):
    
    df_cleanning = make_mole_word_into_code(df_cleanning, mole_mapping_code_dict,\
                                           df_cleanning.MOLE_NAME_STANDARD_WORDS,output_col="MOLE_NAME_STANDARD_WORDS_CODE")
    df_cleanning = make_mole_word_into_code(df_cleanning,mole_mapping_code_dict,\
                                           df_cleanning.MOLE_NAME_CLEANNING_WORDS,output_col="MOLE_NAME_CLEANNING_WORDS_CODE")
    df_cleanning.select("MOLE_NAME_STANDARD_WORDS",\
                        "MOLE_NAME_STANDARD_WORDS_CODE",\
                        "MOLE_NAME_CLEANNING_WORDS",\
                        "MOLE_NAME_CLEANNING_WORDS_CODE").distinct().show(500)
    print(df_cleanning.printSchema())
    
    return df_cleanning

def make_mole_word_into_code(df_cleanning, mole_mapping_code_dict, input_col, output_col):
    mole_mapping_code_dict = mole_mapping_code_dict
    @pandas_udf(ArrayType(IntegerType()),PandasUDFType.SCALAR)
    def make_string_convert_into_code(input_col):
        frame = {"input_col":input_col}
        df = pd.DataFrame(frame)
        nonlocal mole_mapping_code_dict
        def execute_string_into_code(s):
            if s in mole_mapping_code_dict.keys():
                value = mole_mapping_code_dict[s]
                if value != None:
                    value = int(value)
                else:
                    value = int(8999)
            else:
                value = int(8999)
            return value 

        df['output_col'] = df.apply(lambda x: np.array(list(map(execute_string_into_code,x.input_col))), axis=1)
        return df['output_col']
    df_cleanning = df_cleanning.withColumn(output_col,make_string_convert_into_code(input_col))
    return df_cleanning

#余弦相似度
def mole_encoding_cosine(df_cleanning):
    df_cleanning = df_cleanning.withColumn("MOLE_COSINE_SIMILARITY", \
                                mole_index_word_cosine_similarity(df_cleanning.MOLE_NAME_CLEANNING_WORDS_CODE,\
                                                                 df_cleanning.MOLE_NAME_STANDARD_WORDS_CODE))
    return df_cleanning

#mnf有效性计算
def get_mole_efftiveness(df_cleanning):
    
    df_cleanning = df_cleanning.withColumn("EFFTIVENESS_MOLE_SE", \
                                    when(df_cleanning.MOLE_COSINE_SIMILARITY >= df_cleanning.EFFTIVENESS_MOLE,\
                                         df_cleanning.MOLE_COSINE_SIMILARITY).otherwise(df_cleanning.EFFTIVENESS_MOLE))
    
    return df_cleanning
    
def select_specified_cols(df_cleanning):

    cols = ["SID", "ID","PACK_ID_CHECK",  "PACK_ID_STANDARD","DOSAGE","MOLE_NAME","PRODUCT_NAME","SPEC","PACK_QTY","MANUFACTURER_NAME","SPEC_ORIGINAL",
                "MOLE_NAME_STANDARD","PRODUCT_NAME_STANDARD","CORP_NAME_STANDARD","MANUFACTURER_NAME_STANDARD","MANUFACTURER_NAME_EN_STANDARD","DOSAGE_STANDARD","SPEC_STANDARD","PACK_QTY_STANDARD",
                "SPEC_STANDARD_GROSS","SPEC_STANDARD_VALID","SPEC_GROSS","SPEC_VALID",
                "EFFTIVENESS_MOLE_NAME","EFFTIVENESS_PRODUCT_NAME","EFFTIVENESS_DOSAGE","EFFTIVENESS_PACK_QTY","EFFTIVENESS_MANUFACTURER","EFFTIVENESS_SPEC"]
    df_cleanning = df_cleanning.select(cols)
    
    return df_cleanning

@pandas_udf(ArrayType(IntegerType()), PandasUDFType.GROUPED_AGG)
def word_index_to_array(v):
    return v.tolist()

@pandas_udf(DoubleType(), PandasUDFType.SCALAR)
def mole_index_word_cosine_similarity(o, v):
    frame = {
        "CLEANNING": o,
        "STANDARD": v
    }
    df = pd.DataFrame(frame)
    def array_to_vector(arr):
        idx = []
        values = []
        # try:
        if type(arr) != np.ndarray:
            s = [8999,]
        # except AttributeError:
        else:
            s = list(set(arr))
        s.sort()
        for item in s:
            if isnan(item):
                idx.append(8999)
                values.append(1)
                break
            else:
                idx.append(item)
                if item < 2000:
                    values.append(2)
                elif (item >= 2000) & (item < 5000):
                    values.append(10)
                else:
                    values.append(1)
        return Vectors.sparse(9000, idx, values)
        #                    (向量长度，索引数组，与索引数组对应的数值数组)
    def cosine_distance(u, v):
        u = u.toArray()
        v = v.toArray()
        return float(np.dot(u, v) / (sqrt(np.dot(u, u)) * sqrt(np.dot(v, v))))
    df["CLENNING_FEATURE"] = df["CLEANNING"].apply(lambda x: array_to_vector(x))
    df["STANDARD_FEATURE"] = df["STANDARD"].apply(lambda x: array_to_vector(x))
    df["RESULT"] = df.apply(lambda x: cosine_distance(x["CLENNING_FEATURE"], x["STANDARD_FEATURE"]), axis=1)
    return df["RESULT"]

################-----------------------------------------------------################