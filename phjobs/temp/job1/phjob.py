# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
import time
from pyspark.sql.functions import regexp_extract , regexp_replace, upper ,concat_ws ,count , max ,col
from nltk.tokenize import sent_tokenize
from nltk.tokenize import word_tokenize

def execute(**kwargs):
    """
        please input your code below
        get spark session: spark = kwargs["spark"]()
    """
###############=======configure=======================############
    logger = phs3logger(kwargs["job_id"], LOG_DEBUG_LEVEL)
    logger.info("当前 owner 为 " + str(kwargs["owner"]))
    logger.info("当前 run_id 为 " + str(kwargs["run_id"]))
    logger.info("当前 job_id 为 " + str(kwargs["job_id"]))
    spark = kwargs["spark"]()
################=======configure=====================##############  

##############---------input-----------------------###############

#############----------input----------------------################


#############------loading files-------------------#################

############------loading files--------------------#################


#######################-------test--------------##################
    '''
    stopWords = ["高新", "化学", "生物", "合资", "中外", "工业", "现代", "化学制品" "科技", "国际", "AU", "OF", "US", "FR", "GE", "FI", "JP", "RO", "CA", "UK", "NO", "IS", "SI", "IT", "JA", \
				"省", "市", "股份", "有限",  "公司", "集团", "制药", "总厂", "厂", "责任", "医药", "(", ")", "（", "）", \
				 "有限公司", "控股", "总公司", "有限", "有限责任", "大药厂", '经济特区', '事业所', '株式会社', \
				 "药业", "制药", "制s3://ph-max-auto/2020-08-11/data_matching/refactor/results/2021-02-25_04-32-21/Report/药厂", "医药集团", "控股集团", "集团股份", "药厂", "分公司", "-", ".", "-", "·", ":", ","]
    stopWords = list(set(stopWords))
    stopWords_dict = {}
    stopWords_dict['STOPWORDS'] = stopWords
    print(stopWords_dict)
    df_mnf_stopwords = spark.createDataFrame(stopWords_dict)
    df_mnf_stopwords.show(50)
    '''
    df = spark.read.csv("s3a://ph-stream/common/public/prod/0.0.23",header=True,encoding='gbk')
    df.show()
    df.repartition(1).write.mode("overwrite").parquet("s3a://ph-stream/common/public/prod/0.0.23")
#####################-------test---------------################

    
    return {}
