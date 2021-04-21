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
    url = r"s3a://ph-max-auto/2020-08-11/data_matching/refactor/results/2021-04-21_08-00-18/Report/"
    df = spark.read.csv(url,header=True)
    df.show()
#####################-------test---------------################

    
    return {}
