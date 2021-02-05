# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
import time
from pyspark.sql.functions import regexp_extract , regexp_replace, upper ,concat_ws ,count , max ,col

def execute(**kwargs):
    """
        please input your code below
        get spark session: spark = kwargs["spark"]()
    """
    logger = phs3logger(kwargs["job_id"], LOG_DEBUG_LEVEL)
    logger.info("当前 owner 为 " + str(kwargs["owner"]))
    logger.info("当前 run_id 为 " + str(kwargs["run_id"]))
    logger.info("当前 job_id 为 " + str(kwargs["job_id"]))
    spark = kwargs["spark"]()
  

  
    
#     df_label =spark.read.csv("s3a://ph-max-auto/2020-08-11/data_matching/refactor/results/2021-02-04_13-14-33/Report",header=True)
    
    df_negative = spark.read.csv("s3a://ph-max-auto/2020-08-11/data_matching/refactor/results/2021-02-05_00-02-13/Report/",header=True)
    df_negative.show()

#     df_label.show(50)
  
#     df_negative.show(20)
#     print(df_negative.columns)
#     cols =[ 'DOSAGE_STANDARD','DOSAGE', 'SPEC_STANDARD','SPEC', 'EFFTIVENESS_SPEC', 'SPEC_ORIGINAL','SIMILARITY']
#     df_negative.filter(col("EFFTIVENESS_SPEC")>0.9).select(cols).show(500)
    
    return {}
