# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

import random
import string
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from pyspark.sql.functions import lit, udf, col, array_contains
from functools import reduce
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession


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
    
    
    _inputs = str(kwargs["raw_inputs"]).replace(" ", "").split(",")
    _time = str(kwargs["time"])
    _output = str(kwargs["clean_output"]) + _time
    
    
    def get_df(path):
        original_raw_df = spark.read.parquet(path)
        print(path + " ======> ")
        original_raw_df.show(2, False)
        # original_raw_df.printSchema()
    
    list(map(get_df, _inputs))

    return {}
