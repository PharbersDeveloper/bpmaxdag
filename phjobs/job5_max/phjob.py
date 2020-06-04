# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
import numpy as np
import pandas as pd
from phlogs.phlogs import phlogger

from pyspark.sql import SparkSession
import time
from pyspark.sql.types import *
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql import functions as func


def execute(a, b):
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("sparkOutlier") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.instance", "2") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()
        
    
    phlogger.info('job5_max')
    
        
    # =========== 数据检查 =============
    phlogger.info('数据检查-start')
    
    
    
    
    phlogger.info('数据检查-Pass')
    
    # =========== 数据执行 =============
    
    phlogger.info('数据执行-start')
    
    phlogger.info('数据执行-Finish')
        
    # =========== 数据验证 =============
    # 与原R流程运行的结果比较正确性: