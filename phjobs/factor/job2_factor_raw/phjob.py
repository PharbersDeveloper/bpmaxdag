# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from ph_logs.ph_logs import phs3logger
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql import functions as func
import os
from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col
import time
import re


def execute(**kwargs):
    """
        please input your code below
        get spark session: spark = kwargs["spark"]()
    """
    logger = phs3logger(kwargs["job_id"])
    logger.info("当前 owner 为 " + str(kwargs["owner"]))
    logger.info("当前 run_id 为 " + str(kwargs["run_id"]))
    logger.info("当前 job_id 为 " + str(kwargs["job_id"]))
    spark = kwargs["spark"]()
    #logger.info(kwargs["a"])
    #logger.info(kwargs["b"])
    #logger.info(kwargs["c"])
    #logger.info(kwargs["d"])
    #return {}
    '''
    os.environ["PYSPARK_PYTHON"] = "python3"
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("data from s3") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.instance", "1") \
        .config("spark.executor.memory", "1g") \
        .config('spark.sql.codegen.wholeStage', False) \
        .getOrCreate()
    
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    if access_key is not None:
        spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
        spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
        spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
        spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
        # spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
        spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.cn-northwest-1.amazonaws.com.cn")
    '''
    # 输入    
    #max_path = 's3a://ph-max-auto/v0.0.1-2020-06-08/'
    #project_name = 'Eisai'
    #outdir = '202009'
    #model_month_right = '201912'
    #model_month_left = '201901'
    #all_models = '固力康'
    max_path = kwargs["max_path"]
    project_name = kwargs["project_name"]
    max_file = kwargs["max_file"]
    outdir = kwargs["outdir"]
    model_month_right = kwargs["model_month_right"]
    model_month_left = kwargs["model_month_left"]
    all_models = kwargs["all_models"]
    
    model_month_right = int(model_month_right)
    model_month_left = int(model_month_left)
    all_models = all_models.replace(' ','').split(',')
    
    mkt_mapping_path = max_path + '/' + project_name + '/mkt_mapping'
    universe_path = max_path + '/' + project_name + '/universe_base'
    max_result_path = max_path + '/' + project_name + '/' + outdir + '/MAX_result/' + max_file
    #panel_result_path = max_path + '/' + project_name + '/' + outdir + '/panel_result'
    
    # =========== 数据执行 ============
    
    mkt_mapping = spark.read.parquet(mkt_mapping_path)
    universe = spark.read.parquet(universe_path)
    
    max_result = spark.read.parquet(max_result_path)
    max_result = max_result.where((col('Date') >= model_month_left) & (col('Date') <= model_month_right))
    
    #panel_result = spark.read.parquet(panel_result_path)
    #panel_result = panel_result.where((col('Date') >= model_month_left) & (col('Date') <= model_month_right))
    
    # 每个市场算 factor
    for market in all_models:
        #market = '固力康'
        # 输入
        rf_out_path = max_path + '/' + project_name + '/forest/' + market + '_rf_result'
        # 输出
        factor1_path = max_path + '/' + project_name + '/forest/' + market + '_factor_1'
        
        # 样本ID
        ID_list = universe.where(col('PANEL') == 1).select('Panel_ID').distinct().toPandas()['Panel_ID'].values.tolist()
        
        # panel 样本
        '''
        panel = panel_result.where(col('DOI') == market)
        panel1 = panel.where(col('HOSP_ID').isin(ID_list)) \
                    .drop('Province', 'City') \
                    .join(universe.select('Panel_ID', 'Province', 'City'), panel.HOSP_ID == universe.Panel_ID, how='inner')
        panel1 = panel1.groupBy('City').agg(func.sum('Sales').alias('panel_sales'))
        '''
        
        # rf 非样本
        rf_out = spark.read.parquet(rf_out_path)
        rf_out = rf_out.select('PHA_ID', 'final_sales') \
                        .join(universe.select('Panel_ID', 'Province', 'City'), rf_out.PHA_ID == universe.Panel_ID, how='left') \
                        .where(~col('PHA_ID').isin(ID_list))
        rf_out = rf_out.groupBy('City').agg(func.sum('final_sales').alias('Sales_rf'))
        
        # max 非样本
        spotfire_out = max_result.where(col('DOI') == market)
        spotfire_out = spotfire_out.where(col('PANEL') != 1) \
                                .groupBy('City').agg(func.sum('Predict_Sales').alias('Sales'))
        
        # 计算factor 城市层面 ： rf 非样本的Sales 除以  max 非样本 的Sales                
        factor_city = spotfire_out.join(rf_out, on='City', how='left')
        factor_city = factor_city.withColumn('factor', col('Sales_rf')/col('Sales'))
        
        # universe join left factor_city 没有的城市factor为1
        factor_city1 = universe.select('City').distinct() \
                                .join(factor_city, on='City', how='left')
        factor_city1 = factor_city1.withColumn('factor', func.when(col('factor').isNull(), func.lit(1)) \
                                                            .otherwise(col('factor')))
         
        factor_out = factor_city1.select('City', 'factor')
        
        factor_out = factor_out.repartition(1)
        factor_out.write.format("parquet") \
                .mode("overwrite").save(factor1_path)
    
    return {}