# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL


def execute(**kwargs):
    logger = phs3logger(kwargs["job_id"], LOG_DEBUG_LEVEL)
    spark = kwargs['spark']()
    result_path_prefix = kwargs["result_path_prefix"]
    depends_path = kwargs["depends_path"]
    
    ### input args ###
    g_input_check = kwargs['g_input_check']
    g_check_standard = kwargs['g_check_standard']
    run_id = kwargs['run_id']
    project_name = kwargs['project_name']
    ### input args ###
    
    ### output args ###
    c = kwargs['c']
    d = kwargs['d']
    ### output args ###

    from pyspark.sql import SparkSession, Window
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    import os
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col
    import json
    import boto3     # %% 
    # 输入参数处理
    def getParameter(g_input_check):
        dict_input_check = json.loads(g_input_check)
        logger.debug(g_input_check)
        return dict_input_check
    
    dict_input_check = getParameter(g_input_check)
    # %% 
    # 数据处理
    def dataProcess(dict_input_check):
        return personalProcess(dict_input_check)
    # %%
    # 数据检查
    def dataCheck(dict_input_check, g_check_standard):
        out = dataProcess(dict_input_check)
        check_formula = str(out) + g_check_standard
        logger.debug(check_formula)
        if eval(check_formula):
            logger.debug('check pass')
        else:
            raise ValueError('check fail: %s' %(check_formula))  
    # %%
    # 执行
    dataCheck(dict_input_check, g_check_standard)
    # %%
    def personalProcess(dict_input_check):
        # 放大比例 最小值
        # 1. 数据读取
        df_ims_sales_info = spark.sql("SELECT * FROM phdatatemp.ims_sales_info WHERE version='%s'"
                                 %(dict_input_check['ims_sales_info']))
    
    
        df_max_result_standard = spark.sql("SELECT * FROM phdatatemp.max_result_standard WHERE version='%s'"
                                 %(run_id))
        df_max_result_standard = df_max_result_standard.drop('version', 'provider', 'owner')
        
        # 2. 数据分析
        max_data_m = df_max_result_standard.withColumn("标准通用名", col('molecule'))
        check_max_sales = max_data_m.groupby('Date', '标准通用名', 'PANEL').agg(func.sum('Predict_Sales').alias('金额'))
        check_max_sales = check_max_sales.groupBy('Date', '标准通用名').pivot('PANEL').agg(func.sum('金额')).persist()
        check_max_sales = check_max_sales.withColumn('放大比例', (col('1')+col('0'))/col('1'))
        out = check_max_sales.agg(func.min('放大比例')).toPandas().iloc[0,0]
        
        return out
