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
    max_path = kwargs['max_path']
    project_name = kwargs['project_name']
    outdir = kwargs['outdir']
    model_month_right = kwargs['model_month_right']
    model_month_left = kwargs['model_month_left']
    all_models = kwargs['all_models']
    max_file = kwargs['max_file']
    factor_optimize = kwargs['factor_optimize']
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
    import time
    import re        # %%
    
    # project_name = 'Sanofi'
    # outdir = '202012'
    # model_month_right = '202012'
    # model_month_left = '202001'
    # all_models = 'SNY15,SNY16,SNY17'
    # max_file = 'MAX_result_201801_202012_city_level'
    

    # %%
    # 输出
    model_month_right = int(model_month_right)
    model_month_left = int(model_month_left)
    all_models = all_models.replace(' ','').split(',')
    
    mkt_mapping_path = max_path + '/' + project_name + '/mkt_mapping'
    universe_path = max_path + '/' + project_name + '/universe_base'
    max_result_path = max_path + '/' + project_name + '/' + outdir + '/MAX_result/' + max_file
    #panel_result_path = max_path + '/' + project_name + '/' + outdir + '/panel_result'

    # %%
    # =========== 数据执行 ============
    logger.debug("job2_factor_raw")
    mkt_mapping = spark.read.parquet(mkt_mapping_path)
    universe = spark.read.parquet(universe_path)
    
    max_result = spark.read.parquet(max_result_path)
    max_result = max_result.where((col('Date') >= model_month_left) & (col('Date') <= model_month_right))
    
    #panel_result = spark.read.parquet(panel_result_path)
    #panel_result = panel_result.where((col('Date') >= model_month_left) & (col('Date') <= model_month_right))
    
    # 每个市场算 factor
    for market in all_models:
        logger.debug("当前market为:" + str(market))
        #market = '固力康'
        # 输入
        rf_out_path = max_path + '/' + project_name + '/forest/' + market + '_rf_result'
        # 输出
        if factor_optimize == 'True':
            factor1_path = max_path + '/' + project_name + '/forest/' + market + '_factor_1'
        else:
            factor_out_path = max_path + '/' + project_name + '/factor/factor_' + market
    
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
                        .join(universe.select('Panel_ID', 'Province', 'City').distinct(), 
                                rf_out.PHA_ID == universe.Panel_ID, how='left') \
                        .where(~col('PHA_ID').isin(ID_list))
        rf_out = rf_out.groupBy('City', 'Province').agg(func.sum('final_sales').alias('Sales_rf'))
    
        # max 非样本
        spotfire_out = max_result.where(col('DOI') == market)
        spotfire_out = spotfire_out.where(col('PANEL') != 1) \
                                .groupBy('City', 'Province').agg(func.sum('Predict_Sales').alias('Sales'))
    
        # 计算factor 城市层面 ： rf 非样本的Sales 除以  max 非样本 的Sales                
        factor_city = spotfire_out.join(rf_out, on=['City', 'Province'], how='left')
        factor_city = factor_city.withColumn('factor', col('Sales_rf')/col('Sales'))
    
        # universe join left factor_city 没有的城市factor为1
        factor_city1 = universe.select('City', 'Province').distinct() \
                                .join(factor_city, on=['City', 'Province'], how='left')
        factor_city1 = factor_city1.withColumn('factor', func.when(((col('factor').isNull()) | (col('factor') <=0)), func.lit(1)) \
                                                            .otherwise(col('factor')))
        factor_city1 = factor_city1.withColumn('factor', func.when(col('factor') >4, func.lit(4)) \
                                                            .otherwise(col('factor')))
    
        factor_out = factor_city1.select('City', 'Province', 'factor')
    
    
        if factor_optimize == 'True':
            factor_out = factor_out.repartition(1)
            factor_out.write.format("parquet") \
                .mode("overwrite").save(factor1_path)
        else:
            factor_out = factor_out.repartition(1)
            factor_out.write.format("parquet") \
                .mode("overwrite").save(factor_out_path)        
    
        
        logger.debug("finish:" + str(market))

