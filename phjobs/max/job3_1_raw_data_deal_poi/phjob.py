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
    project_name = kwargs['project_name']
    out_path = kwargs['out_path']
    run_id = kwargs['run_id']
    owner = kwargs['owner']
    # g_input_version = kwargs['g_input_version']
    g_database_temp = kwargs['g_database_temp']
    g_database_input = kwargs['g_database_input']
    ### input args ###
    
    ### output args ###
    # g_out_raw_data_deal_poi = kwargs['g_out_raw_data_deal_poi']
    ### output args ###

    from pyspark.sql import SparkSession, Window
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    import os
    import pandas as pd
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col    
    import json
    import boto3        
    
    # %% 
    # 输入数据读取
    df_raw_data = kwargs['df_product_mapping_out']
    df_poi = kwargs['df_poi']
    # %%
    # =========== 数据执行 =============
    logger.debug('数据执行-start')
    
    # 数据读取
    products_of_interest = df_poi.select('poi').distinct() \
                                .toPandas()["poi"].values.tolist()
    # raw_data 处理
    df_raw_data = df_raw_data.withColumn("S_Molecule_for_gr",
                                   func.when(col("标准商品名").isin(products_of_interest), col("标准商品名")).
                                   otherwise(col('S_Molecule')))
    # %%
    # =========== 数据输出 =============    
    def lowerColumns(df):
        df = df.toDF(*[i.lower() for i in df.columns])
        return df
    
    df_raw_data = lowerColumns(df_raw_data)
    
    logger.debug('数据执行-Finish')
    
    return {'out_df':df_raw_data}
