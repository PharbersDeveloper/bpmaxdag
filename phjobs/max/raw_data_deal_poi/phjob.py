# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL


def execute(**kwargs):
    logger = phs3logger(kwargs["run_id"], LOG_DEBUG_LEVEL)
    spark = kwargs['spark']
    
    ### input args ###
    g_input_version = kwargs['g_input_version']
    ### input args ###
    
    ### output args ###
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
    # =========== 输入数据读取 =========== 
    def dealToNull(df):
        df = df.replace(["None", ""], None)
        return df
    
    def dealScheme(df, dict_scheme):
        # 数据类型处理
        if dict_scheme != {}:
            for i in dict_scheme.keys():
                df = df.withColumn(i, col(i).cast(dict_scheme[i]))
        return df
    
    def getInputVersion(df, table_name):
        # 如果 table在g_input_version中指定了version，则读取df后筛选version，否则使用传入的df
        version = g_input_version.get(table_name, '')
        if version != '':
            version_list =  version.replace(' ','').split(',')
            df = df.where(col('version').isin(version_list))
        return df
    
    def readInFile(table_name, dict_scheme={}):
        df = kwargs[table_name]
        df = dealToNull(df)
        df = dealScheme(df, dict_scheme)
        df = getInputVersion(df, table_name.replace('df_', ''))
        return df
       
    df_raw_data = readInFile('df_product_mapping_out')
    
    df_poi = readInFile('df_poi')
    
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
