# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL


def execute(**kwargs):
    logger = phs3logger(kwargs["run_id"], LOG_DEBUG_LEVEL)
    spark = kwargs['spark']
    
    ### input args ###
    # g_hz_city = "宁波,杭州,温州,金华,绍兴"
    g_hz_city = kwargs["g_hz_city"]
    g_input_version = kwargs['g_input_version']
    ### input args ###
    
    ### output args ###
    ### output args ###

    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    import os
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col
    import time
    import re
    import pandas as pd
    import numpy as np
    import json
    from functools import reduce
    from pyspark.sql import Window    # %%
    # =========== 输入数据读取 ===========
    def dealToNull(df):
        df = df.replace(["None", ""], None)
        return df
    
    def dealScheme(df, dict_scheme):
        # 数据类型处理
        if dict_scheme == {}:
            for i in dict_scheme.keys():
                df = df.withColumn(i, col(i).cast(dict_scheme[i]))
        return df
    
    def lowCol(df):
        df = df.toDF(*[c.lower() for c in df.columns])
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
        df = lowCol(df)
        df = dealScheme(df, dict_scheme)
        df = getInputVersion(df, table_name.replace('df_', ''))
        return df
    

    # %% 
    # =========== 输入数据读取 =========== 
    df_project_price = readInFile("df_project_price")
    df_project_price_hz = readInFile("df_project_price_hz")

    # %%
    # =========== 数据执行 =============
    def unionDf(df1, df2, utype='same'):
        if utype=='same':
            all_cols =  list(set(df1.columns).intersection(set(df2.columns)) - set(['version']))
        elif utype=='all':
            all_cols =  list(set(set(df1.columns + df2.columns) - set(['version'])))     
            for i in all_cols:
                if i not in df1.columns:
                    df1 = df1.withColumn(i, func.lit(None))
                if i not in df2.columns:
                    df2 = df2.withColumn(i, func.lit(None))            
        df_all = df1.select(all_cols).union(df2.select(all_cols)) 
        return df_all
    
    df_project_result = df_project_price.where(~col('city').isin(g_hz_city.replace(' ','').split(',')))
    df_project_result_hz = df_project_price_hz.where(col('city').isin(g_hz_city.replace(' ','').split(',')))
    
    df_project_result_all = unionDf(df_project_result, df_project_result_hz, utype='same')
    # %%
    # =========== 数据输出 =============
    df_out = lowCol(df_project_result_all)
    return {"out_df":df_out}
