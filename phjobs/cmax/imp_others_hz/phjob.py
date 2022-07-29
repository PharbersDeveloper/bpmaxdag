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
    from pyspark.sql import Window
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
    df_raw_data_hz = readInFile("df_rawdata_hangzhou")
    df_out_sample_hz = readInFile("df_out_sample_hz")
    
    # %% 
    # =========== 函数定义 =============
    def getImpOthers(df_raw_data_hz, df_out_sample_hz):
        df_imp_others = df_raw_data_hz.withColumn('flag_sample', func.lit(1)) \
                                .join(df_out_sample_hz, on='pchc', how='inner') \
                                .orderBy('province', 'city', 'district', 'pchc', 'market', 'packid', 'date') \
                                .groupby('date', 'province', 'city', 'district', 'market', 'packid', 'flag_sample') \
                                    .agg(func.sum('sales').alias('sales'), func.sum('units').alias('units')) \
                                .where(col('units') > 0.0).where(col('sales') > 0.0)
        return df_imp_others
                
    # %%
    # =========== 数据执行 =============
    df_imp_others_hz = getImpOthers(df_raw_data_hz, df_out_sample_hz)
    # %%
    # =========== 数据输出 =============
    df_out = lowCol(df_imp_others_hz)
    return {"out_df":df_out}
