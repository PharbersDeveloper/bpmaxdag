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
    from functools import reduce    # %%
    # =========== 输入数据读取 ===========
    def dealToNull(df):
        df = df.replace(["None", ""], None)
        return df
    
    def dealScheme(df, dict_scheme):
        # 数据类型处理 {"col":"type"}
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
    df_pchc_universe = readInFile('df_pchc_universe')
    # %%
    # ==========  数据执行  ============
    def reName(df, dict_rename={}):
        df = reduce(lambda df, i_dict:df.withColumnRenamed(i_dict[0], i_dict[1]), zip(dict_rename.keys(), dict_rename.values()), df)
        return df
        
            
    df_pchc_mapping = reName(df_pchc_universe, 
                             dict_rename={'省':'province', '地级市':'city', '区[县_县级市]':'district', '新版PCHC_Code':'pchc'})
    
    df_pchc_mapping1 = df_pchc_mapping.where( (~col('pchc_name').isNull()) & (~col('pchc').isNull()) ) \
                                    .groupby('province', 'city', 'district', 'PCHC_Name').agg(func.first('pchc', ignorenulls=True).alias('pchc')) \
                                    .withColumnRenamed('PCHC_Name', 'hospital')
    
    df_pchc_mapping2 = df_pchc_mapping.where( (~col('招标样本名称').isNull()) & (~col('pchc').isNull()) ) \
                                    .groupby('province', 'city', 'district', '招标样本名称').agg(func.first('pchc', ignorenulls=True).alias('pchc')) \
                                    .withColumnRenamed('招标样本名称', 'hospital')
    
    df_pchc_mapping3 = df_pchc_mapping1.union(df_pchc_mapping2) \
                                        .withColumn('province', func.regexp_replace("province", "省|市", "")) \
                                        .withColumn('city', func.regexp_replace("city", "市", "")) \
                                        .distinct()

    # %%
    # =========== 数据输出 =============
    # 读回
    df_out = lowCol(df_pchc_mapping3)
    return {"out_df":df_out}
