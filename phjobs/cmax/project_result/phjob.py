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
    # g_hz_city = "宁波,杭州,温州,金华,绍兴"
    g_hz_city = kwargs["g_hz_city"]
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
    
    def readInFile(df, dict_scheme={}):
        df = dealToNull(df)
        df = lowCol(df)
        df = dealScheme(df, dict_scheme)
        return df
    
    
    def readClickhouse(database, dbtable, version):
        df = spark.read.format("jdbc") \
                .option("url", "jdbc:clickhouse://192.168.16.117:8123/" + database) \
                .option("dbtable", dbtable) \
                .option("driver", "ru.yandex.clickhouse.ClickHouseDriver") \
                .option("user", "default") \
                .option("password", "") \
                .option("batchsize", 1000) \
                .option("socket_timeout", 300000) \
                .option("rewrtieBatchedStatements", True).load()
        if version != 'all':
            version = version.replace(" ","").split(',')
            df = df.where(df['version'].isin(version))
        return df

    # %% 
    # =========== 输入数据读取 =========== 
    # df_project_price = readClickhouse('default', 'ftZnwL38MzTJPr1s_project_price', '袁毓蔚_Auto_cMax_enlarge_Auto_cMax_enlarge_developer_2022-03-02T04:29:04.515849+00:00')
    # df_project_price_hz = readClickhouse('default', 'ftZnwL38MzTJPr1s_project_price_hz', '袁毓蔚_Auto_cMax_enlarge_Auto_cMax_enlarge_developer_2022-03-02T04:29:04.515849+00:00')
    
    df_project_price = readInFile(kwargs["df_project_price"])
    df_project_price_hz = readInFile(kwargs["df_project_price_hz"])

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
