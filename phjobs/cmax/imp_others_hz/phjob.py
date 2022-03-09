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
    # df_raw_data_hz = readClickhouse('default', 'ftZnwL38MzTJPr1s_rawdata_hangzhou', '袁毓蔚_Auto_cMax_enlarge_Auto_cMax_enlarge_developer_2022-02-28T02:26:02+00:00')
    # df_out_sample_hz = readClickhouse('default', 'ftZnwL38MzTJPr1s_out_sample_hz', '袁毓蔚_Auto_cMax_enlarge_Auto_cMax_enlarge_developer_2022-02-28T02:26:02+00:00')
    
    df_raw_data_hz = readInFile(kwargs["df_rawdata_hangzhou"])
    df_out_sample_hz = readInFile(kwargs["df_out_sample_hz"])
    
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
