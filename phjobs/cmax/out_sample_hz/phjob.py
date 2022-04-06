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
    df_raw_data_hz = readInFile("df_rawdata_hangzhou")
    # %%
    # =========== 函数定义 =============
    def unpivot(df, keys):
        # 功能：数据宽变长
        # 参数说明 df:dataframe,  keys 待转换表中需要保留的主键key，以list[]类型传入
        # 转换是为了避免字段类不匹配，统一将数据转换为string类型，如果保证数据类型完全一致，可以省略该句
        df = df.select(*[col(_).astype("string") for _ in df.columns])
        cols = [_ for _ in df.columns if _ not in keys]
        stack_str = ','.join(map(lambda x: "'%s', `%s`" % (x, x), cols))
        # feature, value 转换后的列名，可自定义
        df = df.selectExpr(*keys, "stack(%s, %s) as (feature, value)" % (len(cols), stack_str))
        return df
    
    
    def getOutSample(df_raw_data_hz):
        df_imp_hz_1 = df_raw_data_hz.withColumn('flag', func.lit(0))
    
        df_sample1_list = df_imp_hz_1.distinct() \
                                .groupby('pchc', 'quarter', 'city') \
                                    .agg( func.size(func.collect_set('packid')).alias('n') ) \
                                .groupBy('pchc', 'city') \
                                    .pivot("quarter").agg(func.sum('n')) \
                                .fillna(0) \
                                .withColumnRenamed('feature', 'quarter') 
        df_sample1_list = unpivot(df_sample1_list, keys=['pchc', 'city']) \
                                .withColumn('value', func.when(col('value') > 0, func.lit(1)).otherwise(col('value')))  \
                                .groupby('pchc', 'city') \
                                      .agg( func.sum('value').alias('value'), func.count('pchc').alias('count')) \
                                .where(col('value') == col('count')) \
                                .select('pchc').distinct()
        
        df_full_list = df_imp_hz_1.select('pchc').distinct()
        
        df_out_sample = df_full_list.subtract(df_sample1_list)
        
        return df_out_sample
    # %%
    # =========== 数据执行 =============
    df_out_sample = getOutSample(df_raw_data_hz)
    # %%
    # =========== 数据输出 =============
    df_out = lowCol(df_out_sample)
    return {"out_df":df_out}
