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
    logger.debug('数据执行-start')
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
    
    raw_data = readInFile('df_raw_data_deal_poi')
    
    # %%
    # ==== 计算价格 ====
    logger.debug('价格计算')
    
    # 补数部分的数量需要用价格得出
    # 1、City_Tier 层面：
    price = raw_data.groupBy("min2", "date", "City_Tier_2010") \
        .agg((func.sum("Sales") / func.sum("Units")).alias("Price"))
    price2 = raw_data.groupBy("min2", "date") \
        .agg((func.sum("Sales") / func.sum("Units")).alias("Price2"))
    price = price.join(price2, on=["min2", "date"], how="left")
    price = price.withColumn("Price", func.when(func.isnull(col('Price')), col('Price2')).
                             otherwise(col('Price')))
    price = price.withColumn("Price", func.when(func.isnull(col('Price')), func.lit(0)).
                             otherwise(col('Price'))) \
        .drop("Price2")
    
    
    # %%
    # =========== 数据输出 =============
    def lowerColumns(df):
        df = df.toDF(*[i.lower() for i in df.columns])
        return df
    
    price = lowerColumns(price)
    
    logger.debug('数据执行-Finish')
    
    return {'out_df':price}
