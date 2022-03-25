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
    model_month_right = kwargs['model_month_right']
    monthly_update = kwargs['monthly_update']
    if_add_data = kwargs['if_add_data']
    g_input_version = kwargs['g_input_version']
    ### input args ###
    
    ### output args ###
    ### output args ###

    import pandas as pd
    import os
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col        
    import json
    import boto3    
    # %% 
    # =========== 数据执行 =========== 
    # 输入参数设置    
    logger.debug('数据执行-start')
    
    if if_add_data != "False" and if_add_data != "True":
        logger.debug('wrong input: if_add_data, False or True') 
        raise ValueError('wrong input: if_add_data, False or True')
    
    if monthly_update != "False" and monthly_update != "True":
        logger.debug('wrong input: monthly_update, False or True') 
        raise ValueError('wrong input: monthly_update, False or True')
    
    model_month_right = int(model_month_right)

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
    
    df_raw_data = readInFile('df_raw_data_deal_poi', dict_scheme={'date':'int', 'year':'int', 'month':'int'})
    
    df_original_range_raw = readInFile('df_original_range_raw', dict_scheme={'year':'int', 'month':'int'})
         
    # %%
    # =========== 函数定义 =============     
    def getNewHospital(df_original_range_raw, df_raw_data, model_month_right):
        df_raw_data = df_raw_data.where(col('Year') < ((model_month_right // 100) + 1))
        
        df_raw_data_for_add = df_raw_data.where(~col('PHA').isNull())

        years = df_raw_data_for_add.select("Year").distinct() \
                                .orderBy(col('Year')) \
                                .toPandas()["Year"].values.tolist()

        # 所有发表医院信息
        df_original_range = df_original_range_raw.where(col('Year').isin(years))

        # 进一步为最后一年独有的医院补最后一年的缺失月（可能也要考虑第一年）:
        years = df_original_range.select("Year").distinct() \
                                .orderBy(col('Year')) \
                                .toPandas()["Year"].values.tolist()

        # 只在最新一年出现的医院
        df_new_hospital = (df_original_range.where(col('Year') == max(years)).select("PHA").distinct()) \
                    .subtract(df_original_range.where(col('Year') != max(years)).select("PHA").distinct())
        return df_new_hospital
        
    def lowerColumns(df):
        df = df.toDF(*[i.lower() for i in df.columns])
        return df
    
    
    # %%
    # =========== 数据执行 =============
    if monthly_update == "False" and if_add_data == "True":
        df_new_hospital = getNewHospital(df_original_range_raw, df_raw_data, model_month_right)
        df_new_hospital = lowerColumns(df_new_hospital)
        logger.debug('数据执行-Finish')   
    else:
        # 创建空dataframe
        schema = StructType([StructField("pha", StringType(), True)])
        df_new_hospital = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
    
    return {'out_df':df_new_hospital}


    
    
    
    
    
    
    
    

