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
    current_year = kwargs['current_year']
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
    # g_out_adding_data = 'adding_data'
    # g_out_new_hospital = 'new_hospital'
    # g_out_raw_data_adding_final = 'raw_data_adding_final'
    
    logger.debug('数据执行-start')
    if if_add_data != "False" and if_add_data != "True":
        logger.debug('wrong input: if_add_data, False or True') 
        raise ValueError('wrong input: if_add_data, False or True')
    
    if monthly_update != "False" and monthly_update != "True":
        logger.debug('wrong input: monthly_update, False or True') 
        raise ValueError('wrong input: monthly_update, False or True')
    
    model_month_right = int(model_month_right)
    
    # 月更新相关参数
    if monthly_update == "True":
        current_year = int(current_year)
    else:
        current_year = model_month_right//100

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

    df_cpa_pha_mapping = readInFile('df_cpa_pha_mapping')
    
    df_published = readInFile('df_published', dict_scheme={'year':'int'})
    
    if monthly_update == "True":       
        df_not_arrived = readInFile('df_not_arrived', dict_scheme={'date':'int'})

    # %% 
    # =========== 数据清洗 =============
    def dealScheme(df, dict_scheme):
        # 数据类型处理
        for i in dict_scheme.keys():
            df = df.withColumn(i, col(i).cast(dict_scheme[i]))
        return df
    def deal_ID_length(df):
        # ID不足7位的补足0到6位
        # 国药诚信医院编码长度是7位数字，cpa医院编码是6位数字，其他还有包含字母的ID
        df = df.withColumn("ID", df["ID"].cast(StringType()))
        # 去掉末尾的.0
        df = df.withColumn("ID", func.regexp_replace("ID", "\\.0", ""))
        df = df.withColumn("ID", func.when(func.length(df.ID) < 7, func.lpad(df.ID, 6, "0")).otherwise(df.ID))
        return df
    
    # 1、选择标准列
    df_raw_data = df_raw_data.drop('version', 'provider', 'owner')
    df_cpa_pha_mapping = df_cpa_pha_mapping.select('ID', 'PHA', '推荐版本').distinct()
    df_published = df_published.select('id', 'source', 'year').distinct()
    if monthly_update == "True":        
        df_not_arrived = df_not_arrived.select('id', 'date').distinct()
    
    # 2、ID列补位
    df_cpa_pha_mapping = deal_ID_length(df_cpa_pha_mapping)
    df_published = deal_ID_length(df_published)
    if monthly_update == "True":        
        df_not_arrived = deal_ID_length(df_not_arrived)
    

    # %%
    # =========== 函数定义 =============
    # 生成 original_range_raw（样本中已到的Year、Month、PHA的搭配）
    
    def getPublishAll(df_published, current_year):
        # CPA：2017到当前年的全量出版医院 
        d = list(map(lambda x: func.lit(col('Year')*100 + (x + 1)), range(12)))
        df_published_all = df_published.where(col("Year") <= current_year) \
                                        .where(col('Source') == 'CPA') \
                                        .withColumn("Dates", func.array(d)) \
                                        .withColumn("Date", func.explode(col("Dates")))
        df_published_all = df_published_all.select('ID', 'Date')
        return df_published_all
    
    def getNotArriveAfterModelYear(df_not_arrived, model_month_right, current_year):
         # 模型之后的未到名单( > model_year 且 <= current_year)
        model_year = model_month_right//100    
        df_not_arrived_all = df_not_arrived.where(((col("Date")/100).cast('int') > model_year) & ((col("Date")/100).cast('int') <= current_year)  )
        df_not_arrived_all = df_not_arrived_all.select(["ID", "Date"])
        return df_not_arrived_all
    
    def getOriginalRangeRaw(df_published_all, df_not_arrived_all):
        # 出版医院 减去 未到名单
        df_original_range_raw = df_published_all.join(df_not_arrived_all, on=['ID', 'Date'], how='left_anti')
        return df_original_range_raw
    
    def unionCPAandNotCPA(df_raw_data, df_original_range_raw):
        # 与 非CPA医院 合并    
        df_original_range_raw_noncpa = df_raw_data.where(col('Source') != 'CPA').select('ID', 'Date').distinct()    
        df_original_range_raw = df_original_range_raw.union(df_original_range_raw_noncpa.select(df_original_range_raw.columns))
        return df_original_range_raw
    
    def mapPHA(df_original_range_raw, df_cpa_pha_mapping):
        # 匹配 PHA
        df_cpa_pha_mapping = df_cpa_pha_mapping.where(col("推荐版本") == 1) \
                                            .select("ID", "PHA").distinct()

        df_original_range_raw = df_original_range_raw.join(df_cpa_pha_mapping, on='ID', how='left')
        df_original_range_raw = df_original_range_raw.where(~col('PHA').isNull()) \
                                                .withColumn('Year', func.substring(col('Date'), 0, 4)) \
                                                .withColumn('Month', func.substring(col('Date'), 5, 2).cast(IntegerType())) \
                                                .select('PHA', 'Year', 'Month').distinct()
        return df_original_range_raw
        
    # %%
    # =========== 数据执行 =============        
    if monthly_update == 'True':
        df_published_all = getPublishAll(df_published, current_year)
        df_not_arrived_all = getNotArriveAfterModelYear(df_not_arrived, model_month_right, current_year)
        # 出版医院 减去 未到名单
        df_original_range_raw = getOriginalRangeRaw(df_published_all, df_not_arrived_all)
    else:
        # 跑模型年的时候，不去除未到名单
        df_original_range_raw = getPublishAll(df_published, current_year)
    
    # 与 非CPA医院 合并
    df_original_range_raw = unionCPAandNotCPA(df_raw_data, df_original_range_raw)
    # 匹配 PHA
    df_original_range_raw = mapPHA(df_original_range_raw, df_cpa_pha_mapping)
     
    # %%
    # =========== 数据输出 =============   
    def lowerColumns(df):
        df = df.toDF(*[i.lower() for i in df.columns])
        return df
    
    df_original_range_raw = lowerColumns(df_original_range_raw)
    
    logger.debug('数据执行-Finish')
    
    return {'out_df':df_original_range_raw}

