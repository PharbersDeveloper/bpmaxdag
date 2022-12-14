# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL


def execute(**kwargs):
    logger = phs3logger(kwargs["run_id"], LOG_DEBUG_LEVEL)
    spark = kwargs['spark']
    
    ### input args ###
    if_two_source = kwargs['if_two_source']
    cut_time_left = kwargs['cut_time_left']
    if_union = kwargs['if_union']
    g_input_version = kwargs['g_input_version']
    ### input args ###
    
    ### output args ###
    ### output args ###

    from pyspark.sql import SparkSession, Window
    from pyspark.sql.types import StringType, IntegerType, DoubleType
    from pyspark.sql import functions as func
    import os
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col
    import time
    import re
    from phcli.ph_tools.addTable.addTableToGlue import AddTableToGlue
    
    # %%
    # project_name = 'Gilead'
    # outdir = '202101'
    # if_two_source = 'True'
    # cut_time_left = '202101'
    # cut_time_right = '202101'
    # test = 'True'
    # %%
    # 输入
    if if_two_source != "False" and if_two_source != "True":
        phlogger.error('wrong input: if_two_source, False or True') 
        raise ValueError('wrong input: if_two_source, False or True')
        
    if if_union == 'True':
        cut_time_left = int(cut_time_left)
    
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
        df = df.drop('traceId')
        return df
    
    raw_data_dedup = readInFile('df_dropdup_cross_source_raw')
       
    # 历史数据 raw_data
    history_raw_data = readInFile('df_max_raw_data')
            
    cpa_pha_map = readInFile('df_cpa_pha_mapping')
            
    
    # %%
    # =============  数据执行 ==============
    # ID 的长度统一
    def dealIDLength(df, colname='ID'):
        # ID不足7位的前面补0到6位
        # 国药诚信医院编码长度是7位数字，cpa医院编码是6位数字
        # 其他来源的ID 还有包含字母的, 所以要为字符型，不能转为 数值型
        df = df.withColumn(colname, col(colname).cast(StringType()))
        # 去掉末尾的.0
        df = df.withColumn(colname, func.regexp_replace(colname, "\\.0", ""))
        df = df.withColumn(colname, func.when(func.length(col(colname)) < 7, func.lpad(col(colname), 6, "0")).otherwise(col(colname)))
        return df
    
    cpa_pha_map = cpa_pha_map.where(cpa_pha_map["推荐版本"] == 1).select('ID', 'PHA')
    cpa_pha_map = dealIDLength(cpa_pha_map)
     
    # %%
    # ==========  4. 与历史数据合并  ==========  
    def union_raw_data(raw_data_dedup, history_raw_data, cpa_pha_map):
        # 1. 历史数据
        # history_raw_data = spark.read.parquet(history_raw_data_path)
        if 'Corp' not in history_raw_data.columns:
            history_raw_data = history_raw_data.withColumn('Corp', func.lit(''))
        if 'Route' not in history_raw_data.columns:
            history_raw_data = history_raw_data.withColumn('Route', func.lit(''))
        for colname, coltype in history_raw_data.dtypes:
            if coltype == "boolean":
                history_raw_data = history_raw_data.withColumn(colname, history_raw_data[colname].cast(StringType()))
                
        history_raw_data = history_raw_data.withColumn('Date', col('Date').cast(IntegerType()))
        history_raw_data = history_raw_data.where(col('Date') < cut_time_left)
        history_raw_data = history_raw_data.drop("Brand_new", "all_info")
        history_raw_data = dealIDLength(history_raw_data)
        # 匹配PHA
        history_raw_data = history_raw_data.join(cpa_pha_map, on='ID', how='left')
    
        # 2. 本期数据
        raw_data_dedup = raw_data_dedup.withColumn('Date', col('Date').cast(IntegerType()))
        # new_raw_data = raw_data_dedup.where((raw_data_dedup.Date >= cut_time_left) & (raw_data_dedup.Date <= cut_time_right))
        new_raw_data = dealIDLength(raw_data_dedup)
        # 匹配PHA
        new_raw_data = new_raw_data.join(cpa_pha_map, on='ID', how='left')
        # 本期 ID
        new_date_mole_id = new_raw_data.select('Date', 'Molecule', 'ID').distinct()
        # 本期 PHA
        new_date_mole_pha = new_raw_data.where(~col('PHA').isNull()).select('Date', 'Molecule', 'PHA').distinct()
        
        # 3. 去重
        history_raw_data = history_raw_data.join(new_date_mole_id, on=['Date', 'Molecule', 'ID'], how='left_anti') \
                                            .join(new_date_mole_pha, on=['Date', 'Molecule', 'PHA'], how='left_anti')
        # 4.合并数据
        all_raw_data = new_raw_data.union(history_raw_data.select(new_raw_data.columns)) \
                                    .drop('PHA')
        all_raw_data = dealIDLength(all_raw_data)
    
        #all_raw_data = all_raw_data.repartition(4)
        #all_raw_data.write.format("parquet") \
        #    .mode("overwrite").save(all_raw_data_path)
        
        return all_raw_data

    # %%
    # 与历史数据合并 
    if if_union == 'True':
        raw_data_dedup_all = union_raw_data(raw_data_dedup, history_raw_data, cpa_pha_map)             
    # 不与历史数据合并        
    else:
        raw_data_dedup_all = dealIDLength(raw_data_dedup)
 

    # %%
    # =========== 数据输出 =============
    def lowerColumns(df):
        df = df.toDF(*[i.lower() for i in df.columns])
        return df
    raw_data_dedup_all = lowerColumns(raw_data_dedup_all)
    return {"out_df":raw_data_dedup_all}

