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
    if_two_source = kwargs['if_two_source']
    cut_time_left = kwargs['cut_time_left']
    if_union = kwargs['if_union']
    ### input args ###
    
    ### output args ###
    c = kwargs['c']
    d = kwargs['d']
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
    

    raw_data_dedup_std = kwargs['df_dropdup_cross_source_raw_std']
    raw_data_dedup_std = dealToNull(raw_data_dedup_std)
             
    cpa_pha_map = kwargs['df_cpa_pha_mapping_common']
    cpa_pha_map = dealToNull(cpa_pha_map)
    
    if if_two_source == 'True':
        history_raw_data_std = kwargs['df_history_raw_data_std']
        history_raw_data_std = dealToNull(history_raw_data_std)

            
    
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
        all_raw_data = new_raw_data.select(history_raw_data.columns).union(history_raw_data) \
                                    .drop('PHA')
        all_raw_data = dealIDLength(all_raw_data)
    
        #all_raw_data = all_raw_data.repartition(4)
        #all_raw_data.write.format("parquet") \
        #    .mode("overwrite").save(all_raw_data_path)
        
        return all_raw_data


    # %%
    # =========== 数据输出 =============
    def lowerColumns(df):
        df = df.toDF(*[i.lower() for i in df.columns])
        return df
    
    # 与历史数据合并
    if if_two_source == 'True':
        if if_union == 'True':
            # 用于max计算
            raw_data_dedup_std_all = union_raw_data(raw_data_dedup_std, history_raw_data_std, cpa_pha_map)
        # 不与历史数据合并        
        else:
            raw_data_dedup_std_all = dealIDLength(raw_data_dedup_std)
            
        raw_data_dedup_std_all = lowerColumns(raw_data_dedup_std_all)   
        return {"out_df":raw_data_dedup_std_all}
    else:
        return {}
