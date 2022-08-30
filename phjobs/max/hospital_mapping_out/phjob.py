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
    import boto3
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col    
    import json    
    

    # %% 
    # =========== 全局 参数检查 =========== 
    def checkArgs(kwargs):
        # True False 参数
        for i in ['monthly_update', 'if_two_source', 'if_add_data', 'if_base', 'hospital_level', 'bedsize', 'add_47']:
            arg = kwargs[i]
            if arg not in ["False", "True"]:
                raise ValueError(f"wrong parameter: {i}, False or True")    

        # 常规参数，存在即可
        for i in ['project_name', 'current_year', 'first_month', 'current_month', 'time_left', 'time_right', 'model_month_left', 'model_month_right', 'max_month', 'year_missing', 
                  'minimum_product_columns', 'minimum_product_sep', 'minimum_product_newname']:
            if kwargs[i] and kwargs[i] != "":
                continue
            else:
                raise ValueError(f"need parameter: {i}")

        # Empty的大小写判断，代码里会判断是否为Empty从而做一些操作
        for i in ['max_month', 'year_missing', 'universe_choice', 'use_d_weight']:
            if kwargs[i].lower() == 'empty' and kwargs[i] != 'Empty':
                raise ValueError(f"wrong parameter:  {i} shoule be 'Empty'")

        # 逗号分割
        for i in ['use_d_weight', 'all_models']:
            arg = kwargs[i]
            if '，' in arg:
                raise ValueError(f"wrong parameter: {i} should split by ,")

        # 冒号分割
        for i in ['factor_choice', 'universe_choice', 'universe_outlier_choice']:
            arg = kwargs[i]
            if '：' in arg or '，' in arg:
                raise ValueError(f"wrong parameter: {i} should use : and ,")

    def judgeVersion(table, versions, client):
        # version 是否以逗号分割
        if '，' in versions:
            raise ValueError(f"wrong g_input_version: {table} should split by ,") 
        list_version = versions.replace(' ','').split(',')

        # 判断version是否存在  
        outPartitions = client.get_partitions(DatabaseName="zudIcG_17yj8CEUoCTHg", TableName=table )
        outPartitionsList = [i['Values'][0] for i in outPartitions['Partitions']]
        for i in list_version:
            if i not in outPartitionsList:
                raise ValueError(f"wrong g_input_version: {table} 不存在该version {i}")

    # 版本
    def checkArgsVersion(kwargs, g_input_version):
        client = boto3.client('glue', 'cn-northwest-1')
        tables = ['prod_mapping', 'max_raw_data_delivery', 'max_raw_data', 'universe_base_common', 'universe_base', 'weight', 'province_city_mapping_common', 'province_city_mapping', 'cpa_pha_mapping_common', 'cpa_pha_mapping', 'id_bedsize', 'product_map_all_atc', 'master_data_map', 'mkt_mapping', 'poi', 'not_arrived', 'published', 'factor', 'universe_outlier']
        if kwargs['if_two_source'] == 'True':
            tables = tables + ['max_raw_data_std']
        if kwargs['use_d_weight'] != 'Empty':
            tables = tables + ['weight_default']
        if kwargs['universe_choice'] != 'Empty':
            tables = tables + ['universe_other']

        for i in tables:
            try:
                versions = g_input_version[i]
            except:
                raise ValueError(f"need g_input_version: {i} ")

            print(i,':',versions)
            judgeVersion(i, versions, client)

    def get_g_input_version(kwargs):
        # 把 universe_choice，factor_choice，universe_outlier_choice 中指定的版本信息添加到 g_input_version中
        def getVersionDict(str_choice):
            dict_choice = {}
            if str_choice != "Empty":
                for each in str_choice.replace(", ",",").split(","):
                    market_name = each.split(":")[0]
                    version_name = each.split(":")[1]
                    dict_choice[market_name]=version_name
            return dict_choice
        all_models = kwargs['all_models']
        g_input_version = kwargs['g_input_version']

        dict_universe_choice = getVersionDict(kwargs['universe_choice'])
        dict_factor = {k: v for k,v in getVersionDict(kwargs['factor_choice']).items() if k in all_models}  
        dict_universe_outlier = {k: v for k,v in getVersionDict(kwargs['universe_outlier_choice']).items() if k in all_models} 
        g_input_version['factor'] = ','.join(dict_factor.values())
        g_input_version['universe_other'] = ','.join(dict_universe_choice.values())
        g_input_version['universe_outlier'] = ','.join(dict_universe_outlier.values())

        return g_input_version

    checkArgs(kwargs)
    checkArgsVersion(kwargs, get_g_input_version(kwargs))   
    
    
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
    
    df_raw_data = readInFile('df_max_raw_data')
    
    df_universe = readInFile('df_universe_base')
    
    df_cpa_pha_mapping = readInFile('df_cpa_pha_mapping')

    # %% 
    # =========== 数据清洗 =============
    logger.debug('数据清洗-start')
    # 函数定义
    def getTrueCol(df, l_colnames, l_df_columns):
        # 检索出正确列名
        l_true_colname = []
        for i in l_colnames:
            if i.lower() in l_df_columns and df.where(~col(i).isNull()).count() > 0:
                l_true_colname.append(i)
        if len(l_true_colname) > 1:
           raise ValueError('有重复列名: %s' %(l_true_colname))
        if len(l_true_colname) == 0:
           raise ValueError('缺少列信息: %s' %(l_colnames)) 
        return l_true_colname[0]  
    
    def getTrueColRenamed(df, dict_cols, l_df_columns):
        # 对列名重命名
        for i in dict_cols.keys():
            true_colname = getTrueCol(df, dict_cols[i], l_df_columns)
            logger.debug(true_colname)
            if true_colname != i:
                if i in l_df_columns:
                    # 删除原表中已有的重复列名
                    df = df.drop(i)
                df = df.withColumnRenamed(true_colname, i)
        return df
    
    def dealScheme(df, dict_scheme):
        # 数据类型处理
        for i in dict_scheme.keys():
            df = df.withColumn(i, col(i).cast(dict_scheme[i]))
        return df
    
    def dealIDLength(df, colname='ID'):
        # ID不足7位的前面补0到6位
        # 国药诚信医院编码长度是7位数字，cpa医院编码是6位数字
        # 其他来源的ID 还有包含字母的, 所以要为字符型，不能转为 数值型
        df = df.withColumn(colname, col(colname).cast(StringType()))
        # 去掉末尾的.0
        df = df.withColumn(colname, func.regexp_replace(colname, "\\.0", ""))
        df = df.withColumn(colname, func.when(func.length(col(colname)) < 7, func.lpad(col(colname), 6, "0")).otherwise(col(colname)))
        return df
    
    # 1、列名清洗
    # 待清洗列名
    dict_cols_universe = {"City_Tier_2010":["City_Tier", "CITYGROUP", "City_Tier_2010"], "PHA":["Panel_ID", "PHA"]}
    
    #  执行
    df_universe = getTrueColRenamed(df_universe, dict_cols_universe, df_universe.columns)
    
    # 2、选择标准列
    # 标准列名
    std_cols_cpa_pha_mapping = ['ID', 'PHA', '推荐版本']
    std_cols_raw_data = ['Date', 'ID', 'Raw_Hosp_Name', 'Brand', 'Form', 'Specifications', 'Pack_Number', 'Manufacturer', 'Molecule', 'Source', 'Corp', 
                     'Route', 'ORG_Measure', 'Sales', 'Units', 'Units_Box', 'Path', 'Sheet']
    std_cols_universe = ["PHA", "City", "City_Tier_2010", "Province"]
    
    #  执行
    df_universe = df_universe.select(std_cols_universe)
    df_raw_data = df_raw_data.select(std_cols_raw_data)
    df_cpa_pha_mapping = df_cpa_pha_mapping.select(std_cols_cpa_pha_mapping)
    
    # 3、数据类型处理
    dict_scheme_raw_data = {"Date":"int", "Sales":"double", "Units":"double", "Units_Box":"double", "Pack_Number":"int"}
        
    df_raw_data = dealScheme(df_raw_data, dict_scheme_raw_data)
    
    # 4、ID列补位
    df_cpa_pha_mapping = dealIDLength(df_cpa_pha_mapping)
    df_raw_data = dealIDLength(df_raw_data)
    
    # 5、其他处理
    if df_raw_data.where(~col('Pack_Number').isNull()).count() == 0:
        df_raw_data = df_raw_data.withColumn("Pack_Number", func.lit(0))

    # %%
    # =========== 数据执行 =============
    logger.debug('数据执行-start')
    
    # 1.2 读取CPA与PHA的匹配关系:
    df_cpa_pha_mapping = df_cpa_pha_mapping.where(col("推荐版本") == 1) \
                                        .select("ID", "PHA").distinct()
    # 1.3 读取原始样本数据:    
    df_raw_data = df_raw_data.join(df_cpa_pha_mapping, on='ID', how="left") \
                        .join(df_universe.select("PHA", "City", "Province").distinct(), on='PHA', how='left') \
                        .join(df_universe.select("PHA", "City", "City_Tier_2010").distinct(), on=["PHA", "City"], how="left")
    
    df_raw_data = df_raw_data.withColumn("Month", (col('Date') % 100).cast(IntegerType())) \
                            .withColumn("Year", ((col('Date') - col('Month')) / 100).cast(IntegerType())) \
                            .withColumn("City_Tier_2010", col('City_Tier_2010').cast(IntegerType()))

    # %%
    # =========== 数据输出 =============
    def lowerColumns(df):
        df = df.toDF(*[i.lower() for i in df.columns])
        return df

    df_raw_data = lowerColumns(df_raw_data)

    logger.debug('数据执行-Finish')
    
    return {'out_df':df_raw_data}

