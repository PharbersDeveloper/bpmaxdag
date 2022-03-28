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
    project_name = kwargs['project_name']
    model_month_left = kwargs['model_month_left']
    model_month_right = kwargs['model_month_right']
    current_year = kwargs['current_year']
    monthly_update = kwargs['monthly_update']
    add_47 = kwargs['add_47']
    g_input_version = kwargs['g_input_version']
    ### input args ###
    
    ### output args ###
    # g_out_panel_result = kwargs['g_out_panel_result']
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
    # =========== 数据执行 =========== 
    # 输入参数设置
    g_out_panel_result = 'panel_result'
    logger.debug('job4_panel')
    
    if add_47 != "False" and add_47 != "True":
        raise ValueError('wrong input: add_47, False or True')
        
    if monthly_update != "False" and monthly_update != "True":
        raise ValueError('wrong input: monthly_update, False or True')
    
    if monthly_update == "True":
        current_year = int(current_year)

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
    
    df_raw_data_adding_final = readInFile('df_raw_data_adding_final')
    
    df_mkt_mapping = readInFile('df_mkt_mapping') 
    
    df_universe = readInFile('df_universe_base')
    
    if monthly_update == "True":   
        df_published = readInFile('df_published')
    
        df_not_arrived = readInFile('df_not_arrived')
    else:
        df_new_hospital = readInFile('df_new_hospital')

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
    
    def deal_ID_length(df):
        # ID不足7位的补足0到6位
        # 国药诚信医院编码长度是7位数字，cpa医院编码是6位数字，其他还有包含字母的ID
        df = df.withColumn("ID", df["ID"].cast(StringType()))
        # 去掉末尾的.0
        df = df.withColumn("ID", func.regexp_replace("ID", "\\.0", ""))
        df = df.withColumn("ID", func.when(func.length(df.ID) < 7, func.lpad(df.ID, 6, "0")).otherwise(df.ID))
        return df
    
    # 1、列名清洗
    # 待清洗列名
    dict_cols_universe = {"City_Tier_2010":["City_Tier", "CITYGROUP", "City_Tier_2010"], "PHA":["Panel_ID", "PHA"]}
    #  执行
    df_universe = getTrueColRenamed(df_universe, dict_cols_universe, df_universe.columns)
    
    # 2、选择标准列
    df_raw_data_adding_final = df_raw_data_adding_final.drop('version', 'provider', 'owner')
    df_mkt_mapping = df_mkt_mapping.select('mkt', '标准通用名').distinct()
    df_universe = df_universe.select("PHA", "City", "Province", "HOSP_NAME").distinct()
    
    if monthly_update == "True":
        df_published = df_published.select('id', 'source', 'year').distinct()
        df_not_arrived = df_not_arrived.select('id', 'date').distinct()
        df_published = deal_ID_length(df_published)
        df_not_arrived = deal_ID_length(df_not_arrived)
    else:
        df_new_hospital = df_new_hospital.select('PHA').distinct()
    
    # 5、其他处理
    if df_universe.where(~col('HOSP_NAME').isNull()).count() == 0:
        df_universe = df_universe.withColumn("HOSP_NAME", func.lit("0"))
    
    df_mkt_mapping = df_mkt_mapping.withColumnRenamed("标准通用名", "通用名")

    # %%
    # =========== 数据执行 =============
    logger.debug('数据执行-start')
    
    # 生成 panel
    panel = df_raw_data_adding_final \
        .join(df_mkt_mapping, df_raw_data_adding_final["S_Molecule"] == df_mkt_mapping["通用名"], how="left") \
        .drop("Province", "City") \
        .join(df_universe, on="PHA", how="left")
    panel = panel \
        .groupBy("ID", "Date", "min2", "mkt", "HOSP_NAME", "PHA", "S_Molecule", "Province", "City", "add_flag") \
        .agg(func.sum("Sales").alias("Sales"), func.sum("Units").alias("Units"))
    
    # 对 panel 列名重新命名
    old_names = ["min2", "mkt", "S_Molecule"]
    new_names = ["Prod_Name", "DOI", "Molecule"]
    for index, name in enumerate(old_names):
        panel = panel.withColumnRenamed(name, new_names[index])
    
    # 拆分 panel_raw_data， panel_add_data
    panel_raw_data = panel.where(panel.add_flag == 0)
    panel_add_data = panel.where(panel.add_flag == 1)
    original_Date_molecule = panel_raw_data.select("Date", "Molecule").distinct()
    original_Date_ProdName = panel_raw_data.select("Date", "Prod_Name").distinct()
    panel_add_data = panel_add_data.join(original_Date_molecule, on=["Date", "Molecule"], how="inner") \
                                    .join(original_Date_ProdName, on=["Date", "Prod_Name"], how="inner")
    
      
    # 生成 panel_filtered
    # 早于model所用时间（历史数据），用new_hospital补数;
    # 处于model所用时间（模型数据），不补数；
    # 晚于model所用时间（月更新数据），用unpublished和not arrived补数
    # 取消Sanofi AZ 特殊处理（20210506）
    city_list = [u'北京市', u'上海市', u'天津市', u'重庆市', u'广州市', u'深圳市', u'西安市', u'大连市', u'成都市', u'厦门市', u'沈阳市']
    Province_list = [u'河北省', u'福建省', u'河北', u"福建"]
          
    # 去除 city_list和 Province_list
    if add_47 == "False":
        panel_add_data = panel_add_data \
            .where(~col('City').isin(city_list)) \
            .where(~col('Province').isin(Province_list))
    
    if monthly_update == "False":
        if project_name == u"贝达" or project_name == "Sanofi" or project_name == "AZ":
            panel_add_data = panel_add_data.where(panel_add_data.Molecule != u"奥希替尼")
        panel_add_data_history = panel_add_data.join(df_new_hospital, on='PHA', how='inner') \
                                            .where(col('Date') < int(model_month_left)) \
                                            .select(panel_raw_data.columns)
        panel_filtered = panel_raw_data.union(panel_add_data_history)
    
    if monthly_update == "True":
        # unpublished文件
        # unpublished 列表创建：published_left中有而published_right没有的ID列表，然后重复12次，时间为current_year*100 + i
        published_right = df_published.where(col('year') == current_year).select('ID').distinct()
        published_left = df_published.where(col('year') == current_year-1 ).select('ID').distinct()    
        unpublished = published_left.subtract(published_right)
        
        d = list(map(lambda x: func.lit(current_year*100 + (x + 1)), range(12)))
        unpublished = unpublished.withColumn("DATES", func.array(d)) \
                                        .withColumn("Date", func.explode(col("DATES"))) \
                                        .select("ID","Date").distinct()
        
        # 合并unpublished和not_arrive文件
        Notarrive_unpublished = unpublished.union(df_not_arrived).distinct()
        
        future_range = Notarrive_unpublished.withColumn("Date", col("Date").cast(DoubleType()))
        panel_add_data_future = panel_add_data.where(col("Date") > int(model_month_right)) \
                                            .join(future_range, on=["Date", "ID"], how="inner") \
                                            .select(panel_raw_data.columns)
        panel_filtered = panel_raw_data.union(panel_add_data_future)    


    # %%
    # ==== **** 输出补数结果 **** ====    
    def lowerColumns(df):
        df = df.toDF(*[i.lower() for i in df.columns])
        return df
    
    panel_filtered = lowerColumns(panel_filtered)
    
    logger.debug('数据执行-Finish')
    
    return {'out_df':panel_filtered}
