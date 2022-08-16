# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL


def execute(**kwargs):
    logger = phs3logger(kwargs["run_id"], LOG_DEBUG_LEVEL)
    spark = kwargs['spark']
    
    ### input args ###
    project_name = kwargs['project_name']
    if_base = kwargs['if_base']
    time_left = kwargs['time_left']
    time_right = kwargs['time_right']
    all_models = kwargs['all_models']
    universe_choice = kwargs['universe_choice']
    factor_choice = kwargs['factor_choice']
    universe_outlier_choice = kwargs['universe_outlier_choice']
    use_d_weight = kwargs['use_d_weight']
    run_id = kwargs['run_id'].replace(":","_")
    owner = kwargs['owner']
    g_input_version = kwargs['g_input_version']
    ### input args ###
    
    ### output args ###
    ### output args ###  
    
    import os
    import pandas as pd
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    from pyspark.sql.functions import col     
    import json
    import boto3
    from functools import reduce

    # %% 
    # =========== 参数处理 =========== 
    logger.debug('job5_max')

    if if_base == "False":
        if_base = False
    elif if_base == "True":
        if_base = True
    else:
        raise ValueError('if_base: False or True')

    if all_models != "Empty":
        all_models = all_models.replace(", ",",").split(",")
    else:
        all_models = []

    if use_d_weight != "Empty":
        use_d_weight = use_d_weight.replace(" ","").split(",")
    else:
        use_d_weight = []

    time_left = int(time_left)
    time_right = int(time_right)

    # 市场的universe文件
    def getVersionDict(str_choice):
        dict_choice = {}
        if str_choice != "Empty":
            for each in str_choice.replace(", ",",").split(","):
                market_name = each.split(":")[0]
                version_name = each.split(":")[1]
                dict_choice[market_name]=version_name
        return dict_choice

    dict_universe_choice = getVersionDict(universe_choice)
    dict_factor = {k: v for k,v in getVersionDict(factor_choice).items() if k in all_models}  
    dict_universe_outlier = {k: v for k,v in getVersionDict(universe_outlier_choice).items() if k in all_models} 
    
    g_input_version['factor'] = ','.join(dict_factor.values())
    g_input_version['universe_other'] = ','.join(dict_universe_choice.values())
    g_input_version['universe_outlier'] = ','.join(dict_universe_outlier.values())
    

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

    df_panel_result = readInFile('df_panel_result').drop('version', 'provider', 'owner').distinct()

    df_PHA_weight = readInFile('df_weight')
    if use_d_weight:
        df_PHA_weight_default = readInFile('df_weight_default')
    else:
        df_PHA_weight_default = 'Empty'

    df_universe_outlier = readInFile('df_universe_outlier')
    df_universe_base = readInFile('df_universe_base')
    if universe_choice != 'Empty':
        df_universe_other = readInFile('df_universe_other')
    else:
        df_universe_other = 'Empty'

    if if_base:
        df_factor_base = readInFile('df_factor_base')
        df_factor_all = 'Empty'
    else:
        df_factor_all = readInFile('df_factor')
        df_factor_base = 'Empty'

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
            #logger.debug(true_colname)
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

    # 1、清洗
    def cleanUniverse(df_universe):
        dict_cols_universe = {"City_Tier_2010":["City_Tier", "CITYGROUP", "City_Tier_2010"], "PHA":["Panel_ID", "PHA"]}
        df_universe = getTrueColRenamed(df_universe, dict_cols_universe, df_universe.columns)
        df_universe = df_universe.select("PHA", "City", "Province", "City_Tier_2010", "HOSP_NAME", "PANEL", "BEDSIZE", "Seg", "Est_DrugIncome_RMB").distinct() \
                                    .withColumn('Seg', col('Seg').cast('int').cast('string'))
        return df_universe
    def cleanFactor(df_factor):
        dict_factor_universe = {"factor":["factor_new", "factor"]}
        df_factor = getTrueColRenamed(df_factor, dict_factor_universe, df_factor.columns)
        df_factor = df_factor.select("factor", "City", "Province", "doi").distinct()
        return df_factor

    # %%
    # =========== 数据准备 =============
    def getPhaWeight(use_d_weight, df_PHA_weight, df_PHA_weight_default):
        # 医院权重文件 
        # 是否加上 weight_default
        if use_d_weight:
            df_PHA_weight_default = df_PHA_weight_default.select('province', 'city', 'doi', 'weight', 'pha').distinct() \
                                        .where(col('doi').isin(use_d_weight)) \
                                        .withColumnRenamed('weight', 'weight_d')
            df_PHA_weight = df_PHA_weight.join(df_PHA_weight_default, on=['province', 'city', 'doi', 'pha'], how='full')
            df_PHA_weight = df_PHA_weight.withColumn('weight', func.when(col('weight').isNull(), col('weight_d')).otherwise(col('weight')))
        df_PHA_weight = df_PHA_weight.select('province', 'city', 'doi', 'weight', 'pha').distinct() \
                                        .withColumnRenamed('province', 'province_w') \
                                        .withColumnRenamed('city', 'city_w')
        return df_PHA_weight


    def getUniverse(all_models, dict_universe_choice, df_universe_base, df_universe_other):
        # universe 读取
        dict_universe = {}
        l_models_use_universe_base = [i for i in all_models if i not in dict_universe_choice.keys()]
        if len(l_models_use_universe_base) > 0:
            df_universe_base_doi = reduce(lambda df1,df2:df1.union(df2), 
                   list(map(lambda i: cleanUniverse(df_universe_base).withColumn('doi', func.lit(i)), l_models_use_universe_base))
                              )
            dict_universe['df_universe_base_doi'] = df_universe_base_doi
        if universe_choice != 'Empty':
            df_universe_other_doi = reduce(lambda df1,df2:df1.union(df2), 
                   list(map(lambda k,v: cleanUniverse(df_universe_other.where(col('version')==v)).withColumn('doi', func.lit(k)), dict_universe_choice.keys(), dict_universe_choice.values()))
                              )
            dict_universe['df_universe_other_doi'] = df_universe_other_doi

        df_universe = reduce(lambda df1,df2:df1.union(df2), dict_universe.values())
        df_universe = df_universe.withColumn("City_Tier_2010", col("City_Tier_2010").cast(StringType()))
        return df_universe

    def getUniverseOutlier(df_universe_outlier, dict_universe_outlier):
        # universe_outlier 读取
        df_universe_outlier_doi = reduce(lambda df1,df2:df1.union(df2), 
               list(map(lambda k,v: cleanUniverse(df_universe_outlier.where(col('version')==v)).withColumn('doi', func.lit(k)), dict_universe_outlier.keys(),  dict_universe_outlier.values()))
              )
        df_universe_outlier_doi = df_universe_outlier_doi.withColumn("City_Tier_2010", col("City_Tier_2010").cast(StringType()))
        df_universe_outlier_doi = df_universe_outlier_doi.select("PHA", "Est_DrugIncome_RMB", "PANEL", "Seg", "BEDSIZE", 'doi')    
        return df_universe_outlier_doi

    def getFactor(df_factor_all, df_factor_base, all_models, dict_factor):
        # 兼容旧factor无doi列，和新流程factor有doi列
        def getMarketFacor(df_factor_all, v, k): 
            df_v = df_factor_all.where( col('version')==v )
            if df_v.where(col('doi')==k ).count() > 0:
                df = df_v.where( col('doi')==k )
            elif df_v.where(col('doi')==k ).count() == 0:
                df = df_v.withColumn('doi', func.lit(k))
            return df    

        # factor 读取
        if if_base == True:
            df_factor_doi = reduce(lambda df1,df2:df1.union(df2), 
                   list(map(lambda i: cleanFactor(df_factor_base).withColumn('doi', func.lit(i)), all_models))
                              )
        else:
            df_factor_doi = reduce(lambda df1,df2:df1.union(df2), 
                   list(map(lambda k,v: cleanFactor(getMarketFacor(df_factor_all, v, k)), dict_factor.keys(), dict_factor.values()))
                              )
        return df_factor_doi

    # =========== 放大分析 =============
    def getPanelData(df_panel_result, all_models, time_left, time_right, df_universe, df_universe_outlier_doi, df_PHA_weight):
        # panel 文件 
        df_original_panel = df_panel_result.drop('version', 'provider', 'owner').distinct() \
                                        .where(col('DOI').isin(all_models)).where((col('Date') >= time_left) & (col('Date') <= time_right))

        # 1. 样本数据
        # panel：整理成 max的格式，包含了所有在universe 的panel列标记为1的医院，当作所有样本医院的 max
        df_universe_panel_all = df_universe.where(col('PANEL') == 1).select('PHA', 'BEDSIZE', 'PANEL', 'Seg', 'doi')
        df_panel = df_original_panel.join(df_universe_panel_all, on=['PHA', 'doi'], how="inner") \
                                .groupBy('PHA', 'Province', 'City', 'Date', 'Molecule', 'Prod_Name', 'BEDSIZE', 'PANEL', 'Seg', 'doi') \
                                .agg(func.sum("Sales").alias("Predict_Sales"), func.sum("Units").alias("Predict_Unit")).persist()

        # panel_seg：整理成seg层面，包含了所有在universe_ot 的panel列标记为1的医院，可以用来得到非样本医院的 max
        df_panel_drugincome = df_universe_outlier_doi.where(col('PANEL') == 1) \
                                            .groupBy("Seg", 'doi') \
                                            .agg(func.sum("Est_DrugIncome_RMB").alias("DrugIncome_Panel")).persist()
        df_original_panel_tmp = df_original_panel.join(df_universe_outlier_doi, on=['PHA', 'doi'], how='left').persist()
        df_panel_seg = df_original_panel_tmp.where(col('PANEL') == 1) \
                                        .groupBy('Date', 'Prod_Name', 'Seg', 'Molecule', 'doi') \
                                        .agg(func.sum("Sales").alias("Sales_Panel"), func.sum("Units").alias("Units_Panel"))
        df_panel_seg = df_panel_seg.join(df_panel_drugincome, on=['Seg', 'doi'], how="left").cache()

        # *** PHA_city 权重计算
        df_original_panel_weight = df_original_panel_tmp.join(df_PHA_weight, on=['PHA', 'doi'], how='left')
        df_original_panel_weight = df_original_panel_weight.withColumn('Weight', func.when(col('Weight').isNull(), func.lit(1)) \
                                                                                .otherwise(col('Weight')))
        df_original_panel_weight = df_original_panel_weight.withColumn('Sales_w', col('Sales') * col('Weight')) \
                                                    .withColumn('Units_w', col('Units') * col('Weight'))
        df_panel_seg_weight = df_original_panel_weight.where(col('PANEL') == 1) \
                                        .groupBy('Date', 'Prod_Name', 'Seg', 'Molecule', 'Province_w', 'City_w', 'doi') \
                                        .agg(func.sum("Sales_w").alias("Sales_Panel_w"), func.sum("Units_w").alias("Units_Panel_w")).persist()
        df_panel_seg_weight = df_panel_seg_weight.join(df_panel_drugincome, on=['Seg', 'doi'], how="left").persist()
        df_panel_seg_weight = df_panel_seg_weight.withColumnRenamed('Province_w', 'Province') \
                                            .withColumnRenamed('City_w', 'City')
        return {"df_panel_seg_weight":df_panel_seg_weight, "df_panel_seg":df_panel_seg, "df_panel":df_panel}

    def getNonePanelData(df_universe, df_factor_doi, all_models):
        # 2. 非样本数据
        # 将非样本的segment和factor等信息合并起来：
        df_universe_factor_panel = reduce(lambda df1,df2:df1.union(df2), 
                             list(map(lambda i:df_universe.where(col('doi')==i).join(df_factor_doi.where(col('doi')==i).select('City', 'factor', 'doi').distinct(), on=["City", 'doi'], how="left") if df_factor_doi.where(col('doi')==i).where(~col('Province').isNull()).count() == 0 else df_universe.where(col('doi')==i).join(df_factor_doi.select('City', 'factor', 'Province', 'doi').distinct(), on=["City", 'Province', 'doi'], how="left"), all_models))  
                               )
        df_universe_factor_panel = df_universe_factor_panel.withColumn("factor", func.when(func.isnull(col('factor')), func.lit(1)).otherwise(col('factor'))) \
                                            .where(col('PANEL') == 0) \
                                            .select('Province', 'City', 'PHA', 'Est_DrugIncome_RMB', 'Seg', 'BEDSIZE', 'PANEL', 'factor', 'doi')
        return df_universe_factor_panel

    def getMaxResult(df_universe_factor_panel, df_panel_seg, df_panel_seg_weight, df_panel):
        # 为这些非样本医院匹配上样本金额、产品、年月、所在segment的drugincome之和
        # 优先有权重的结果
        df_max_result = df_universe_factor_panel.join(df_panel_seg, on=['Seg', 'doi'], how="left")
        df_max_result = df_max_result.join(df_panel_seg_weight.select('Date', 'Prod_Name', 'Molecule', 'Seg', 'Province', 'City', 'Sales_Panel_w', 'Units_Panel_w', 'doi').distinct(), 
                                        on=['Date', 'Prod_Name', 'Molecule', 'Seg', 'Province', 'City', 'doi'], how="left").cache()
        df_max_result = df_max_result.withColumn('Sales_Panel', func.when(col('Sales_Panel_w').isNull(), col('Sales_Panel')) \
                                                                .otherwise(col('Sales_Panel_w'))) \
                                .withColumn('Units_Panel', func.when(col('Units_Panel_w').isNull(), col('Units_Panel')) \
                                                                .otherwise(col('Units_Panel_w'))) \
                                .drop('Sales_Panel_w', 'Units_Panel_w')
        # 预测值等于样本金额乘上当前医院drugincome再除以所在segment的drugincome之和
        df_max_result = df_max_result.withColumn("Predict_Sales", (col('Sales_Panel') / col('DrugIncome_Panel')) * col('Est_DrugIncome_RMB')) \
                                    .withColumn("Predict_Unit", (col('Units_Panel') / col('DrugIncome_Panel')) * col('Est_DrugIncome_RMB'))
        # 为什么有空，因为部分segment无样本或者样本金额为0：
        df_max_result = df_max_result.where(~func.isnull(col('Predict_Sales')))
        df_max_result = df_max_result.withColumn("positive", func.when(col("Predict_Sales") > 0, 1).otherwise(0))
        df_max_result = df_max_result.withColumn("positive", func.when(col("Predict_Unit") > 0, 1).otherwise(col('positive')))
        df_max_result = df_max_result.where(col('positive') == 1).drop("positive")
        # 乘上factor
        df_max_result = df_max_result.withColumn("Predict_Sales", col("Predict_Sales") * col('factor')) \
                                .withColumn("Predict_Unit", col('Predict_Unit') *col('factor')) \
                                .select('PHA', 'Province', 'City', 'Date', 'Molecule', 'Prod_Name', 'BEDSIZE', 'PANEL',
                                        'Seg', 'Predict_Sales', 'Predict_Unit', 'doi')
        # 3. 合并样本部分
        df_max_result = df_max_result.union(df_panel.select(df_max_result.columns))
        df_max_result = dealScheme(df_max_result, {"PHA":"string", "Province":"string", "City":"string", "Date":"double", "Molecule":"string", "Prod_Name":"string", "BEDSIZE":"string", "PANEL":"string", "Seg":"string", "Predict_Sales":"double", "Predict_Unit":"double", "DOI":"string"})
        return df_max_result

    # =========== 执行 =============
    # 文件处理
    df_PHA_weight_final = getPhaWeight(use_d_weight, df_PHA_weight, df_PHA_weight_default)
    df_universe = getUniverse(all_models, dict_universe_choice, df_universe_base, df_universe_other)
    df_universe_outlier_doi = getUniverseOutlier(df_universe_outlier, dict_universe_outlier)
    df_factor_doi = getFactor(df_factor_all, df_factor_base, all_models, dict_factor)

    # 放大分析
    dict_panel_result = getPanelData(df_panel_result, all_models, time_left, time_right, df_universe, df_universe_outlier_doi, df_PHA_weight_final)
    df_panel_seg_weight = dict_panel_result['df_panel_seg_weight']
    df_panel_seg = dict_panel_result['df_panel_seg']
    df_panel = dict_panel_result['df_panel']
    df_universe_factor_panel = getNonePanelData(df_universe, df_factor_doi, all_models)
    df_max_result_raw = getMaxResult(df_universe_factor_panel, df_panel_seg, df_panel_seg_weight, df_panel)
    
    
    # =========== 数据输出 =============
    def lowerColumns(df):
        df = df.toDF(*[i.lower() for i in df.columns])
        return df
    
    df_max_result_raw = lowerColumns(df_max_result_raw)
    
    logger.debug('数据执行-Finish')
    
    return {'out_df':df_max_result_raw}
