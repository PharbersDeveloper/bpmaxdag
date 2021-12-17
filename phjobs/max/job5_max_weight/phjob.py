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
    if_base = kwargs['if_base']
    time_left = kwargs['time_left']
    time_right = kwargs['time_right']
    all_models = kwargs['all_models']
    universe_choice = kwargs['universe_choice']
    use_d_weight = kwargs['use_d_weight']
    if_others = kwargs['if_others']
    out_path = kwargs['out_path']
    run_id = kwargs['run_id']
    owner = kwargs['owner']
    g_database_temp = kwargs['g_database_temp']
    g_database_input = kwargs['g_database_input']
    ### input args ###
    
    ### output args ###
    g_out_max = kwargs['g_out_max']
    ### output args ###

    
    
    
    
    import os
    import pandas as pd
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    from pyspark.sql.functions import col     
    import json
    import boto3    # %%
    # project_name = 'Empty'
    # if_base = 'False'
    # time_left = 0
    # time_right = 0
    # left_models = 'Empty'
    # left_models_time_left = 'Empty'
    # right_models = 'Empty'
    # right_models_time_right = 'Empty'
    # all_models = 'Empty'
    # universe_choice = 'Empty'
    # if_others = 'False'
    # use_d_weight = 'Empty'
    # %% 
    # 输入参数设置
    g_out_max = 'max_result_raw'
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
        
        
    # dict_input_version = json.loads(g_input_version)
    # print(dict_input_version)
    
    # 市场的universe文件
    universe_choice_dict={}
    if universe_choice != "Empty":
        for each in universe_choice.replace(", ",",").split(","):
            market_name = each.split(":")[0]
            universe_name = each.split(":")[1]
            universe_choice_dict[market_name]=universe_name
            
    # 输出
    p_out_max = out_path + g_out_max

    # %% 
    # 输入数据读取
    df_panel_result = kwargs['df_panel_result']
    
    df_PHA_weight =  kwargs['df_weight']
    
    if use_d_weight:
        df_PHA_weight_default =  kwargs['df_weight_default']

    # %% 
    # =========== 数据清洗 =============
    logger.debug('数据清洗-start')
    
    # 函数定义
    def getTrueCol(df, l_colnames, l_df_columns):
        # 检索出正确列名
        l_true_colname = []
        for i in l_colnames:
            if i.lower() in l_df_columns and df.where( (~col(i).isNull()) & (col(i) != 'None') ).count() > 0:
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
        
    # 1、清洗
    def cleanUniverse(df_universe):
        dict_cols_universe = {"City_Tier_2010":["City_Tier", "CITYGROUP", "City_Tier_2010"], "PHA":["Panel_ID", "PHA"]}
        df_universe = getTrueColRenamed(df_universe, dict_cols_universe, df_universe.columns)
        df_universe = df_universe.select("PHA", "City", "Province", "City_Tier_2010", "HOSP_NAME", "PANEL", "BEDSIZE", "Seg", "Est_DrugIncome_RMB").distinct()
        return df_universe
    def cleanFactor(df_factor):
        dict_factor_universe = {"factor":["factor_new", "factor"]}
        df_factor = getTrueColRenamed(df_factor, dict_factor_universe, df_factor.columns)
        df_factor = df_factor.select("factor", "City", "Province").distinct()
        return df_factor
    
    # 2、选择标准列
    df_panel_result = df_panel_result.drop('version', 'provider', 'owner').distinct()
    df_PHA_weight = df_PHA_weight.select('province', 'city', 'doi', 'weight', 'pha').distinct()
    if use_d_weight:
        df_PHA_weight_default = df_PHA_weight_default.select('province', 'city', 'doi', 'weight', 'pha').distinct()

    # %%
    # =========== 函数定义：输出结果 =============
    def createPartition(p_out):
        # 创建分区
        logger.debug('创建分区')
        Location = p_out + '/version=' + run_id + '/provider=' + project_name + '/owner=' + owner
        g_out_table = p_out.split('/')[-1]
        
        partition_input_list = [{
         "Values": [run_id, project_name,  owner], 
        "StorageDescriptor": {
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
            }, 
            "Location": Location, 
        } 
            }]    
        client = boto3.client('glue', region_name='cn-northwest-1')
        glue_info = client.batch_create_partition(DatabaseName=g_database_temp, TableName=g_out_table, PartitionInputList=partition_input_list)
        logger.debug(glue_info)
        
    def outResult(df, p_out):
        df = df.withColumn('version', func.lit(run_id)) \
                .withColumn('provider', func.lit(project_name)) \
                .withColumn('owner', func.lit(owner))
        df.repartition(1).write.format("parquet") \
                 .mode("append").partitionBy("version", "provider", "owner") \
                 .parquet(p_out)

    # %%
    # =========== 数据准备 =============
    # 医院权重文件 
    # 是否加上 weight_default
    if use_d_weight:
        df_PHA_weight_default = df_PHA_weight_default.where(col('DOI').isin(use_d_weight))
        df_PHA_weight_default = df_PHA_weight_default.withColumnRenamed('Weight', 'Weight_d')
        df_PHA_weight = df_PHA_weight.join(df_PHA_weight_default, on=['Province', 'City', 'DOI', 'PHA'], how='full')
        df_PHA_weight = df_PHA_weight.withColumn('Weight', func.when(col('Weight').isNull(), col('Weight_d')).otherwise(col('Weight')))
    
    df_PHA_weight = df_PHA_weight.select('Province', 'City', 'DOI', 'Weight', 'PHA')
    df_PHA_weight = df_PHA_weight.withColumnRenamed('Province', 'Province_w') \
                                    .withColumnRenamed('City', 'City_w')

    # %%
    # =========== 计算 max 函数 =============
    def calculate_max(market, if_base=False, if_box=False):
        logger.debug('market:' + market)
        # =========== 输入 =============
        # universe 读取
        if market in universe_choice_dict.keys():
            filetype = universe_choice_dict[market]
            df_universe =  kwargs['df_universe_other'].where(col('filetype')==filetype)  
        else:
            df_universe =  kwargs['df_universe_base']
            
        df_universe = cleanUniverse(df_universe)
            
        # universe_outlier 读取
        df_universe_outlier = kwargs['df_universe_outlier'].where(col('filetype')==market)
        df_universe_outlier = cleanUniverse(df_universe_outlier)
        
        # factor 读取
        if if_base:
            df_factor = kwargs['df_factor_base']
        else:
            df_factor = kwargs['df_factor'].where(col('filetype')==market)

        df_factor = cleanFactor(df_factor)   
            
        # weight 文件
        df_PHA_weight_market = df_PHA_weight.where(col('DOI') == market)
    
        # =========== 数据执行 =============
        logger.debug('数据执行-start')
        # universe 文件读取与处理：
        df_universe_outlier = df_universe_outlier.withColumn("City_Tier_2010", col("City_Tier_2010").cast(StringType()))
        df_universe_outlier = df_universe_outlier.select("PHA", "Est_DrugIncome_RMB", "PANEL", "Seg", "BEDSIZE")      
        df_universe = df_universe.withColumn("City_Tier_2010", col("City_Tier_2010").cast(StringType()))
        
        # panel 文件 
        df_original_panel = df_panel_result.where(col('DOI') == market)
        
        # 获得 panel, panel_seg：
        # 1. 样本数据
        # panel：整理成 max的格式，包含了所有在universe 的panel列标记为1的医院，当作所有样本医院的 max
        df_universe_panel_all = df_universe.where(col('PANEL') == 1).select('PHA', 'BEDSIZE', 'PANEL', 'Seg')
        df_panel = df_original_panel.join(df_universe_panel_all, on='PHA', how="inner") \
                                .groupBy('PHA', 'Province', 'City', 'Date', 'Molecule', 'Prod_Name', 'BEDSIZE', 'PANEL', 'Seg') \
                                .agg(func.sum("Sales").alias("Predict_Sales"), func.sum("Units").alias("Predict_Unit")).persist()
        
        # panel_seg：整理成seg层面，包含了所有在universe_ot 的panel列标记为1的医院，可以用来得到非样本医院的 max
        df_panel_drugincome = df_universe_outlier.where(col('PANEL') == 1) \
                                            .groupBy("Seg") \
                                            .agg(func.sum("Est_DrugIncome_RMB").alias("DrugIncome_Panel")).persist()
        df_original_panel_tmp = df_original_panel.join(df_universe_outlier, on='PHA', how='left').persist()
        df_panel_seg = df_original_panel_tmp.where(col('PANEL') == 1) \
                                        .groupBy('Date', 'Prod_Name', 'Seg', 'Molecule') \
                                        .agg(func.sum("Sales").alias("Sales_Panel"), func.sum("Units").alias("Units_Panel"))
        df_panel_seg = df_panel_seg.join(df_panel_drugincome, on="Seg", how="left").cache()
        
        # *** PHA_city 权重计算
        df_original_panel_weight = df_original_panel_tmp.join(df_PHA_weight_market, on=['PHA'], how='left')
        df_original_panel_weight = df_original_panel_weight.withColumn('Weight', func.when(col('Weight').isNull(), func.lit(1)) \
                                                                                .otherwise(col('Weight')))
        df_original_panel_weight = df_original_panel_weight.withColumn('Sales_w', col('Sales') * col('Weight')) \
                                                    .withColumn('Units_w', col('Units') * col('Weight'))
        df_panel_seg_weight = df_original_panel_weight.where(col('PANEL') == 1) \
                                        .groupBy('Date', 'Prod_Name', 'Seg', 'Molecule', 'Province_w', 'City_w') \
                                        .agg(func.sum("Sales_w").alias("Sales_Panel_w"), func.sum("Units_w").alias("Units_Panel_w")).persist()
        df_panel_seg_weight = df_panel_seg_weight.join(df_panel_drugincome, on="Seg", how="left").persist()
        df_panel_seg_weight = df_panel_seg_weight.withColumnRenamed('Province_w', 'Province') \
                                            .withColumnRenamed('City_w', 'City')
        
        # 2. 非样本数据
        # 将非样本的segment和factor等信息合并起来：
        if df_factor.where( (~col('Province').isNull()) & (col('Province') != 'None') ).count() == 0:
            df_factor = df_factor.select('City', 'factor').distinct()
            df_universe_factor_panel = df_universe.join(df_factor, on=["City"], how="left").cache().persist()
        else:
            df_factor = df_factor.select('City', 'factor', 'Province').distinct()
            df_universe_factor_panel = df_universe.join(df_factor, on=["City", 'Province'], how="left").persist()
            
        df_universe_factor_panel = df_universe_factor_panel.withColumn("factor", func.when(func.isnull(col('factor')), func.lit(1)).otherwise(col('factor'))) \
                                            .where(col('PANEL') == 0) \
                                            .select('Province', 'City', 'PHA', 'Est_DrugIncome_RMB', 'Seg', 'BEDSIZE', 'PANEL', 'factor')
        
        # 为这些非样本医院匹配上样本金额、产品、年月、所在segment的drugincome之和
        # 优先有权重的结果
        df_max_result = df_universe_factor_panel.join(df_panel_seg, on="Seg", how="left")
        df_max_result = df_max_result.join(df_panel_seg_weight.select('Date', 'Prod_Name', 'Molecule', 'Seg', 'Province', 'City', 'Sales_Panel_w', 'Units_Panel_w').distinct(), 
                                        on=['Date', 'Prod_Name', 'Molecule', 'Seg', 'Province', 'City'], how="left").cache()
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
                                        'Seg', 'Predict_Sales', 'Predict_Unit')
        # 3. 合并样本部分
        df_max_result = df_max_result.union(df_panel.select(df_max_result.columns))
        # 输出结果
        df_max_result = df_max_result.withColumn("DOI", func.lit(market))
        
        outResult(df_max_result, p_out_max)
        logger.debug("输出 max_result：" + market)

    # %%
    # 执行函数
    if if_others == "False":
        for i in all_models:
            calculate_max(i, if_base=if_base, if_box=False)
    elif if_others == "True":
        for i in all_models:
            calculate_max(i, if_base=if_base, if_box=True)
            
    createPartition(p_out_max)
    
    # 读回
    df_max_result_raw = spark.sql("SELECT * FROM %s.max_result_raw WHERE version='%s' AND provider='%s' AND  owner='%s'" 
                                 %(g_database_temp, run_id, project_name, owner))
    
    
    # =========== 数据输出 =============
    def lowerColumns(df):
        df = df.toDF(*[i.lower() for i in df.columns])
        return df
    
    df_max_result_raw = lowerColumns(df_max_result_raw)
    
    logger.debug('数据执行-Finish')
    
    return {'out_df':df_max_result_raw}
