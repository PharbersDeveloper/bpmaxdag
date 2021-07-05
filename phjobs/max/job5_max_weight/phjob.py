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
    left_models = kwargs['left_models']
    left_models_time_left = kwargs['left_models_time_left']
    right_models = kwargs['right_models']
    right_models_time_right = kwargs['right_models_time_right']
    all_models = kwargs['all_models']
    universe_choice = kwargs['universe_choice']
    use_d_weight = kwargs['use_d_weight']
    out_path = kwargs['out_path']
    run_id = kwargs['run_id']
    owner = kwargs['owner']
    g_input_version = kwargs['g_input_version']
    g_database_temp = kwargs['g_database_temp']
    g_database_input = kwargs['g_database_input']
    ### input args ###
    
    ### output args ###
    a = kwargs['a']
    b = kwargs['b']
    ### output args ###

    
    
    import os
    import pandas as pd
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    from pyspark.sql.functions import col     
    import json
    import boto3
    
    
    # %%
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
# # %%
# print('job5_max')
# # 输入输出

# if left_models != "Empty":
#     left_models = left_models.replace(", ",",").split(",")
# else:
#     left_models = []
# if right_models != "Empty":
#     right_models = right_models.replace(", ",",").split(",")
# else:
#     right_models = []
# if left_models_time_left == "Empty":
#     left_models_time_left = 0
# if right_models_time_right == "Empty":
#     right_models_time_right = 0
# time_parameters = [int(time_left), int(time_right), left_models, int(left_models_time_left), right_models, int(right_models_time_right)]
    # %% 
    # 输入参数设置
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
    
    dict_input_version = json.loads(g_input_version)
    logger.debug(dict_input_version)
    
    # 市场的universe文件
    universe_choice_dict={}
    if universe_choice != "Empty":
        for each in universe_choice.replace(", ",",").split(","):
            market_name = each.split(":")[0]
            universe_name = each.split(":")[1]
            universe_choice_dict[market_name]=universe_name
universe_choice_dict
    # %% 
    # 输入数据读取
    df_panel_result = spark.sql("SELECT * FROM %s.panel_result WHERE version='%s' AND provider='%s' AND  owner='%s'" 
                                     %(g_database_temp, run_id, project_name, owner))
    
    
    df_PHA_weight =  spark.sql("SELECT * FROM %s.weight WHERE provider='%s' AND filetype='%s' AND version='%s'" 
                                     %(g_database_input, project_name, 'weight', dict_input_version['weight']['weight']))
    
    if use_d_weight:
        df_PHA_weight_default =  spark.sql("SELECT * FROM %s.weight WHERE provider='%s' AND filetype='%s' AND version='%s'" 
                                         %(g_database_input, project_name, 'weight_default', dict_input_version['weight']['weight_default']))
df_panel_result
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
        df_PHA_weight = df_PHA_weight_default.select('province', 'city', 'doi', 'weight', 'pha').distinct()
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
            df_universe =  spark.sql("SELECT * FROM %s.universe_other WHERE provider='%s' AND filetype='%s' AND version='%s'" 
                                     %(g_database_input, project_name, filetype, dict_input_version['universe_other'][filetype]))    
        else:
            df_universe =  spark.sql("SELECT * FROM %s.universe_base WHERE provider='%s' AND version='%s'" 
                                 %(g_database_input, project_name, dict_input_version['universe_base']))
        df_universe = cleanUniverse(df_universe)
                   
            
        # universe_outlier 读取
        df_universe_outlier = spark.sql("SELECT * FROM %s.universe_outlier WHERE provider='%s' AND filetype='%s' AND version='%s'" 
                                     %(g_database_input, project_name, market, dict_input_version['universe_outlier'][market]))
        df_universe_outlier = cleanUniverse(df_universe_outlier)
        
        # factor 读取
        if if_base:
            df_factor = spark.sql("SELECT * FROM %s.factor WHERE provider='%s' AND filetype='%s' AND version='%s'" 
                                     %(g_database_input, project_name, market, dict_input_version['factor']['base']))
        else:
            df_factor = spark.sql("SELECT * FROM %s.factor WHERE provider='%s' AND filetype='%s' AND version='%s'" 
                                     %(g_database_input, project_name, market, dict_input_version['factor'][market]))   
        df_factor = cleanFactor(df_factor)   
            
        # weight 文件
        PHA_weight_market = PHA_weight.where(col('DOI') == market)
    
        # =========== 数据执行 =============
        logger.debug('数据执行-start')
        # 选择 market 的时间范围：choose_months
        time_range = str(time_left) + '_' + str(time_right)
    
        df_universe_outlier = df_universe_outlier.withColumn("City_Tier_2010", col("City_Tier_2010").cast(StringType()))
        df_universe_outlier = df_universe_outlier.select("PHA", "Est_DrugIncome_RMB", "PANEL", "Seg", "BEDSIZE")
        
        # universe 文件读取与处理：read_universe
        df_universe = df_universe.withColumn("City_Tier_2010", col("City_Tier_2010").cast(StringType()))
        
        # panel 文件读取 获得 original_panel
        original_panel = df_panel_result.where((col('DOI') == market) & (col('Date') >= time_left) & (col('Date') <= time_right)).persist()
        
        # 获得 panel, panel_seg：group_panel_by_seg
        # panel：整理成max的格式，包含了所有在universe的panel列标记为1的医院，当作所有样本医院的max
        universe_panel_all = df_universe.where(col('PANEL') == 1).select('PHA', 'BEDSIZE', 'PANEL', 'Seg')
        
        panel = original_panel \
            .join(universe_panel_all, on='PHA', how="inner") \
            .groupBy('PHA', 'Province', 'City', 'Date', 'Molecule', 'Prod_Name', 'BEDSIZE', 'PANEL', 'Seg') \
            .agg(func.sum("Sales").alias("Predict_Sales"), func.sum("Units").alias("Predict_Unit")).cache()
        # panel_seg：整理成seg层面，包含了所有在universe_ot的panel列标记为1的医院，可以用来得到非样本医院的max
        panel_drugincome = universe_outlier.where(universe_outlier.PANEL == 1) \
            .groupBy("Seg") \
            .agg(func.sum("Est_DrugIncome_RMB").alias("DrugIncome_Panel")).cache() # TEST
        original_panel_tmp = original_panel.join(universe_outlier, original_panel.HOSP_ID == universe_outlier.PHA, how='left').persist()
        panel_seg = original_panel_tmp.where(original_panel_tmp.PANEL == 1) \
            .groupBy('Date', 'Prod_Name', 'Seg', 'Molecule') \
            .agg(func.sum("Sales").alias("Sales_Panel"), func.sum("Units").alias("Units_Panel")).cache()
        panel_seg = panel_seg.join(panel_drugincome, on="Seg", how="left").cache() # TEST
        # *** PHA_city 权重计算
        original_panel_weight = original_panel_tmp.join(PHA_weight_market, on=['PHA'], how='left')
        original_panel_weight = original_panel_weight.withColumn('Weight', func.when(col('Weight').isNull(), func.lit(1)) \
                                                                                .otherwise(col('Weight')))
        original_panel_weight = original_panel_weight.withColumn('Sales_w', col('Sales') * original_panel_weight.Weight) \
                                                    .withColumn('Units_w', original_panel_weight.Units * original_panel_weight.Weight)
        panel_seg_weight = original_panel_weight.where(original_panel_weight.PANEL == 1) \
            .groupBy('Date', 'Prod_Name', 'Seg', 'Molecule', 'Province_w', 'City_w') \
            .agg(func.sum("Sales_w").alias("Sales_Panel_w"), func.sum("Units_w").alias("Units_Panel_w")).persist()
        panel_seg_weight = panel_seg_weight.join(panel_drugincome, on="Seg", how="left").persist()
        panel_seg_weight = panel_seg_weight.withColumnRenamed('Province_w', 'Province') \
                        .withColumnRenamed('City_w', 'City')
        # 将非样本的segment和factor等信息合并起来：get_uni_with_factor
        # factor = spark.read.parquet(factor_path)
        if "factor" not in factor.columns:
            factor = factor.withColumnRenamed("factor_new", "factor")
            
        if 'Province' in factor.columns:
            factor = factor.select('City', 'factor', 'Province').distinct()
            universe_factor_panel = universe.join(factor, on=["City", 'Province'], how="left").persist()
        else:
            factor = factor.select('City', 'factor').distinct()
            universe_factor_panel = universe.join(factor, on=["City"], how="left").cache().persist()
            
        universe_factor_panel = universe_factor_panel \
            .withColumn("factor", func.when(func.isnull(universe_factor_panel.factor), func.lit(1)).otherwise(universe_factor_panel.factor)) \
            .where(universe_factor_panel.PANEL == 0) \
            .select('Province', 'City', 'PHA', 'Est_DrugIncome_RMB', 'Seg', 'BEDSIZE', 'PANEL', 'factor').cache() # TEST
        # 为这些非样本医院匹配上样本金额、产品、年月、所在segment的drugincome之和
        # 优先有权重的结果
        max_result = universe_factor_panel.join(panel_seg, on="Seg", how="left")
        max_result = max_result.join(panel_seg_weight.select('Date', 'Prod_Name', 'Molecule', 'Seg', 'Province', 'City', 'Sales_Panel_w', 'Units_Panel_w').distinct(), 
                                        on=['Date', 'Prod_Name', 'Molecule', 'Seg', 'Province', 'City'], how="left")
        max_result = max_result.withColumn('Sales_Panel', func.when(max_result.Sales_Panel_w.isNull(), max_result.Sales_Panel) \
                                                                .otherwise(max_result.Sales_Panel_w)) \
                                .withColumn('Units_Panel', func.when(max_result.Units_Panel_w.isNull(), max_result.Units_Panel) \
                                                                .otherwise(max_result.Units_Panel_w)) \
                                .drop('Sales_Panel_w', 'Units_Panel_w')
        # 预测值等于样本金额乘上当前医院drugincome再除以所在segment的drugincome之和
        max_result = max_result.withColumn("Predict_Sales", (max_result.Sales_Panel / max_result.DrugIncome_Panel) * max_result.Est_DrugIncome_RMB) \
            .withColumn("Predict_Unit", (max_result.Units_Panel / max_result.DrugIncome_Panel) * max_result.Est_DrugIncome_RMB).cache() # TEST
        # 为什么有空，因为部分segment无样本或者样本金额为0：remove_nega
        max_result = max_result.where(~func.isnull(max_result.Predict_Sales))
        max_result = max_result.withColumn("positive", func.when(max_result["Predict_Sales"] > 0, 1).otherwise(0))
        max_result = max_result.withColumn("positive", func.when(max_result["Predict_Unit"] > 0, 1).otherwise(max_result.positive))
        max_result = max_result.where(max_result.positive == 1).drop("positive")
        # 乘上factor
        max_result = max_result.withColumn("Predict_Sales", max_result.Predict_Sales * max_result.factor) \
            .withColumn("Predict_Unit", max_result.Predict_Unit * max_result.factor) \
            .select('PHA', 'Province', 'City', 'Date', 'Molecule', 'Prod_Name', 'BEDSIZE', 'PANEL',
                    'Seg', 'Predict_Sales', 'Predict_Unit')
        # 合并样本部分
        max_result = max_result.union(panel.select(max_result.columns))
        # 输出结果
        # if if_base == False:
        max_result = max_result.repartition(2)
        if if_box:
            max_path = out_path_dir + "/MAX_result/MAX_result_" + time_range + '_'  + market + "_hosp_level_box"
            max_result.write.format("parquet") \
                .mode("overwrite").save(max_path)
        else:
            max_path = out_path_dir + "/MAX_result/MAX_result_" + time_range + '_' + market + "_hosp_level"
            max_result.write.format("parquet") \
                .mode("overwrite").save(max_path)
        logger.debug('数据执行-Finish')

    # %%
    # 执行函数
    if if_others == "False":
        for i in all_models:
            calculate_max(i, if_base=if_base, if_box=False)
    elif if_others == "True":
        for i in all_models:
            calculate_max(i, if_base=if_base, if_box=True)

    # %%
    # =========== 数据验证 =============
    # 与原R流程运行的结果比较正确性:
    if int(need_test) > 0:
        logger.debug('数据验证-start')
        def check_out(my_out_path, R_out_path):
            my_out = spark.read.parquet(my_out_path)
            R_out = spark.read.parquet(R_out_path)
            R_out = R_out.where(R_out.Date/100 < 2020)
            # 检查内容：列缺失，列的类型，列的值
            for colname, coltype in R_out.dtypes:
                # 列是否缺失
                if colname not in my_out.columns:
                    logger.warning ("miss columns:", colname)
                else:
                    # 数据类型检查
                    if my_out.select(colname).dtypes[0][1] != coltype:
                        logger.debug("different type columns: " + colname + ", " + my_out.select(colname).dtypes[0][1] + ", " + "right type: " + coltype)
                    # 数值列的值检查
                    if coltype == "double" or coltype == "int":
                        sum_my_out = my_out.groupBy().sum(colname).toPandas().iloc[0, 0]
                        sum_R = R_out.groupBy().sum(colname).toPandas().iloc[0, 0]
                        # print(colname, sum_raw_data, sum_R)
                        if (sum_my_out - sum_R) != 0:
                            logger.debug("different value(sum) columns: " + colname + ", " + str(sum_my_out) + ", " + "right value: " + str(sum_R))
        my_out_path = u"s3a://ph-max-auto/v0.0.1-2020-06-08/京新/MAX_result/MAX_result_201801-202004他汀_hosp_level"
        R_out_path = u"/common/projects/max/京新/MAX_result/MAX_result_201801-202004他汀_hosp_level"
        logger.debug(u"他汀")
        check_out(my_out_path, R_out_path)
        my_out_path = u"s3a://ph-max-auto/v0.0.1-2020-06-08/京新/MAX_result/MAX_result_201801-202004癫痫新分子_hosp_level"
        R_out_path = u"/common/projects/max/京新/MAX_result/MAX_result_201801-202004癫痫新分子_hosp_level"
        logger.debug(u"癫痫新分子")
        check_out(my_out_path, R_out_path)
        logger.debug('数据验证-Finish')

