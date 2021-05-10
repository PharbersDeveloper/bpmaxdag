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
    max_path = kwargs['max_path']
    out_path = kwargs['out_path']
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
    if_others = kwargs['if_others']
    out_dir = kwargs['out_dir']
    ### input args ###
    
    ### output args ###
    c = kwargs['c']
    d = kwargs['d']
    ### output args ###

    from pyspark.sql import SparkSession
    from pyspark.sql.types import StringType, IntegerType, DoubleType
    from pyspark.sql import functions as func
    import os    # %%
    '''
    max_path = 's3a://ph-max-auto/v0.0.1-2020-06-08/'
    out_path = 's3a://ph-max-auto/v0.0.1-2020-06-08/'
    project_name = '汇宇'
    if_base = 'False'
    time_left = '202001'
    time_right = '202011'
    left_models = "Empty"
    left_models_time_left = "Empty"
    right_models = "Empty"
    right_models_time_right = "Empty"
    all_models = '阿扎胞苷,紫杉醇,奥沙利铂'
    universe_choice = '奥沙利铂:universe_base_肿瘤'
    if_others = 'False'
    out_dir = '202011'
    '''
    # %%
    # 输入输出
    if if_base == "False":
        if_base = False
    elif if_base == "True":
        if_base = True
    else:
        raise ValueError('if_base: False or True')
    if left_models != "Empty":
        left_models = left_models.replace(", ",",").split(",")
    else:
        left_models = []
    if right_models != "Empty":
        right_models = right_models.replace(", ",",").split(",")
    else:
        right_models = []
    if left_models_time_left == "Empty":
        left_models_time_left = 0
    if right_models_time_right == "Empty":
        right_models_time_right = 0
    
    time_parameters = [int(time_left), int(time_right), left_models, int(left_models_time_left), right_models, int(right_models_time_right)]
    
    if all_models != "Empty":
        all_models = all_models.replace(", ",",").split(",")
    else:
        all_models = []
    
    project_path = max_path + "/" + project_name
    
    if if_others == "True":
        out_dir = out_dir + "/others_box/"
    out_path_dir = out_path + "/" + project_name + '/' + out_dir
    
    # 市场的universe文件
    universe_choice_dict={}
    if universe_choice != "Empty":
        for each in universe_choice.replace(", ",",").split(","):
            market_name = each.split(":")[0]
            universe_name = each.split(":")[1]
            universe_choice_dict[market_name]=universe_name
    
    # 医院权重文件	 
    PHA_weight_path = max_path + "/" + project_name + '/PHA_weight'
    PHA_weight = spark.read.parquet(PHA_weight_path)
    PHA_weight = PHA_weight.select('Province', 'City', 'DOI', 'Weight', 'PHA')
    PHA_weight = PHA_weight.withColumnRenamed('Province', 'Province_w') \
                            .withColumnRenamed('City', 'City_w')
    # %%
    out_path = out_path_dir + "/max_check/check_lu"
    out_csv_path = out_path_dir + "/max_check/check_lu.csv"
    
    if if_others == "False":
        if_box=False
    elif if_others == "True":
        if_box=True
    
    for index, market in enumerate(all_models):
        if project_name == "Sanofi" or project_name == "AZ":
            if market in ['SNY6', 'SNY10', 'SNY12', 'SNY13', 'AZ12', 'AZ18', 'AZ21']:
                universe_path = project_path + '/universe_az_sanofi_onc'
            elif market in ['SNY5', 'SNY9', 'AZ10', 'AZ11', 'AZ15', 'AZ16', 'AZ14', 'AZ26', 'AZ24']:
                universe_path = project_path + '/universe_az_sanofi_mch'
            else:
                universe_path = project_path + '/universe_base'
        else:
            if market in universe_choice_dict.keys():
                universe_path = project_path + '/' + universe_choice_dict[market]
            else:
                universe_path = project_path + '/universe_base'
    
        # universe_outlier_path 以及 factor_path 文件选择
        universe_outlier_path = project_path + "/universe/universe_ot_" + market
        if if_base:
            factor_path = project_path + "/factor/factor_base"
        else:
            factor_path = project_path + "/factor/factor_" + market
    
        # panel 文件选择与读取 获得 original_panel
        panel_box_path = out_path_dir + "/panel_result_box"
        panel_path = out_path_dir + "/panel_result"
    
        if if_box:
            original_panel_path = panel_box_path
        else:
            original_panel_path = panel_path
    
        PHA_weight_market = PHA_weight.where(PHA_weight.DOI == market)
    
        # =========== 数据执行 =============
    
        #logger.info('数据执行-start')
    
        # 选择 market 的时间范围：choose_months
        time_left_raw = time_parameters[0]
        time_right_raw = time_parameters[1]
        left_models = time_parameters[2]
        left_models_time_left = time_parameters[3]
        right_models = time_parameters[4]
        right_models_time_right = time_parameters[5]
    
        if market in left_models:
            time_left = left_models_time_left
        else:
            time_left = time_left_raw
            
        if market in right_models:
            time_right = right_models_time_right
        else:
            time_right = time_right_raw
            
        time_range = str(time_left) + '_' + str(time_right)
    
        # universe_outlier 文件读取与处理：read_uni_ot
        universe_outlier = spark.read.parquet(universe_outlier_path)
        if "CITYGROUP" in universe_outlier.columns:
            universe_outlier = universe_outlier.withColumnRenamed("CITYGROUP", "City_Tier_2010")
        elif "City_Tier" in universe_outlier.columns:
            universe_outlier = universe_outlier.withColumnRenamed("City_Tier", "City_Tier_2010")
        universe_outlier = universe_outlier.withColumnRenamed("Panel_ID", "PHA") \
            .withColumnRenamed("Hosp_name", "HOSP_NAME")
        universe_outlier = universe_outlier.withColumn("City_Tier_2010", universe_outlier["City_Tier_2010"].cast(StringType()))
        universe_outlier = universe_outlier.select("PHA", "Est_DrugIncome_RMB", "PANEL", "Seg", "BEDSIZE")
    
        # universe 文件读取与处理：read_universe
        universe = spark.read.parquet(universe_path)
        if "CITYGROUP" in universe.columns:
            universe = universe.withColumnRenamed("CITYGROUP", "City_Tier_2010")
        elif "City_Tier" in universe.columns:
            universe = universe.withColumnRenamed("City_Tier", "City_Tier_2010")
        universe = universe.withColumnRenamed("Panel_ID", "PHA") \
            .withColumnRenamed("Hosp_name", "HOSP_NAME")
        universe = universe.withColumn("City_Tier_2010", universe["City_Tier_2010"].cast(StringType()))
    
        # panel 文件读取 获得 original_panel
        original_panel = spark.read.parquet(original_panel_path)
        original_panel = original_panel.where((original_panel.DOI == market) & (original_panel.Date >= time_left) & (original_panel.Date <= time_right)).cache() # TEST
    
        # 获得 panel, panel_seg：group_panel_by_seg
    
        # panel：整理成max的格式，包含了所有在universe的panel列标记为1的医院，当作所有样本医院的max
        universe_panel_all = universe.where(universe.PANEL == 1).select('PHA', 'BEDSIZE', 'PANEL', 'Seg')
        panel = original_panel \
            .join(universe_panel_all, original_panel.HOSP_ID == universe_panel_all.PHA, how="inner") \
            .groupBy('PHA', 'Province', 'City', 'Date', 'Molecule', 'Prod_Name', 'BEDSIZE', 'PANEL', 'Seg') \
            .agg(func.sum("Sales").alias("Predict_Sales"), func.sum("Units").alias("Predict_Unit")).cache()
    
        # panel_seg：整理成seg层面，包含了所有在universe_ot的panel列标记为1的医院，可以用来得到非样本医院的max
        panel_drugincome = universe_outlier.where(universe_outlier.PANEL == 1) \
            .groupBy("Seg") \
            .agg(func.sum("Est_DrugIncome_RMB").alias("DrugIncome_Panel")).cache() # TEST
        original_panel_tmp = original_panel.join(universe_outlier, original_panel.HOSP_ID == universe_outlier.PHA, how='left').cache() # TEST
    
        panel_seg = original_panel_tmp.where(original_panel_tmp.PANEL == 1) \
            .groupBy('Date', 'Prod_Name', 'Seg', 'Molecule') \
            .agg(func.sum("Sales").alias("Sales_Panel"), func.sum("Units").alias("Units_Panel")).cache()
        panel_seg = panel_seg.join(panel_drugincome, on="Seg", how="left").cache() # TEST
    
        # *** PHA_city 权重计算
        original_panel_weight = original_panel_tmp.join(PHA_weight_market, on=['PHA'], how='left')
        original_panel_weight = original_panel_weight.withColumn('Weight', func.when(original_panel_weight.Weight.isNull(), func.lit(1)) \
                                                                                .otherwise(original_panel_weight.Weight))
        original_panel_weight = original_panel_weight.withColumn('Sales_w', original_panel_weight.Sales * original_panel_weight.Weight) \
                                                    .withColumn('Units_w', original_panel_weight.Units * original_panel_weight.Weight)
    
        panel_seg_weight = original_panel_weight.where(original_panel_weight.PANEL == 1) \
            .groupBy('Date', 'Prod_Name', 'Seg', 'Molecule', 'Province_w', 'City_w') \
            .agg(func.sum("Sales_w").alias("Sales_Panel_w"), func.sum("Units_w").alias("Units_Panel_w")).cache() # TEST
        panel_seg_weight = panel_seg_weight.join(panel_drugincome, on="Seg", how="left").cache() # TEST
    
        panel_seg_weight = panel_seg_weight.withColumnRenamed('Province_w', 'Province') \
                        .withColumnRenamed('City_w', 'City')
    
        # 将非样本的segment和factor等信息合并起来：get_uni_with_factor
        factor = spark.read.parquet(factor_path)
        if "factor" not in factor.columns:
            factor = factor.withColumnRenamed("factor_new", "factor")
        factor = factor.select('City', 'factor')
        universe_factor_panel = universe.join(factor, on="City", how="left").cache() # TEST
        universe_factor_panel = universe_factor_panel \
            .withColumn("factor", func.when(func.isnull(universe_factor_panel.factor), func.lit(1)).otherwise(universe_factor_panel.factor)) \
            .where(universe_factor_panel.PANEL == 1) \
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
        max_result = max_result.select('PHA', 'Province', 'City', 'Date', 'Molecule', 'Prod_Name', 'BEDSIZE', 'PANEL',
                    'Seg', 'Predict_Sales', 'Predict_Unit')
    
        panel_out = panel.withColumnRenamed('Predict_Sales', 'Sales') \
                        .withColumnRenamed('Predict_Unit', 'Unit') \
                        .select('PHA', 'Date', 'Prod_Name', 'Sales', 'Unit')
        out = max_result.join(panel_out, on=['PHA', 'Date', 'Prod_Name'], how='left')
        
        out = out.withColumn('DOI', func.lit(market))
        
        # 输出结果
        out = out.repartition(1)
        if index == 0:
            out.write.format("parquet") \
                .mode("overwrite").save(out_path)
        else:
            out.write.format("parquet") \
                .mode("append").save(out_path)
    # %%
    out_csv = spark.read.parquet(out_path)
    out_csv = out_csv.repartition(1)
    out_csv.write.format("csv").option("header", "true") \
        .mode("overwrite").save(out_csv_path)
