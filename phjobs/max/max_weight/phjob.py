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
    g_project_name = kwargs['g_project_name']
    g_market = kwargs['g_market']
    g_universe = kwargs['g_universe']
    g_factor = kwargs['g_factor']
    g_universe_ot = kwargs['g_universe_ot']
    g_use_d_weight = kwargs['g_use_d_weight']
    time_left = kwargs['time_left']
    time_right = kwargs['time_right']
    max_path = kwargs['max_path']
    out_dir = kwargs['out_dir']
    dag_name = kwargs['dag_name']
    run_id = kwargs['run_id']
    depend_job_names_keys = kwargs['depend_job_names_keys']
    ### input args ###
    
    ### output args ###
    g_max_out = kwargs['g_max_out']
    ### output args ###

    from pyspark.sql.types import StringType, IntegerType, DoubleType
    from pyspark.sql import functions as func
    from pyspark.sql.functions import col

    # 测试输入
    g_project_name = "贝达"
    g_market = 'BD1'
    g_universe = 'universe_onc'
    g_factor = 'factor_BD1'
    g_universe_ot = 'universe_ot_BD1'
    
    time_left = "202001"
    time_right = "202012"
    out_dir = "202012_test"

    logger.debug('数据执行-start：max放大')
    
    # =========== 输入 输出 =============
    
    out_path_dir = max_path + "/" + g_project_name + '/' + out_dir
    
    if g_use_d_weight != "Empty":
        g_use_d_weight = g_use_d_weight.replace(" ","").split(",")
    else:
        g_use_d_weight = []
            
    # 输入
    p_universe = max_path + "/" + g_project_name + "/" + g_universe
    p_factor = max_path + "/" + g_project_name + "/factor/" + g_factor
    p_universe_ot = max_path + "/" + g_project_name + "/universe/"+ g_universe_ot
    
    p_panel = out_path_dir + "/panel_result"
    p_PHA_weight = max_path + "/" + g_project_name + '/PHA_weight'
    if g_use_d_weight:
        p_PHA_weight_default = max_path + "/" + g_project_name + '/PHA_weight_default'

    # # =========== 数据准备，测试用 =============
    # 1、panel 文件
    df_original_panel = spark.read.parquet(p_panel)
    df_original_panel
    df_original_panel = df_original_panel.withColumnRenamed('ID', 'ID') \
                        .withColumnRenamed('Date', 'DATE') \
                        .withColumnRenamed('Prod_Name', 'MIN_STD') \
                        .withColumnRenamed('DOI', 'MARKET') \
                        .withColumnRenamed('Hosp_name', 'HOSP_NAME') \
                        .withColumnRenamed('HOSP_ID', 'PHA') \
                        .withColumnRenamed('Molecule', 'MOLECULE_STD') \
                        .withColumnRenamed('Province', 'PROVINCE') \
                        .withColumnRenamed('City', 'CITY') \
                        .withColumnRenamed('add_flag', 'ADD_FLAG') \
                        .withColumnRenamed('std_route', 'ROUTE_STD') \
                        .withColumnRenamed('Sales', 'SALES') \
                        .withColumnRenamed('Units', 'UNITS')
    df_original_panel = df_original_panel.select("ID", "DATE", "MIN_STD", "MARKET", "HOSP_NAME", 
                                           "PHA", "MOLECULE_STD", "PROVINCE", "CITY", "ADD_FLAG", "ROUTE_STD",
                                           "SALES", "UNITS")
    df_original_panel = df_original_panel.where(col('DATE') == 202012)
    
    df_original_panel = df_original_panel.where((col('MARKET') == g_market) & 
                                          (col('DATE') >= time_left) & (col('DATE') <= time_right)).cache()

    # 2.医院权重文件	 
    df_PHA_weight = spark.read.parquet(p_PHA_weight)
    df_PHA_weight = df_PHA_weight.withColumnRenamed('Province', 'PROVINCE_WEIGHT') \
                            .withColumnRenamed('City', 'CITY_WEIGHT') \
                            .withColumnRenamed('DOI', 'MARKET') \
                            .withColumnRenamed('PHA', 'PHA') \
                            .withColumnRenamed('Weight', 'WEIGHT')
    
    
    # 是否加上 weight_default
    if g_use_d_weight:
        df_PHA_weight_default = spark.read.parquet(p_PHA_weight_default)
        df_PHA_weight_default = df_PHA_weight_default.withColumnRenamed('Province', 'PROVINCE_WEIGHT') \
                                                        .withColumnRenamed('City', 'CITY_WEIGHT') \
                                                        .withColumnRenamed('DOI', 'MARKET') \
                                                        .withColumnRenamed('PHA', 'PHA') \
                                                        .withColumnRenamed('Weight', 'WEIGHT_DEFAULT')
        
        df_PHA_weight_default = df_PHA_weight_default.where(col('MARKET').isin(g_use_d_weight))
        df_PHA_weight = df_PHA_weight.join(df_PHA_weight_default, on=['PROVINCE_WEIGHT', 'CITY_WEIGHT', 'MARKET', 'PHA'], how='full')
        df_PHA_weight = df_PHA_weight.withColumn('WEIGHT', 
                                                 func.when(col('WEIGHT').isNull(), col('WEIGHT_DEFAULT')).otherwise(col('WEIGHT')))
    
    df_PHA_weight = df_PHA_weight.select('PROVINCE_WEIGHT', 'CITY_WEIGHT', 'MARKET', 'WEIGHT', 'PHA')
    df_PHA_weight_market = df_PHA_weight.where(df_PHA_weight.MARKET == g_market)

    # 3. universe 文件
    df_universe = spark.read.parquet(p_universe)
    df_universe = df_universe.withColumnRenamed('Panel_ID', 'PHA') \
                        .withColumnRenamed('BEDSIZE', 'BEDSIZE') \
                        .withColumnRenamed('PANEL', 'PANEL') \
                        .withColumnRenamed('Seg', 'SEG') \
                        .withColumnRenamed('City', 'CITY') \
                        .withColumnRenamed('Province', 'PROVINCE') \
                        .withColumnRenamed('Est_DrugIncome_RMB', 'EST_DRUGINCOME_RMB')
    df_universe = df_universe.select("PHA", "BEDSIZE", "PANEL", "SEG", 'CITY', 'PROVINCE', 'EST_DRUGINCOME_RMB')
    df_universe

    # 4. factor 文件
    df_factor = spark.read.parquet(p_factor)
    df_factor = df_factor.withColumnRenamed('City', 'CITY') \
                .withColumnRenamed('Province', 'PROVINCE') \
                .withColumnRenamed('factor_new', 'FACTOR')
    df_factor

    # 5. universe_ot_ 文件
    df_universe_outlier = spark.read.parquet(p_universe_ot)
    df_universe_outlier = df_universe_outlier.withColumnRenamed('Panel_ID', 'PHA') \
                                        .withColumnRenamed('BEDSIZE', 'BEDSIZE') \
                                        .withColumnRenamed('Seg', 'SEG') \
                                        .withColumnRenamed('PANEL', 'PANEL') \
                                        .withColumnRenamed('Est_DrugIncome_RMB', 'EST_DRUGINCOME_RMB')
    df_universe_outlier = df_universe_outlier.select("PHA", "EST_DRUGINCOME_RMB", "PANEL", "SEG", "BEDSIZE")
    df_universe_outlier

    # =========== 数据执行 =============
    # 每次只执行一个月(模型年是一年)的一个market的数据
    
    # 获得 panel, df_panel_seg：group_panel_by_seg
    # 1、panel：universe 中panel为1的样本医院整理成max的格式，包含了所有在universe的panel列标记为1的医院，当作所有样本医院的max
    df_universe_panel_all = df_universe.where(df_universe.PANEL == 1).select('PHA', 'BEDSIZE', 'PANEL', 'SEG')
    
    df_panel = df_original_panel.join(df_universe_panel_all, on='PHA', how="inner") \
        .groupBy('PHA', 'PROVINCE', 'CITY', 'DATE', 'MOLECULE_STD', 'MIN_STD', 'BEDSIZE', 'PANEL', 'SEG') \
        .agg(func.sum("SALES").alias("PREDICT_SALES"), func.sum("UNITS").alias("PREDICT_UNIT")).cache()
    
    # 2、df_panel_seg：整理成seg层面，包含了所有在universe_ot的panel列标记为1的医院，可以用来得到非样本医院的max
    # df_universe_outlier 中 panel=1 样本的金额
    df_panel_drugincome = df_universe_outlier.where(col('PANEL') == 1) \
                                    .groupBy("SEG") \
                                    .agg(func.sum("EST_DRUGINCOME_RMB").alias("DRUGINCOME_PANEL")).cache()
    # df_universe_outlier 中 panel=1 的医院，对应的 raw_data 金额
    df_original_panel_tmp = df_original_panel.join(df_universe_outlier, on='PHA', how='left').cache()
    df_panel_seg = df_original_panel_tmp.where(col('PANEL') == 1) \
                                .groupBy('DATE', 'MIN_STD', 'SEG', 'MOLECULE_STD') \
                                .agg(func.sum("SALES").alias("SALES_PANEL"), func.sum("UNITS").alias("UNITS_PANEL")).cache()
    df_panel_seg = df_panel_seg.join(df_panel_drugincome, on="SEG", how="left").cache()
    # 3、PHA_city 权重计算
    df_original_panel_weight = df_original_panel_tmp.join(df_PHA_weight_market, on=['PHA'], how='left')
    df_original_panel_weight = df_original_panel_weight.withColumn('WEIGHT', func.when(col('WEIGHT').isNull(), func.lit(1)) \
                                                                            .otherwise(col('WEIGHT')))
    df_original_panel_weight = df_original_panel_weight.withColumn('SALES_WEIGHT', col('SALES') * col('WEIGHT')) \
                                                .withColumn('UNITS_WEIGHT', col('UNITS') * col('WEIGHT'))
    df_panel_seg_weight = df_original_panel_weight.where(col('PANEL') == 1) \
        .groupBy('DATE', 'MIN_STD', 'SEG', 'MOLECULE_STD', 'PROVINCE_WEIGHT', 'CITY_WEIGHT') \
        .agg(func.sum("SALES_WEIGHT").alias("SALES_PANEL_WEIGHT"), func.sum("UNITS_WEIGHT").alias("UNITS_PANEL_WEIGHT")).cache() # TEST
    df_panel_seg_weight = df_panel_seg_weight.join(df_panel_drugincome, on="SEG", how="left").cache() # TEST
    df_panel_seg_weight = df_panel_seg_weight.withColumnRenamed('PROVINCE_WEIGHT', 'PROVINCE') \
                    .withColumnRenamed('CITY_WEIGHT', 'CITY')

    # 将非样本的segment和factor等信息合并起来：get_uni_with_factor
    if 'PROVINCE' in df_factor.columns:
        df_factor = df_factor.select('CITY', 'FACTOR', 'PROVINCE').distinct()
        df_universe_factor_panel = df_universe.join(df_factor, on=["CITY", 'PROVINCE'], how="left").cache()
    else:
        df_factor = df_factor.select('CITY', 'FACTOR').distinct()
        df_universe_factor_panel = df_universe.join(df_factor, on=["CITY"], how="left").cache()
    
    df_universe_factor_panel = df_universe_factor_panel \
        .withColumn("FACTOR", func.when(func.isnull(col('FACTOR')), func.lit(1)).otherwise(col('FACTOR'))) \
        .where(col('PANEL') == 0) \
        .select('PROVINCE', 'CITY', 'PHA', 'EST_DRUGINCOME_RMB', 'SEG', 'BEDSIZE', 'PANEL', 'FACTOR').cache()

    
    # 为这些非样本医院匹配上样本金额、产品、年月、所在segment的drugincome之和
    # 优先有权重的结果
    df_max_result = df_universe_factor_panel.join(df_panel_seg, on="SEG", how="left")
    df_max_result = df_max_result.join(df_panel_seg_weight.select('DATE', 'MIN_STD', 'MOLECULE_STD', 'SEG', 'PROVINCE', 'CITY', 'SALES_PANEL_WEIGHT', 'UNITS_PANEL_WEIGHT').distinct(), 
                                    on=['DATE', 'MIN_STD', 'MOLECULE_STD', 'SEG', 'PROVINCE', 'CITY'], how="left")
    df_max_result = df_max_result.withColumn('SALES_PANEL', func.when(col('SALES_PANEL_WEIGHT').isNull(), col('SALES_PANEL')) \
                                                            .otherwise(col('SALES_PANEL_WEIGHT'))) \
                            .withColumn('UNITS_PANEL', func.when(col('UNITS_PANEL_WEIGHT').isNull(), col('UNITS_PANEL')) \
                                                            .otherwise(col('UNITS_PANEL_WEIGHT'))) \
                            .drop('SALES_PANEL_WEIGHT', 'UNITS_PANEL_WEIGHT')
    # 预测值等于样本金额乘上当前医院drugincome再除以所在segment的drugincome之和
    df_max_result = df_max_result.withColumn("PREDICT_SALES", (col('SALES_PANEL') / col('DRUGINCOME_PANEL')) * col('EST_DRUGINCOME_RMB')) \
        .withColumn("PREDICT_UNIT", (col('UNITS_PANEL') / col('DRUGINCOME_PANEL')) * col('EST_DRUGINCOME_RMB')).cache() # TEST
    # 为什么有空，因为部分segment无样本或者样本金额为0：remove_nega
    df_max_result = df_max_result.where(~func.isnull(col('PREDICT_SALES')))
    df_max_result = df_max_result.withColumn("POSITIVE", func.when(col("PREDICT_SALES") > 0, 1).otherwise(0))
    df_max_result = df_max_result.withColumn("POSITIVE", func.when(col("PREDICT_UNIT") > 0, 1).otherwise(col('POSITIVE')))
    df_max_result = df_max_result.where(col('POSITIVE') == 1).drop("POSITIVE")
    # 乘上factor
    df_max_result = df_max_result.withColumn("PREDICT_SALES", col('PREDICT_SALES') * col('FACTOR')) \
        .withColumn("PREDICT_UNIT", col('PREDICT_UNIT') * col('FACTOR')) \
        .select('PHA', 'PROVINCE', 'CITY', 'DATE', 'MOLECULE_STD', 'MIN_STD', 'BEDSIZE', 'PANEL',
                'SEG', 'PREDICT_SALES', 'PREDICT_UNIT')
    # 合并样本部分
    df_max_result = df_max_result.union(df_panel.select(df_max_result.columns))

    # 输出结果
    df_max_result = df_max_result.repartition(2)
    df_max_result.write.format("parquet") \
                    .mode("overwrite").save(result_path_prefix + g_max_out)
    logger.debug('数据执行-Finish')

    # df_max_result.agg(func.sum('Predict_Sales'), func.sum('Predict_Unit')).show()

    # df=spark.read.parquet('s3a://ph-max-auto/v0.0.1-2020-06-08/贝达/202012_test/MAX_result/MAX_result_202001_202012_BD1_hosp_level/')
    # df.where(col('Date')==202012).agg(func.sum('Predict_Sales'), func.sum('Predict_Unit')).show()







