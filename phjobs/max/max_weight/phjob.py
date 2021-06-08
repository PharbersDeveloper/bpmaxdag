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
    g_universe_ot = kwargs['g_universe_ot']
    g_use_d_weight = kwargs['g_use_d_weight']
    g_monthly_update = kwargs['g_monthly_update']
    depend_job_names_keys = kwargs['depend_job_names_keys']
    g_max_path = kwargs['g_max_path']
    g_base_path = kwargs['g_base_path']
    g_year = kwargs['g_year']
    g_month = kwargs['g_month']
    ### input args ###
    
    ### output args ###
    g_max_out = kwargs['g_max_out']
    ### output args ###

    
    
    
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    from pyspark.sql.functions import col    
    # %%
    # 测试输入
    
    # dag_name = 'Max'
    # run_id = 'max_test_beida_202012'
    
    # g_project_name = "贝达"
    # g_market = 'BD1'
    # g_universe = 'universe_onc'
    # g_universe_ot = 'universe_ot_BD1'
    # result_path_prefix=get_result_path({"name":job_name, "dag_name":dag_name, "run_id":run_id})
    # depends_path=get_depends_path({"name":job_name, "dag_name":dag_name, 
    #                                  "run_id":run_id, "depend_job_names_keys":depend_job_names_keys })
    # g_monthly_update = 'True'
    # g_year = '2020'
    # g_month = '12'
    
    # # g_monthly_update = 'False'
    # # g_year = '2019'

    # %%
    logger.debug('数据执行-start：max放大')
    
    # =========== 输入 输出 =============
    
    # out_path_dir = g_max_path + "/" + g_project_name + '/' + out_dir
    # p_panel = out_path_dir + "/panel_result"
    
    if g_use_d_weight != "Empty":
        g_use_d_weight = g_use_d_weight.replace(" ","").split(",")
    else:
        g_use_d_weight = []
            
    # 输入
    p_universe = g_max_path + "/" + g_project_name + "/" + g_universe
    p_universe_ot = g_max_path + "/" + g_project_name + "/universe/"+ g_universe_ot
    
    
    g_year = int(g_year)
    
    # 根据是否为月更选项panel文件路径
    if g_monthly_update == 'True':
        p_panel = depends_path['panel_monthly_out']
        g_month = int(g_month)
    elif g_monthly_update == 'False':
        p_panel = depends_path['panel_model_out']
    
        
    # 输出
    p_max_out = result_path_prefix + g_max_out

    # %%
    ## jupyter测试
    
    # p_panel = p_panel.replace("s3:", "s3a:")
    # p_max_out = p_max_out.replace("s3:", "s3a:")
    # %% 
    
    def createView(company, table_name, sub_path, other = "",
        time="2021-04-06",
        base_path = g_base_path):
             
            definite_path = "{base_path}/{sub_path}/TIME={time}/COMPANY={company}/{other}"
            path = definite_path.format(
                base_path = base_path,
                sub_path = sub_path,
                time = time,
                company = company,
                other = other
            )
            spark.read.parquet(path).createOrReplaceTempView(table_name)
            
    
    factor_market = "MODEL="+g_market
    createView(g_project_name, "factor", "DIMENSION/MAPPING/FACTOR", other = factor_market, time = "2021-04-14")
    createView(g_project_name, "weight", "DIMENSION/MAPPING/WEIGHT", time = "2021-04-14")
    # %%
    # # =========== 数据准备，测试用 =============
    # 1、panel 文件
    '''
    df_original_panel = spark.read.parquet(p_panel)
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
    '''

    # %%
    
    ### 使用其他项目测试
    # createView("Takeda", "weight", "DIMENSION/MAPPING/WEIGHT", time = "2021-04-14")
    ###
    
    ## OTHER 为weight
    ## "DEFAULT"是原PHA_weight_default文件
    ## DOI=MARKET
    weight_sql = """  SELECT * FROM weight """
    df_weight = spark.sql(weight_sql)
    df_weight = df_weight.withColumnRenamed("PROVINCE", "PROVINCE_WEIGHT")\
                        .withColumnRenamed("CITY", "CITY_WEIGHT")\
                        .withColumnRenamed("DOI", "MARKET")
    df_PHA_weight = df_weight.where(df_weight.FLAG=="OTHER").select(["PROVINCE_WEIGHT", "CITY_WEIGHT", "MARKET", "WEIGHT","PHA"])
    if g_use_d_weight:
        df_PHA_weight_default = df_weight.where(df_weight.FLAG=="DEFAULT").select(["PROVINCE_WEIGHT", "CITY_WEIGHT", "MARKET", "WEIGHT","PHA"])\
                                        .withColumnRenamed('WEIGHT', 'WEIGHT_DEFAULT')
        df_PHA_weight_default = df_PHA_weight_default.where(col('MARKET').isin(g_use_d_weight))
        df_PHA_weight = df_PHA_weight.join(df_PHA_weight_default, on=['PROVINCE_WEIGHT', 'CITY_WEIGHT', 'MARKET', 'PHA'], how='full')
        df_PHA_weight = df_PHA_weight.withColumn('WEIGHT', 
                                                 func.when( col('WEIGHT').isNull(), 
                                                col('WEIGHT_DEFAULT') ).otherwise(col('WEIGHT')))
    
    df_PHA_weight = df_PHA_weight.select('PROVINCE_WEIGHT', 'CITY_WEIGHT', 'MARKET', 'WEIGHT', 'PHA')
    df_PHA_weight_market = df_PHA_weight.where(df_PHA_weight.MARKET == g_market)

    # %%
    # 3. universe 文件
    
    df_universe = spark.read.parquet(p_universe)
    df_universe = df_universe.withColumnRenamed('Panel_ID', 'PHA') \
                        .withColumnRenamed('BEDSIZE', 'BEDSIZE') \
                        .withColumnRenamed('PANEL', 'PANEL') \
                        .withColumnRenamed('Seg', 'SEG') \
                        .withColumnRenamed('City', 'CITY') \
                        .withColumnRenamed('Province', 'PROVINCE') \
                        .withColumnRenamed('Est_DrugIncome_RMB', 'EST_DRUGINCOME_RMB')
    # print(df_universe)
    
    '''
    # 2、读取 universe 数据(Panel列不一样，暂时不替换)
    def createView(company, table_name, model,
            time="2021-04-06", 
            base_path = "s3a://ph-max-auto/2020-08-11/data_matching/refactor/data/MAX"):
                
                definite_path = "{base_path}/{model}/TIME={time}/COMPANY={company}"
                dim_path = definite_path.format(
                    base_path = base_path,
                    model = model,
                    time = time,
                    company = company
                )
                spark.read.parquet(dim_path).createOrReplaceTempView(table_name)
                
    createView(g_project_name, "hospital_dimesion", "DIMENSION/HOSPITAL_DIMENSION", "2021-04-06")
    createView(g_project_name, "hospital_fact", "FACT/HOSPITAL_FACT", "2021-04-06")
    createView(g_project_name, "cpa_gyc_mapping", "DIMENSION/MAPPING/CPA_GYC_MAPPING/STANDARD", "2021-04-06")
    createView(g_project_name, "product_dimesion", "DIMENSION/PRODUCT_DIMENSION", "2021-04-06")
    createView(g_project_name, "mnf_dimesion", "DIMENSION/MNF_DIMENSION", "2021-04-06")
    createView(g_project_name, "product_rel_dimesion", "DIMENSION/PRODUCT_RELATIONSHIP_DIMENSION", "2021-04-06")
    createView(g_project_name, "raw_data_fact", "FACT/RAW_DATA_FACT", "2021-04-06")
    
    base_universe_sql = """
        SELECT PHA_ID AS PHA, HOSPITAL_ID, HOSP_NAME, 
                    PROVINCE, CITY, CITYGROUP AS CITY_TIER, 
                    REGION, TOTAL AS BEDSIZE, SEG, BID_SAMPLE AS PANEL, MEDICINE_RMB AS EST_DRUGINCOME_RMB  FROM (
                SELECT 
                    PHA_ID, HOSPITAL_ID, HOSP_NAME, 
                    PROVINCE, CITY, CITYGROUP, 
                    REGION, TAG, VALUE, SEG 
                FROM hospital_dimesion AS hdim 
                    INNER JOIN hospital_fact AS hfct
                    ON hdim.ID == hfct.HOSPITAL_ID WHERE (CATEGORY = 'BEDCAPACITY' AND TAG = 'TOTAL') OR (CATEGORY = 'IS' AND TAG = 'BID_SAMPLE') OR (CATEGORY = 'REVENUE' AND TAG = 'MEDICINE_RMB')
            )
            PIVOT (
                SUM(VALUE)
                FOR TAG in ('TOTAL', 'BID_SAMPLE', 'MEDICINE_RMB')
            )
    """
    df_universe = spark.sql(base_universe_sql)
    '''
    # %%
    # 读取factor
    factor_sql = """  SELECT * FROM factor """
    df_factor = spark.sql(factor_sql)
    df_factor = df_factor.select(["PROVINCE", "CITY", "FACTOR"])

    # %%
    # 5. universe_ot_ 文件
    df_universe_outlier = spark.read.parquet(p_universe_ot)
    df_universe_outlier = df_universe_outlier.withColumnRenamed('Panel_ID', 'PHA') \
                                        .withColumnRenamed('BEDSIZE', 'BEDSIZE') \
                                        .withColumnRenamed('Seg', 'SEG') \
                                        .withColumnRenamed('PANEL', 'PANEL') \
                                        .withColumnRenamed('Est_DrugIncome_RMB', 'EST_DRUGINCOME_RMB')
    df_universe_outlier = df_universe_outlier.select("PHA", "EST_DRUGINCOME_RMB", "PANEL", "SEG", "BEDSIZE")
    # df_universe_outlier

    # %%
    # =========== 数据读取 =============
    # 1、读取 panel
    # df_original_panel = spark.read.parquet(p_panel)
    struct_type_panel = StructType([ StructField('ID', StringType(), True),
                                        StructField('DATE', IntegerType(), True),
                                        StructField('PACK_ID', StringType(), True),
                                        StructField('MIN_STD', StringType(), True),
                                        StructField('MARKET', StringType(), True),
                                        StructField('HOSP_NAME', StringType(), True),
                                        StructField('PHA', StringType(), True),
                                        StructField('MOLECULE_STD', StringType(), True),
                                        StructField('PROVINCE', StringType(), True),
                                        StructField('CITY', StringType(), True),
                                        StructField('ADD_FLAG', IntegerType(), True),
                                        StructField('ROUTE_STD', StringType(), True),
                                        StructField('SALES', DoubleType(), True),
                                        StructField('UNITS', DoubleType(), True) ])
    df_original_panel = spark.read.format("parquet").load(p_panel, schema=struct_type_panel)
    df_original_panel = df_original_panel.select("ID", "DATE", 'PACK_ID', "MIN_STD", "MARKET", "HOSP_NAME", 
                                           "PHA", "MOLECULE_STD", "PROVINCE", "CITY", "ADD_FLAG", "ROUTE_STD",
                                           "SALES", "UNITS")
    
    if g_monthly_update == 'True':
        df_original_panel = df_original_panel.where(col('DATE') == (g_year*100 + g_month))
    elif g_monthly_update == 'False':
        df_original_panel = df_original_panel.where((col('DATE')/100).cast(IntegerType()) == g_year)
    

    # %%
    # =========== 数据执行 =============
    df_universe = df_universe.select("PHA", "BEDSIZE", "PANEL", "SEG", 'CITY', 'PROVINCE', 'EST_DRUGINCOME_RMB')

    # %%
    # == 放大过程 ==
    # 每次只执行一个月(模型年是一年)的一个market的数据
    
    # 获得 panel, df_panel_seg：group_panel_by_seg
    # 1、panel：universe 中panel为1的样本医院整理成max的格式，包含了所有在universe的panel列标记为1的医院，当作所有样本医院的max
    df_universe_panel_all = df_universe.where(df_universe.PANEL == 1).select('PHA', 'BEDSIZE', 'PANEL', 'SEG')
    
    df_panel = df_original_panel.join(df_universe_panel_all, on='PHA', how="inner") \
        .groupBy('PHA', 'PROVINCE', 'CITY', 'DATE', 'MOLECULE_STD', 'MIN_STD', 'BEDSIZE', 'PANEL', 'SEG', 'PACK_ID') \
        .agg(func.sum("SALES").alias("PREDICT_SALES"), func.sum("UNITS").alias("PREDICT_UNIT")).cache()
    
    # 2、df_panel_seg：整理成seg层面，包含了所有在universe_ot的panel列标记为1的医院，可以用来得到非样本医院的max
    # df_universe_outlier 中 panel=1 样本的金额
    df_panel_drugincome = df_universe_outlier.where(col('PANEL') == 1) \
                                    .groupBy("SEG") \
                                    .agg(func.sum("EST_DRUGINCOME_RMB").alias("DRUGINCOME_PANEL")).cache()
    # df_universe_outlier 中 panel=1 的医院，对应的 raw_data 金额
    df_original_panel_tmp = df_original_panel.join(df_universe_outlier, on='PHA', how='left').cache()
    df_panel_seg = df_original_panel_tmp.where(col('PANEL') == 1) \
                                .groupBy('DATE', 'MIN_STD', 'SEG', 'MOLECULE_STD', 'PACK_ID') \
                                .agg(func.sum("SALES").alias("SALES_PANEL"), func.sum("UNITS").alias("UNITS_PANEL")).cache()
    df_panel_seg = df_panel_seg.join(df_panel_drugincome, on="SEG", how="left").cache()
    # 3、PHA_city 权重计算
    df_original_panel_weight = df_original_panel_tmp.join(df_PHA_weight_market, on=['PHA'], how='left')
    df_original_panel_weight = df_original_panel_weight.withColumn('WEIGHT', func.when(col('WEIGHT').isNull(), func.lit(1)) \
                                                                            .otherwise(col('WEIGHT')))
    df_original_panel_weight = df_original_panel_weight.withColumn('SALES_WEIGHT', col('SALES') * col('WEIGHT')) \
                                                .withColumn('UNITS_WEIGHT', col('UNITS') * col('WEIGHT'))
    df_panel_seg_weight = df_original_panel_weight.where(col('PANEL') == 1) \
        .groupBy('DATE', 'MIN_STD', 'SEG', 'MOLECULE_STD', 'PROVINCE_WEIGHT', 'CITY_WEIGHT', 'PACK_ID') \
        .agg(func.sum("SALES_WEIGHT").alias("SALES_PANEL_WEIGHT"), func.sum("UNITS_WEIGHT").alias("UNITS_PANEL_WEIGHT")).cache() # TEST
    df_panel_seg_weight = df_panel_seg_weight.join(df_panel_drugincome, on="SEG", how="left").cache() # TEST
    df_panel_seg_weight = df_panel_seg_weight.withColumnRenamed('PROVINCE_WEIGHT', 'PROVINCE') \
                    .withColumnRenamed('CITY_WEIGHT', 'CITY')

    # %%
    # 将非样本的segment和factor等信息合并起来：get_uni_with_factor
    if  df_factor.where(df_factor["PROVINCE"]=="null" ).count() == 0 :
        df_factor = df_factor.select('CITY', 'FACTOR', 'PROVINCE').distinct()
        df_universe_factor_panel = df_universe.join(df_factor, on=["CITY", 'PROVINCE'], how="left").cache()
        logger.debug("province is not null")
    elif ( df_factor.where(df_factor["PROVINCE"]=="null" ).count() == df_factor.count() ):
        df_factor = df_factor.select('CITY', 'FACTOR').distinct()
        df_universe_factor_panel = df_universe.join(df_factor, on=["CITY"], how="left").cache()
        logger.debug("province is all null")
    else:
        raise ValueError("factor文件的处理有问题")
    
    df_universe_factor_panel = df_universe_factor_panel \
        .withColumn("FACTOR", func.when(func.isnull(col('FACTOR')), func.lit(1)).otherwise(col('FACTOR'))) \
        .where(col('PANEL') == 0) \
        .select('PROVINCE', 'CITY', 'PHA', 'EST_DRUGINCOME_RMB', 'SEG', 'BEDSIZE', 'PANEL', 'FACTOR').cache()

    # %%
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
                'SEG', 'PREDICT_SALES', 'PREDICT_UNIT', 'PACK_ID')
    # 合并样本部分
    df_max_result = df_max_result.union(df_panel.select(df_max_result.columns))

    # %%
    # =========== 输出结果 =============
    df_max_result = df_max_result.repartition(1)
    df_max_result.write.format("parquet").partitionBy("DATE") \
                        .mode("append").save(p_max_out)
    
    logger.debug('数据执行-Finish')

    # %%
    # df = spark.read.parquet('s3a://ph-max-auto/2020-08-11/Max/refactor/runs/max_test_beida_202012/max_weight/max_weight_result/DATE=202012/')
    # df.agg(func.sum('Predict_Sales'), func.sum('Predict_Unit')).show()

    # %%
    # 月更
    # df=spark.read.parquet('s3a://ph-max-auto/v0.0.1-2020-06-08/贝达/202012_test/MAX_result/MAX_result_202001_202012_BD1_hosp_level/')
    # df = df.where(col('Date')==202012)
    # df.agg(func.sum('Predict_Sales'), func.sum('Predict_Unit')).show()
    # print( df.count() )
    
    # df_max_result.where(col('Date')==202012).agg(func.sum('Predict_Sales'), func.sum('Predict_Unit')).show()
    # print(df_max_result.count() )
    # %%
    # 模型
    # df=spark.read.parquet('s3a://ph-max-auto/v0.0.1-2020-06-08/贝达/201912_test/MAX_result/MAX_result_201701_201912_BD1_hosp_level')
    # df.where(col('Date')>=201901).where(col('Date')<=201912).agg(func.sum('Predict_Sales'), func.sum('Predict_Unit')).show()

