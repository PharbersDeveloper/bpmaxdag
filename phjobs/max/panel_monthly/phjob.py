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
    g_model_month_right = kwargs['g_model_month_right']
    g_year = kwargs['g_year']
    g_current_month = kwargs['g_current_month']
    g_add_47 = kwargs['g_add_47']
    depend_job_names_keys = kwargs['depend_job_names_keys']
    g_monthly_update = kwargs['g_monthly_update']
    dag_name = kwargs['dag_name']
    run_id = kwargs['run_id']
    max_path = kwargs['max_path']
    ### input args ###
    
    ### output args ###
    g_panel = kwargs['g_panel']
    ### output args ###

    from pyspark.sql.functions import col
    from pyspark.sql.types import IntegerType, StringType, StructType, StructField, DoubleType    
    from pyspark.sql import  functions as func
    import pandas as pd           # %%
    # 测试用的参数
    '''
    g_project_name ="贝达"
    g_model_month_right="201912"
    g_year=2020
    g_current_month="12"
    g_add_47="True"
    '''
    # %%
    logger.debug('panel_monthly')
    # 是否运行此job
    if g_monthly_update == "True":
         return
        
    # 输入
    # p_universe = max_path + "/" + g_project_name + "/universe_base"
    p_market  = max_path + "/" + g_project_name + "/mkt_mapping"
    p_raw_data_adding_final = depends_path['raw_data_adding_final']
    
    # 月更新就没有 hostpital的数据
    if g_add_47 != "False" and g_add_47 != "True":
        logger.error('wrong input: g_add_47, False or True') 
        raise ValueError('wrong input: g_add_47, False or True')
        
    # 月更新相关输入
    # if monthly_update == "True":
    g_year = int(g_year)
    g_current_month = int(g_current_month)
    
    # if g_p_not_arrived == "Empty":
    p_not_arrived = max_path + "/Common_files/Not_arrived" + str(g_year*100 + g_current_month) + ".csv"  
    
    # if g_p_published == "Empty":
    p_published_right = max_path + "/Common_files/Published" + str(g_year) + ".csv"
    p_published_left = max_path + "/Common_files/Published" + str(g_year - 1) + ".csv"
    # else:
    #     p_published  = g_p_published.replace(" ","").split(",")
    #     p_published_left = p_published[0]
    #     p_published_right = p_published[1]
    
    # 输出
    p_result_panel = result_path_prefix + g_panel
    # %%
    # =========== 数据准备 测试用=============
    # 读取 market
    df_market = spark.read.parquet(p_market)
    df_markets = df_market.withColumnRenamed("标准通用名", "MOLECULE_STD") \
                            .withColumnRenamed("model", "MARKET") \
                            .withColumnRenamed("mkt", "MARKET")
    
    # 读取 df_universe
    '''
    df_universe = spark.read.parquet(p_universe)
    df_universe = df_universe.withColumnRenamed('Panel_ID', 'PHA') \
                        .withColumnRenamed('CITYGROUP', 'CITY_TIER') \
                        .withColumnRenamed('Province', 'PROVINCE') \
                        .withColumnRenamed('City', 'CITY') \
                        .withColumnRenamed('Hosp_name', 'HOSP_NAME') \
                        .withColumnRenamed('PANEL', 'PANEL') \
                        .withColumnRenamed('BEDSIZE', 'BEDSIZE') \
                        .withColumnRenamed('Seg', 'SEG')
    df_universe = df_universe.select('PHA', 'CITY_TIER', 'PROVINCE', 'CITY', 'HOSP_NAME', 'PANEL', 'BEDSIZE', 'SEG')
    df_universe = df_universe.withColumn('PANEL', col('PANEL').cast(IntegerType())) \
                        .withColumn('BEDSIZE', col('BEDSIZE').cast(IntegerType())) \
                        .withColumn('SEG', col('SEG').cast(IntegerType())) \
                        .withColumn('CITY_TIER', col('CITY_TIER').cast(StringType()))
    '''
    # %%
    # =========== 数据读取 =============
    # 1、读取 raw_data_adding_final
    df_raw_data_adding_final = spark.read.parquet(p_raw_data_adding_final)
    df_raw_data_adding_final = df_raw_data_adding_final.persist()
    
    # 2、读取 universe 数据
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
                REGION, TOTAL AS BEDSIZE, SEG, BID_SAMPLE AS PANEL FROM (
            SELECT 
                PHA_ID, HOSPITAL_ID, HOSP_NAME, 
                PROVINCE, CITY, CITYGROUP, 
                REGION, TAG, VALUE, SEG 
            FROM hospital_dimesion AS hdim 
                INNER JOIN hospital_fact AS hfct
                ON hdim.ID == hfct.HOSPITAL_ID WHERE (CATEGORY = 'BEDCAPACITY' AND TAG = 'TOTAL') OR (CATEGORY = 'IS' AND TAG = 'BID_SAMPLE')
        )
        PIVOT (
            SUM(VALUE)
            FOR TAG in ('TOTAL', 'BID_SAMPLE')
        )
    """
    
    df_universe = spark.sql(base_universe_sql)
    # %%
    # =========== 数据执行 =============
    df_markets = df_markets.select("MARKET", "MOLECULE_STD").distinct()
    df_universe = df_universe.select("PHA", "HOSP_NAME", "PROVINCE", "CITY").distinct()
    # %%
    # 生成 panel
    # S_Molecule -> MOLECULE_STD
    df_panel = df_raw_data_adding_final \
        .join(df_markets, on=["MOLECULE_STD"], how="left") \
        .drop("PROVINCE", "CITY") \
        .join(df_universe, on="PHA", how="left") \
        .withColumn("DATE", df_raw_data_adding_final.YEAR * 100 + df_raw_data_adding_final.MONTH)
    
    df_panel = df_panel \
        .groupBy("ID", "DATE", "MIN_STD", "MARKET", "HOSP_NAME", "PHA", "MOLECULE_STD", "PROVINCE", "CITY", "ADD_FLAG", "ROUTE_STD") \
        .agg(func.sum("SALES").alias("SALES"), func.sum("UNITS").alias("UNITS"))
    
    
    # 拆分 panel_raw_data， panel_add_data
    df_panel_raw_data = df_panel.where(df_panel.ADD_FLAG == 0)
    df_panel_raw_data.persist()
    
    df_panel_add_data = df_panel.where(df_panel.ADD_FLAG == 1)
    df_panel_add_data.persist()
    
    df_original_date_molecule = df_panel_raw_data.select("DATE", "MOLECULE_STD").distinct()
    df_original_date_min_std = df_panel_raw_data.select("DATE", "MIN_STD").distinct()
    df_panel_add_data = df_panel_add_data \
        .join(df_original_date_molecule, on=["DATE", "MOLECULE_STD"], how="inner") \
        .join(df_original_date_min_std, on=["DATE", "MIN_STD"], how="inner")
    # %%
    # 生成 panel_filtered
    # 早于model所用时间（历史数据），用new_hospital补数;
    # 处于model所用时间（模型数据），不补数；
    # 晚于model所用时间（月更新数据），用unpublished和not arrived补数
    # %%
    #### 月更新
    if g_project_name == "Sanofi" or g_project_name == "AZ":
        kct = [u'北京市', u'长春市', u'长沙市', u'常州市', u'成都市', u'重庆市', u'大连市', u'福厦泉市', u'广州市',
               u'贵阳市', u'杭州市', u'哈尔滨市', u'济南市', u'昆明市', u'兰州市', u'南昌市', u'南京市', u'南宁市', u'宁波市',
               u'珠三角市', u'青岛市', u'上海市', u'沈阳市', u'深圳市', u'石家庄市', u'苏州市', u'太原市', u'天津市', u'温州市',
               u'武汉市', u'乌鲁木齐市', u'无锡市', u'西安市', u'徐州市', u'郑州市', u'合肥市', u'呼和浩特市', u'福州市', u'厦门市',
               u'泉州市', u'珠海市', u'东莞市', u'佛山市', u'中山市']
        city_list = [u'北京市', u'上海市', u'天津市', u'重庆市', u'广州市', u'深圳市', u'西安市', u'大连市', u'成都市', u'厦门市', u'沈阳市']
        province_list = [u'河北省', u'福建省', u'河北', u"福建"]
        df_panel_add_data = df_panel_add_data \
            .where(~df_panel_add_data.CITY.isin(city_list)) \
            .where(~df_panel_add_data.PROVINCE.isin(province_list)) \
            .where(~(~(df_panel_add_data.CITY.isin(kct)) & (df_panel_add_data.MOLECULE_STD == u"奥希替尼")))
    else:
        city_list = [u'北京市', u'上海市', u'天津市', u'重庆市', u'广州市', u'深圳市', u'西安市', u'大连市', u'成都市', u'厦门市', u'沈阳市']
        Province_list = [u'河北省', u'福建省', u'河北', u"福建"]
                    
        # 去除 city_list和 Province_list
        if g_add_47 == "False":
            df_panel_add_data = df_panel_add_data \
                .where(~df_panel_add_data.CITY.isin(city_list)) \
                .where(~df_panel_add_data.PROVINCE.isin(province_list))
            
    
    # unpublished文件
    # unpublished 列表创建：published_left中有而published_right没有的ID列表，然后重复12次，时间为g_current_year*100 + i
    df_published_left = spark.read.csv(p_published_left, header=True)
    df_published_left = df_published_left.select('ID').distinct()
    
    df_published_right = spark.read.csv(p_published_right, header=True)
    df_published_right = df_published_right.select('ID').distinct()
    
    df_unpublished_id_list = df_published_left.subtract(df_published_right).toPandas()['ID'].values.tolist()
    unpublished_id_num = len(df_unpublished_id_list)
    all_month = list(range(1,13,1))*unpublished_id_num
    all_month.sort()
    
    unpublished_dict = {"ID":df_unpublished_id_list*12,"DATE":[ str(g_year*100 + i ) for i in all_month]}
    
    df = pd.DataFrame(data=unpublished_dict)
    df = df[["ID","DATE"]]
    schema = StructType([StructField("ID", StringType(), True), StructField("DATE", StringType(), True)])
    df_unpublished = spark.createDataFrame(df, schema)
    df_unpublished = df_unpublished.select("ID","DATE")
    
    # not_arrive文件
    df_notarrive = spark.read.csv(p_not_arrived, header=True)
    df_notarrive = df_notarrive.select("ID","DATE")
    
    # 合并df_unpublished和not_arrive文件
    df_notarrive_unpublished = df_unpublished.union(df_notarrive).distinct()
    
    df_future_range = df_notarrive_unpublished.withColumn("DATE", df_notarrive_unpublished["DATE"].cast(DoubleType()))
    df_panel_add_data_future = df_panel_add_data.where(df_panel_add_data.DATE > int(g_model_month_right)) \
        .join(df_future_range, on=["DATE", "ID"], how="inner") \
        .select(df_panel_raw_data.columns)
    
    df_panel_filtered = df_panel_raw_data.union(df_panel_add_data_future)
    
    df_panel_filtered.persist()

    # %%
    # =========== 输出 =============
    df_panel_filtered = df_panel_filtered.repartition(2)
    df_panel_filtered.write.format("parquet") \
        .mode("overwrite").save(p_result_panel)
    # %%
    # ############ 月更新结果比较
    # ## 原来月更新的job4 结果存储路径
    # p_result_month_old = "s3a://ph-max-auto/v0.0.1-2020-06-08/贝达/202012_test/panel_result/"
    
    # ### 读取原来的job4-month 的输出结果
    # df_result_month_old = spark.read.parquet(p_result_month_old)
    # df_result_month_old.select("Date").distinct().show()
    
    
    # print( df_result_month_old.schema )
    # # print( df_result_month_old.show(1, vertical=True))
    
    # df_result_month_202012 = df_result_month_old.where(  df_result_month_old.Date==202012.00  )
    # df_result_month_202012 = df_result_month_202012.withColumn("DATE", df_result_month_202012["Date"].cast(IntegerType()) )
    # df_result_month_202012 = df_result_month_202012.withColumnRenamed("Prod_Name", "MIN_STD" )\
    #                         .withColumnRenamed("Sales", "SalesOld")
    
    # df_result_month_202012.persist()
    # # df_result_month_202012.show(1, vertical=True) 
    # df_result_month_202012.count()
    
    # compare  = df_panel_filtered.join( df_result_month_202012, on=["DATE", "ID", "MIN_STD"], how="left" )
    # compare.persist()
    
    # print(compare.count() )
    # # compare.show(1, vertical=True)
    
    # compare_result = compare.withColumn("sales_error", compare["SALES"] - compare["SalesOld"])
    # # compare_result.where( compare_result.sales_error !=0.0).show(1, vertical=True)
    
    # compare_result.where( compare_result.sales_error !=0.0).select(["SALES", "SalesOld", "sales_error"] ).collect()

