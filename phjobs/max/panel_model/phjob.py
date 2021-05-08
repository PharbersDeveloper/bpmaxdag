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
    g_model_month_left = kwargs['g_model_month_left']
    g_model_month_right = kwargs['g_model_month_right']
    g_year = kwargs['g_year']
    g_add_47 = kwargs['g_add_47']
    depend_job_names_keys = kwargs['depend_job_names_keys']
    g_monthly_update = kwargs['g_monthly_update']
    dag_name = kwargs['dag_name']
    run_id = kwargs['run_id']
    max_path = kwargs['max_path']
    g_city_47 = kwargs['g_city_47']
    g_province_47 = kwargs['g_province_47']
    ### input args ###
    
    ### output args ###
    g_panel = kwargs['g_panel']
    ### output args ###

    
    
    from pyspark.sql.functions import col
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType, StructField
    from pyspark.sql import  functions as func    
    # %%
    # 测试用的参数s
    
    # g_project_name ="贝达"
    # g_model_month_left="201901"
    # g_model_month_right="201912"
    # g_add_47="True" 
    # g_year=2019
    # result_path_prefix=get_result_path({"name":job_name, "dag_name":dag_name, "run_id":run_id})
    # depends_path=get_depends_path({"name":job_name, "dag_name":dag_name, 
    #                                  "run_id":run_id, "depend_job_names_keys":depend_job_names_keys })

    # %%
    # 是否运行此job
    if g_monthly_update == "True":
        return
    
    
    logger.debug('panel_model')
    # 输入
    # p_universe = max_path + "/" + g_project_name + "/universe_base"
    p_market  = max_path + "/" + g_project_name + "/mkt_mapping"
    p_raw_data_adding_final = depends_path['raw_data_adding_final']
    p_new_hospital = depends_path['new_hospital']
    
    if g_add_47 != "False" and g_add_47 != "True":
        logger.error('wrong input: g_add_47, False or True') 
        raise ValueError('wrong input: g_add_47, False or True')
    
    g_year = int(g_year)    
    l_city = g_city_47.replace(' ','').split(',')
    l_province = g_province_47.replace(' ','').split(',')
        
    # 月更新相关输入
    # monthly_update == "False":
            
    # 输出
    p_panel_result = result_path_prefix + g_panel

    # %%
    # print( p_panel_result)
    # print(p_new_hospital)

    # %%
    # =========== 数据准备 测试用=============
    # 读取 market
    df_market = spark.read.parquet(p_market)
    df_markets = df_market.withColumnRenamed("标准通用名", "MOLECULE_STD") \
                            .withColumnRenamed("model", "MARKET") \
                            .withColumnRenamed("mkt", "MARKET")

    # %%
    # =========== 数据读取 =============
    # 1、读取 raw_data_adding_final
    # df_raw_data_adding_final = spark.read.parquet(p_raw_data_adding_final)
    struct_type_data_adding_final = StructType([ StructField('PHA', StringType(), True),
                                                    StructField('ID', StringType(), True),
                                                    StructField('PACK_ID', StringType(), True),
                                                    StructField('MANUFACTURER_STD', StringType(), True),
                                                    StructField('YEAR_MONTH', IntegerType(), True),
                                                    StructField('MOLECULE_STD', StringType(), True),
                                                    StructField('BRAND_STD', StringType(), True),
                                                    StructField('PACK_NUMBER_STD', IntegerType(), True),
                                                    StructField('FORM_STD', StringType(), True),
                                                    StructField('SPECIFICATIONS_STD', StringType(), True),
                                                    StructField('SALES', DoubleType(), True),
                                                    StructField('UNITS', DoubleType(), True),
                                                    StructField('CITY', StringType(), True),
                                                    StructField('PROVINCE', StringType(), True),
                                                    StructField('CITY_TIER', DoubleType(), True),
                                                    StructField('MONTH', IntegerType(), True),
                                                    StructField('YEAR', IntegerType(), True),
                                                    StructField('MIN_STD', StringType(), True),
                                                    StructField('MOLECULE_STD_FOR_GR', StringType(), True),
                                                    StructField('ADD_FLAG', IntegerType(), True) ])
    df_raw_data_adding_final = spark.read.format("parquet").load(p_raw_data_adding_final, schema=struct_type_data_adding_final)
    df_raw_data_adding_final = df_raw_data_adding_final.where((col('YEAR_MONTH')/100).cast(IntegerType()) == g_year)
    
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
            SELECT PANEL_ID AS PHA, HOSPITAL_ID, HOSP_NAME, 
                PROVINCE, CITY, CITYGROUP AS CITY_TIER, 
                REGION, TOTAL AS BEDSIZE, SEG, BID_SAMPLE AS PANEL FROM (
            SELECT 
                hdim.PANEL_ID, HOSPITAL_ID, HOSP_NAME, 
                PROVINCE, CITY, CITYGROUP, 
                REGION, TAG, VALUE, SEG 
            FROM hospital_dimesion AS hdim 
                INNER JOIN hospital_fact AS hfct
                ON hdim.ID == hfct.HOSPITAL_ID
            WHERE (CATEGORY = 'BEDCAPACITY' AND TAG = 'TOTAL') OR (CATEGORY = 'IS' AND TAG = 'BID_SAMPLE')
            )
            PIVOT (
                SUM(VALUE)
                FOR TAG in ('TOTAL', 'BID_SAMPLE')
            )
    """
    
    df_universe = spark.sql(base_universe_sql)
    ## SQL 读太慢了
    # df_universe = spark.read.parquet("s3a://ph-max-auto/2020-08-11/Max/refactor/runs/max_test_beida_202012/temporary/universe")
    # df_universe = spark.read.parquet("s3a://ph-max-auto/2020-08-11/Max/refactor/runs/max_test_beida_202012/temporary/universe_PHA_ID")

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
    
    # df_panel = df_panel \
    #     .groupBy("ID", "DATE", "MIN_STD", "MARKET", "HOSP_NAME", "PHA", "MOLECULE_STD", "PROVINCE", "CITY", "ADD_FLAG", "ROUTE_STD") \
    #     .agg(func.sum("SALES").alias("SALES"), func.sum("UNITS").alias("UNITS"))
    
    ###################################################### 新的表中没有 ROUTE_STD 这一列
    df_panel = df_panel \
        .groupBy("ID", "DATE", "PACK_ID","MIN_STD", "MARKET", "HOSP_NAME", "PHA", "MOLECULE_STD", "PROVINCE", "CITY", "ADD_FLAG" ) \
        .agg(func.sum("SALES").alias("SALES"), func.sum("UNITS").alias("UNITS"))
    ##################################################### 
    
    # 拆分 panel_raw_data， panel_add_data
    df_panel_raw_data = df_panel.where(df_panel.ADD_FLAG == 0)
    df_panel_raw_data.persist()
    
    df_panel_add_data = df_panel.where(df_panel.ADD_FLAG == 1)
    df_panel_add_data.persist()
    
    df_original_date_molecule = df_panel_raw_data.select("DATE", "MOLECULE_STD").distinct()
    df_original_date_min_std = df_panel_raw_data.select("DATE", "MIN_STD").distinct()
    df_panel_add_data = df_panel_add_data \
        .join(df_original_date_molecule, on=["DATE", "MOLECULE_STD"], how="inner") \
        .join(df_original_date_min_std, on=["DATE", "MIN_STD"], how="inner").persist()

    # %%
    # 生成 panel_filtered
    # 早于model所用时间（历史数据），用new_hospital补数;
    # 处于model所用时间（模型数据），不补数；
    # 晚于model所用时间（月更新数据），用unpublished和not arrived补数
    # 取消Sanofi AZ 特殊处理（20210506）

    # %%
    ####  模型
    df_new_hospital = spark.read.parquet(p_new_hospital)
    df_new_hospital = df_new_hospital.toPandas()["PHA"].tolist()
    
    # l_city = [u'北京市', u'上海市', u'天津市', u'重庆市', u'广州市', u'深圳市', u'西安市', u'大连市', u'成都市', u'厦门市', u'沈阳市']
    # l_province = [u'河北省', u'福建省', u'河北', u"福建"]
    
    if g_project_name == u"贝达" or g_project_name == "Sanofi" or g_project_name == "AZ":
        df_panel_add_data = df_panel_add_data.where(df_panel_add_data.MOLECULE_STD != u"奥希替尼")
    
    # 去除 l_city和 l_province
    if g_add_47 == "False":
        df_panel_add_data = df_panel_add_data \
            .where( df_panel_add_data.CITY.isin(l_city)) \
            .where( df_panel_add_data.PROVINCE.isin(l_province))
    
    df_panel_add_data_history = df_panel_add_data \
        .where(df_panel_add_data.PHA.isin(df_new_hospital)) \
        .where(df_panel_add_data.DATE < int(g_model_month_left)) \
        .select(df_panel_raw_data.columns)
    df_panel_filtered = df_panel_raw_data.union(df_panel_add_data_history)

    # %%
    ##### 输出保存的结果
    df_panel_filtered = df_panel_filtered.repartition(1)
    df_panel_filtered.write.format("parquet").partitionBy("DATE") \
                        .mode("append").save(p_result_panel)

    # %%
    # df_panel_filtered.select("DATE").distinct().show()
    
    
    # df_panel_filtered = df_panel_filtered.distinct()
    # df_panel_filtered.show(1, vertical=True)
    
    # df_data_old = spark.read.parquet("s3a://ph-max-auto/2020-08-11/Max/refactor/runs/max_test_beida_202012_bk/panel_model/panel_result")
    # df_data_old = df_data_old.withColumnRenamed("SALES", "SALES_OLD")\
    #                             .withColumnRenamed("UNITS", "UNITS_OLD").distinct()
    # # # df_data_old.show(1,  vertical=True)
    
    # compare = df_panel_filtered.join( df_data_old, on=[ "ID", "MIN_STD", "PHA", "DATE","MARKET", 
    #             "HOSP_NAME", "PROVINCE", "CITY", "MOLECULE_STD" ] ,how="inner")
    # print(df_panel_filtered.count(), df_data_old.count(), compare.count() )
    
    # compare.withColumn("Error", compare["SALES"]- compare["SALES_OLD"] ).select("Error").distinct().collect()
    # compare.withColumn("Error_2", compare["UNITS"]- compare["UNITS_OLD"] ).select("Error_2").distinct().collect()

    # %%
    ##### 检查更新 Code 后 和原来的数据结果是否一置
    
    
    # ######################################################   读取原来的job4-model的输出结果
    # p_result_model_old = "s3a://ph-max-auto/v0.0.1-2020-06-08/贝达/201912_test/panel_result/"
    # df_result_model_old = spark.read.parquet(p_result_model_old)
    # # print(df_result_model_old.columns)
    
    # df_result_model_old.select("Date").distinct().show()
    
    # df_result_model_2019 =  df_result_model_old.where( ( df_result_model_old.Date>=201900.00) & (df_result_model_old.Date<=202000.00) )
    # df_result_model_2019 = df_result_model_2019.withColumn("DATE", df_result_model_2019["Date"].cast(IntegerType()) )
    # df_result_model_2019 = df_result_model_2019.withColumnRenamed("Prod_Name", "MIN_STD" )\
    #                         .withColumnRenamed("Sales", "SalesOld")
    
    
    
    # compare  = df_panel_filtered.join( df_result_model_2019, on=["DATE", "ID", "MIN_STD"], how="left" )
    
    # print( df_panel_filtered.count(), df_result_model_2019.count(),  compare.count() )
    
    # compare_result = compare.withColumn("sales_error", compare["SALES"] - compare["SalesOld"])
    # compare_result.where( func.abs( compare_result.sales_error)>0.01 ).count()
    
    # # errow =  compare.select("SALES").exceptAll(compare.select("SalesOld") )

