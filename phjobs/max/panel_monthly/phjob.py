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
    g_month = kwargs['g_month']
    g_current_month = kwargs['g_current_month']
    g_add_47 = kwargs['g_add_47']
    depend_job_names_keys = kwargs['depend_job_names_keys']
    g_monthly_update = kwargs['g_monthly_update']
    g_max_path = kwargs['g_max_path']
    g_base_path = kwargs['g_base_path']
    g_city_47 = kwargs['g_city_47']
    g_province_47 = kwargs['g_province_47']
    ### input args ###
    
    ### output args ###
    g_panel = kwargs['g_panel']
    ### output args ###

    
    
    
    from pyspark.sql.functions import col
    from pyspark.sql.types import IntegerType, StringType, StructType, StructField, DoubleType    
    from pyspark.sql import  functions as func
    import pandas as pd           
    
    # %%
    # 测试用的参数
    
    # dag_name = 'Max'
    # run_id = 'max_test_beida_202012'
    # g_project_name ="贝达"
    # g_model_month_right="201912"
    # g_year=2020
    # g_month=12
    # g_current_month="12"
    # g_add_47="True"
    # result_path_prefix=get_result_path({"name":job_name, "dag_name":dag_name, "run_id":run_id})
    # depends_path=get_depends_path({"name":job_name, "dag_name":dag_name, 
    #                                  "run_id":run_id, "depend_job_names_keys":depend_job_names_keys })

    # %%
    logger.debug('panel_monthly')
    # 是否运行此job
    if g_monthly_update == "False":
         return
        
    # 输入
    p_raw_data_adding_final = depends_path['raw_data_adding_final']
    
    # 月更新就没有 hostpital的数据
    if g_add_47 != "False" and g_add_47 != "True":
        logger.error('wrong input: g_add_47, False or True') 
        raise ValueError('wrong input: g_add_47, False or True')
    
    l_city = g_city_47.replace(' ','').split(',')
    l_province = g_province_47.replace(' ','').split(',')
    
    # 月更新相关输入
    # if monthly_update == "True":
    g_year = int(g_year)
    g_month = int(g_month)
    g_current_month = int(g_current_month)
    
    
    # 输出
    p_result_panel = result_path_prefix + g_panel

    # %%
    ### 
    # p_raw_data_adding_final =p_raw_data_adding_final.replace("s3:", "s3a:")
    # p_result_panel = p_result_panel.replace("s3:", "s3a:")
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
    
    createView(g_project_name, "mole_market_mapping", "DIMENSION/MAPPING/MARKET_MOLE_MAPPING", time = "2021-04-14")
    createView("PHARBERS", "publish_not_arrvier", "DIMENSION/MAPPING/PUBLISH_NOT_ARRIVE", time = "2021-04-14")
    
    createView(g_project_name, "hospital_dimesion", "DIMENSION/HOSPITAL_DIMENSION", time = "2021-04-14")
    createView(g_project_name, "hospital_fact", "FACT/HOSPITAL_FACT", time = "2021-04-14")
    createView(g_project_name, "hospital_base_seg_panel_mapping", "DIMENSION/MAPPING/HOSPITAL_BASE_MARKET_SEG_PANEL_MAPPING", time = "2021-04-14")
    # %%
    # =========== 数据准备 测试用=============
    # 读取 market
    mole_market_mapping_sql = """
        SELECT * FROM mole_market_mapping
    """
    df_markets =  spark.sql(mole_market_mapping_sql)
    df_markets = df_markets.withColumnRenamed("COMMON_NAME",  "MOLECULE_STD")\
                                    .select(["MOLECULE_STD", "MARKET" ]).distinct()

    # %%
    # =========== 数据读取 =============
    # 1、读取 raw_data_adding_final
    # df_raw_data_adding_final = spark.read.parquet(p_raw_data_adding_final)
    struct_type_data_adding_final = StructType([  StructField('PHA', StringType(), True),
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
    df_raw_data_adding_final = df_raw_data_adding_final.where(col('YEAR_MONTH') == (g_year*100 + g_month))
    
    # df_raw_data_adding_final.show(1)
    # %%
    # 2、读取 universe 数据
    
    
    base_universe_sql = """
        SELECT  PHA, HOSPITAL_ID, HOSP_NAME,
                PROVINCE, CITY, CITY_TIER,
                REGION, TOTAL AS BEDSIZE, SEG, PANEL,
                MEDICINE_RMB AS EST_DRUGINCOME_RMB
        FROM (
            SELECT
                hfct.PHA_ID AS PHA, hfct.HOSPITAL_ID,
                hdim.HOSP_NAME, hdim.PROVINCE, hdim.CITY, hdim.CITYGROUP AS CITY_TIER,
                hdim.REGION, hfct.TAG, hfct.VALUE
            FROM hospital_dimesion AS hdim
            INNER JOIN hospital_fact AS hfct ON hdim.ID == hfct.HOSPITAL_ID
            LEFT JOIN hospital_base_seg_panel_mapping AS hmsm ON hmsm.HOSPITAL_FACT_ID == hfct.ID
            WHERE
                (hfct.CATEGORY = 'BEDCAPACITY' AND hfct.TAG = 'TOTAL')
                OR
                (hfct.CATEGORY = 'REVENUE' AND hfct.TAG = 'MEDICINE_RMB')
                OR
                (hfct.CATEGORY = 'CPAGY' AND (hfct.TAG = 'SEG' OR hfct.TAG = 'PANEL'))
                AND
                (hmsm.CATEGORY = 'CPAGY' AND hmsm.TAG = 'BASE')
        )
        PIVOT (
            SUM(VALUE)
            FOR TAG in ('TOTAL', 'MEDICINE_RMB', 'SEG', 'PANEL')
        )
    """
    df_universe = spark.sql(base_universe_sql)
    df_universe = df_universe.select("PHA", "HOSP_NAME", "PROVINCE", "CITY").distinct()
    
    ## SQL 读太慢了
    # df_universe = spark.read.parquet("s3a://ph-max-auto/2020-08-11/Max/refactor/runs/max_test_beida_202012/temporary/universe_20210517")
    # df_universe.show(1)
    # %%
    # =========== 数据执行 =============
    # 生成 panel
    # S_Molecule -> MOLECULE_STD
    df_panel = df_raw_data_adding_final \
        .join(df_markets, on=["MOLECULE_STD"], how="left") \
        .drop("PROVINCE", "CITY") \
        .join(df_universe, on="PHA", how="left") \
        .withColumn("DATE", df_raw_data_adding_final.YEAR * 100 + df_raw_data_adding_final.MONTH)
    
    
    df_panel = df_panel \
        .groupBy("ID", "DATE", "PACK_ID", "MIN_STD","MARKET", "HOSP_NAME", "PHA", "MOLECULE_STD", "PROVINCE", "CITY", "ADD_FLAG" ) \
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
    # 取消Sanofi AZ 特殊处理（20210506）

    # %%
    published_not_arrvier_sql = """
            SELECT * FROM publish_not_arrvier
        """
    df_published_not_arrive= spark.sql(published_not_arrvier_sql)
    df_published_not_arrive = df_published_not_arrive.withColumn("YEAR", (df_published_not_arrive["DATE"].cast("int")/100).cast("int") )
    # not_arrived
    df_not_arrived = df_published_not_arrive.where( (col("TYPE")=="NOT_ARRIVED") &( col("YEAR")==g_year ) )
    # published
    df_published = df_published_not_arrive.where( col("TYPE")=="PUBLISHED" )
    df_published_right =  df_published.where( col("YEAR") ==g_year )
    df_published_left =  df_published.where( col("YEAR")==(g_year-1 ))
    
    
    # unpublished 列表创建：published_left中有而published_right没有的ID列表，然后重复12次，时间为g_current_year*100 + i
    df_published_left = df_published_left.select('ID').distinct()
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
    df_notarrive = df_not_arrived.select("ID","DATE")
    
    # 合并df_unpublished和not_arrive文件
    df_notarrive_unpublished = df_unpublished.union(df_notarrive).distinct()
    
    df_future_range = df_notarrive_unpublished.withColumn("DATE", df_notarrive_unpublished["DATE"].cast(IntegerType()))

    # %%
    #### 月更新
    # l_city = [u'北京市', u'上海市', u'天津市', u'重庆市', u'广州市', u'深圳市', u'西安市', u'大连市', u'成都市', u'厦门市', u'沈阳市']
    # l_province = [u'河北省', u'福建省', u'河北', u"福建"]
    
    # 去除 city_list和 Province_list
    if g_add_47 == "False":
        df_panel_add_data = df_panel_add_data \
            .where(~df_panel_add_data.CITY.isin(l_city)) \
            .where(~df_panel_add_data.PROVINCE.isin(l_province))
            
    df_panel_add_data_future = df_panel_add_data.where(df_panel_add_data.DATE > int(g_model_month_right)) \
        .join(df_future_range, on=["DATE", "ID"], how="inner") \
        .select(df_panel_raw_data.columns)
    
    df_panel_filtered = df_panel_raw_data.union(df_panel_add_data_future)
    
    # df_panel_filtered.persist()
    # %%
    # =========== 输出 =============
    df_panel_filtered = df_panel_filtered.repartition(1)
    df_panel_filtered.write.format("parquet").partitionBy("DATE") \
                        .mode("append").save(p_result_panel)
    

    # %%
    # ====== check ============
    # p_result_month_old = "s3a://ph-max-auto/v0.0.1-2020-06-08/贝达/202012_test/panel_result/"
    # df_result_month_old = spark.read.parquet(p_result_month_old)
    # check = df_result_month_old.where(  df_result_month_old.Date==202012.00  )
    # check.groupby("ADD_FLAG").agg(func.sum('Sales'), func.sum('Units') ).show()
    # df_panel_filtered.groupby("ADD_FLAG").agg(func.sum('Sales'), func.sum('UNITS') ).show()
    # print( check.count())
    # print( df_panel_filtered.count() )
    # %%
    # df_data_old = spark.read.parquet('s3a://ph-max-auto/2020-08-11/Max/refactor/runs/max_test_beida_202012_bk/panel_monthly/panel_result')
    # df_data_old.show(1, vertical=True)
    
    # ##### 比较差值
    # df_panel_filtered = df_panel_filtered.distinct()
    # df_panel_filtered.show(1, vertical=True)
    
    # df_data_old = spark.read.parquet("s3a://ph-max-auto/2020-08-11/Max/refactor/runs/max_test_beida_202012_bk/panel_monthly/panel_result")
    # df_data_old = df_data_old.withColumnRenamed("SALES", "SALES_OLD")\
    #                             .withColumnRenamed("UNITS", "UNITS_OLD").distinct()
    # # df_data_old.show(1,  vertical=True)
    
    # compare = df_panel_filtered.join( df_data_old, on=[ "ID", "MIN_STD", "PHA", "DATE",
    #                                     "MARKET", "HOSP_NAME", "PROVINCE", "CITY", "MOLECULE_STD" ] ,how="inner")
    # print(df_panel_filtered.count(), df_data_old.count(), compare.count() )
    
    # print( compare.withColumn("Error", compare["SALES"]- compare["SALES_OLD"] ).select("Error").distinct().collect() )
    # print( compare.withColumn("Error_2", compare["UNITS"]- compare["UNITS_OLD"] ).select("Error_2").distinct().collect() )
    
    # #### 找到匹配不到的
    # df_panel_filtered.join( df_data_old, on=[ "ID", "MIN_STD", "PHA", "DATE","MARKET",
    #                                                    "HOSP_NAME", "PROVINCE", "CITY", "MOLECULE_STD" ] ,how="anti").show()
    # print( df_panel_filtered.where(df_panel_filtered["ID"]==360161).count() )

    # %%
    # df_raw_data = df_panel_filtered
    # df_data_old = spark.read.parquet("s3a://ph-max-auto/2020-08-11/Max/refactor/runs/max_test_beida_202012_bk/panel_monthly/panel_result")
    # old_col_list = df_data_old.columns
    # raw_col_list = df_raw_data.columns
    # sam_col_list = list( set(old_col_list)&set(raw_col_list)   )
    
    # df_raw_data = df_raw_data.withColumn("SALES", col("SALES").cast("int")).withColumn("UNITS", col("UNITS").cast("int"))
    # df_data_old = df_data_old.withColumn("SALES", col("SALES").cast("int")).withColumn("UNITS", col("UNITS").cast("int"))
    # print(sam_col_list )
    # df_data_old_sam_col = df_data_old.select( sam_col_list  )
    # df_raw_data_sam_col = df_raw_data.select( sam_col_list )
    
    # subtract_result_1 =  df_data_old_sam_col.subtract( df_raw_data_sam_col )
    # subtract_result_1.show(1)
    # print("subtract-number:  ", subtract_result_1.count() )
    # subtract_result_2 =  df_raw_data_sam_col.subtract( df_data_old_sam_col )
    
    # subtract_result_2.show(2)
    # print("subtract-number:  ", subtract_result_2.count() )
    # %%
    # ############ 月更新结果比较
    # ## 原来月更新的job4 结果存储路径
    # p_result_month_old = "s3a://ph-max-auto/v0.0.1-2020-06-08/贝达/202012_test/panel_result/"
    
    # # ### 读取原来的job4-month 的输出结果
    # df_result_month_old = spark.read.parquet(p_result_month_old)
    # # df_result_month_old.select("Date").distinct().show()
    
    
    # # print( df_result_month_old.schema )
    # # # print( df_result_month_old.show(1, vertical=True))
    
    # df_result_month_202012 = df_result_month_old.where(  df_result_month_old.Date==202012.00  )
    # df_result_month_202012 = df_result_month_202012.withColumn("DATE", df_result_month_202012["Date"].cast(IntegerType()) )
    # df_result_month_202012 = df_result_month_202012.withColumnRenamed("Prod_Name", "MIN_STD" )\
    #                         .withColumnRenamed("Sales", "SalesOld")
    
    # # df_result_month_202012.persist()
    # # # df_result_month_202012.show(1, vertical=True) 
    # # df_result_month_202012.count()
    
    # compare  = df_panel_filtered.join( df_result_month_202012, on=["DATE", "ID", "MIN_STD"], how="inner" )
    # # compare.persist()
    
    
    # print(compare.count(), df_panel_filtered.count(), df_result_month_202012.count() )
    # compare_result = compare.withColumn("sales_error", compare["SALES"] - compare["SalesOld"])
    # print( compare_result.where( func.abs( compare_result.sales_error) >0.01 ).count() )
    
    # compare_result.where( compare_result.sales_error !=0.0).select(["SALES", "SalesOld", "sales_error"] ).collect()

