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
    g_minimum_product_columns = kwargs['g_minimum_product_columns']
    g_minimum_product_sep = kwargs['g_minimum_product_sep']
    g_minimum_product_newname = kwargs['g_minimum_product_newname']
    g_if_two_source = kwargs['g_if_two_source']
    g_hospital_level = kwargs['g_hospital_level']
    g_bedsize = kwargs['g_bedsize']
    p_id_bedsize = kwargs['p_id_bedsize']
    g_monthly_update = kwargs['g_monthly_update']
    g_year = kwargs['g_year']
    g_month = kwargs['g_month']
    g_market = kwargs['g_market']
    depend_job_names_keys = kwargs['depend_job_names_keys']
    g_max_path = kwargs['g_max_path']
    g_base_path = kwargs['g_base_path']
    g_out_dir = kwargs['g_out_dir']
    ### input args ###
    
    ### output args ###
    g_max_result_city = kwargs['g_max_result_city']
    ### output args ###

    
    
    
    
    
    
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType,StructField
    from pyspark.sql import functions as func
    from pyspark.sql.functions import col
    import boto3
    import os           
    # %%
    
    
    # dag_name = 'Max'
    # run_id = 'max_test_beida_202012'
    # g_project_name = "贝达"
    # g_market = 'BD1'
    # g_time_left = "202012"
    # g_time_right = "202012"
    # g_out_dir = "202012_test"
    # g_if_two_source = "True"
    # result_path_prefix=get_result_path({"name":job_name, "dag_name":dag_name, "run_id":run_id})
    # depends_path=get_depends_path({"name":job_name, "dag_name":dag_name, 
    #                                      "run_id":run_id, "depend_job_names_keys":depend_job_names_keys })
    
    # # # g_monthly_update = 'False'
    # # # g_year = '2019'
    
    # g_monthly_update = 'True'
    # g_year = '2020'
    # g_month = '12'

    # %%
    # 输入
    g_minimum_product_columns = g_minimum_product_columns.replace(" ","").split(",")
    if g_minimum_product_sep == "kong":
        g_minimum_product_sep = ""
    
    if g_bedsize != "False" and g_bedsize != "True":
        raise ValueError('g_bedsize: False or True')
    if g_hospital_level != "False" and g_hospital_level != "True":
        raise ValueError('g_hospital_level: False or True')
    
    g_year = int(g_year)
    if g_monthly_update == 'True':
        g_month = int(g_month)
    
    out_path_dir = g_max_path + "/" + g_project_name + '/' + g_out_dir
    
    
    # 输入
    p_max_weight_result = depends_path['max_weight_result']
                
    # 输出
    p_max_result_city = result_path_prefix + g_max_result_city

    # %%
    
    # p_max_weight_result = p_max_weight_result.replace("s3:", "s3a:")
    # p_max_result_city = p_max_result_city.replace("s3:", "s3a:")
    # %%
    # 1. 创建临时表
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
            
    
    # rawdata_std        
    createView(g_project_name, "raw_data_std_fact", "FACT/RAW_DATA_STD_FACT", time = "2021-04-14")
    # rawdata
    createView(g_project_name, "raw_data_fact", "FACT/RAW_DATA_FACT", time = "2021-04-14")
    
    createView(g_project_name, "hospital_dimesion", "DIMENSION/HOSPITAL_DIMENSION", time = "2021-04-14")
    createView(g_project_name, "product_dimesion", "DIMENSION/PRODUCT_DIMENSION", time = "2021-04-14")
    createView(g_project_name, "mnf_dimesion", "DIMENSION/MNF_DIMENSION", time = "2021-04-14")
    createView(g_project_name, "product_rel_dimesion", "DIMENSION/PRODUCT_RELATIONSHIP_DIMENSION", time = "2021-04-14")
    #
    createView(g_project_name, "province_city_mapping", "DIMENSION/MAPPING/PROVINCE_CITY_MAPPING", time = "2021-04-14")
    createView(g_project_name, "mole_market_mapping", "DIMENSION/MAPPING/MARKET_MOLE_MAPPING", time = "2021-04-14") 
    createView(g_project_name, "cpa_pha_mapping", "DIMENSION/MAPPING/CPA_PHA_MAPPING", time = "2021-04-14")
    
    createView("PHARBERS", "fill_cpa_pha_mapping", "DIMENSION/MAPPING/CPA_GYC_MAPPING/BACKFILL", time = "2021-04-14") 
    createView("PHARBERS", "backfill_bedsize", "DIMENSION/MAPPING/BACKFILL_BEDSIZE", time = "2021-04-14")
    # %%
    # 2、df_product_map
    df_cpa_pha_mapping = spark.sql("SELECT * FROM cpa_pha_mapping").select('ID', 'PHA').distinct()
    # %%
    # 3、market_mapping
    mole_market_mapping_sql = """  SELECT * FROM mole_market_mapping """
    df_market =  spark.sql(mole_market_mapping_sql).withColumnRenamed("COMMON_NAME", "MOLECULE_STD")\
                        .select(["MARKET", "MOLECULE_STD"]).distinct()
    if df_market.select("MARKET").dtypes[0][1] == "double":
        df_market = df_market.withColumn("MARKET", col("MARKET").cast(IntegerType()))
    # %%
    # 4、province_city_mapping
    province_city_mapping_sql = """  SELECT * FROM province_city_mapping  """
    df_province_city_mapping = spark.sql(province_city_mapping_sql)
    df_province_city_mapping = df_province_city_mapping.select(["ID", "PROVINCE","CITY"]).distinct()
    # %%
    # 5、province_city_mapping
    fill_cpa_pha_mapping_sql = """
            SELECT * FROM fill_cpa_pha_mapping
        """
    df_cpa_pha_mapping_common = spark.sql(fill_cpa_pha_mapping_sql).select(["ID", "PHA"]).distinct()
    # %%
    # 6.ID_Bedsize
    backfill_bedsize_sql = """ SELECT * FROM backfill_bedsize """
    df_ID_Bedsize = spark.sql(backfill_bedsize_sql)
    df_ID_Bedsize = df_ID_Bedsize.select(["ID", "BEDSIZE"]).distinct()
    # %%
    # 7.读取rawdata_std数据
    raw_data_std_sql = """
            SELECT
                RW.RAW_CODE AS ID, HD.PANEL_ID AS PHA_ID, RW.RAW_PACK_ID AS PACK_ID, RW.RAW_MANUFACTURER AS MANUFACTURER_STD, RW.DATE,
                RW.RAW_MOLE_NAME AS MOLECULE_STD,  RW.RAW_PRODUCT_NAME AS BRAND_STD, 
                RW.RAW_PACK AS PACK_NUMBER_STD, RW.RAW_DOSAGE AS FORM_STD, RW.RAW_SPEC AS SPECIFICATIONS_STD,
                RW.SALES, RW.UNITS
            FROM raw_data_std_fact AS RW
                LEFT JOIN hospital_dimesion AS HD ON RW.HOSPITAL_ID == HD.ID
                LEFT JOIN product_dimesion AS PD ON RW.PRODUCT_ID == PD.ID
                LEFT JOIN mnf_dimesion AS MD ON PD.MNF_ID == MD.ID
                LEFT JOIN product_rel_dimesion AS PRM ON PD.PACK_ID == PRM.ID AND PRM.CATEGORY = 'IMS PACKID'
        """
    raw_data_sql = """
            SELECT
                RW.RAW_CODE AS ID, HD.PANEL_ID AS PHA_ID, RW.RAW_PACK_ID AS PACK_ID, RW.RAW_MANUFACTURER AS MANUFACTURER_STD, RW.DATE,
                RW.RAW_MOLE_NAME AS MOLECULE_STD,  RW.RAW_PRODUCT_NAME AS BRAND_STD, 
                RW.RAW_PACK AS PACK_NUMBER_STD, RW.RAW_DOSAGE AS FORM_STD, RW.RAW_SPEC AS SPECIFICATIONS_STD,
                RW.SALES, RW.UNITS
            FROM raw_data_fact AS RW
                LEFT JOIN hospital_dimesion AS HD ON RW.HOSPITAL_ID == HD.ID
                LEFT JOIN product_dimesion AS PD ON RW.PRODUCT_ID == PD.ID
                LEFT JOIN mnf_dimesion AS MD ON PD.MNF_ID == MD.ID
                LEFT JOIN product_rel_dimesion AS PRM ON PD.PACK_ID == PRM.ID AND PRM.CATEGORY = 'IMS PACKID'
        """
    
    
    #  选择raw_data数据来源
    if g_if_two_source == "True":
        df_raw_data = spark.sql(raw_data_std_sql)
    elif g_if_two_source == "False":
        df_raw_data = spark.sql(raw_data_sql)
    else:
        raise ValueError("请确定raw_data的正确来源")
    
    
    df_raw_data = df_raw_data.withColumn("MIN_STD", func.format_string("%s|%s|%s|%s|%s", "BRAND_STD","FORM_STD",
                                            "SPECIFICATIONS_STD", "PACK_NUMBER_STD", "MANUFACTURER_STD"))
    df_raw_data = df_raw_data.withColumn("SALES", col("SALES").cast("double"))\
                                    .withColumn("UNITS", col("UNITS").cast("double"))
    # %%
    # =========== 数据执行 =============
    '''
    合并raw_data 和 max 结果
    旧：双源读取_std文件重新匹配job12的信息，非双源读取job2结果不用重新匹配信息
    新：都统一读取raw，重新匹配
    '''
    # 一. raw文件处理
    # 获得 PHA
    df_raw_data = df_raw_data.join(df_cpa_pha_mapping, on="ID", how="left")
        
    ## 对raw_data日期进行控制
    if g_monthly_update == 'True':
        df_raw_data = df_raw_data.where(col("DATE")  == g_year * 100 + g_month)
    else:
        df_raw_data = df_raw_data.where((col("DATE") >= g_year * 100 + 1) & (col('DATE') <= g_year * 100 + 12))
    
    df_raw_data.persist()
    # %%
    # 2、匹配：市场名
    df_raw_data = df_raw_data.join(df_market, on="MOLECULE_STD", how="left")
    
    # 3、匹配:通用province_city
    df_raw_data = df_raw_data.join(df_province_city_mapping, on="ID", how="left") \
                            .withColumn("PANEL", func.lit(1))
    
    # 删除医院
    # hospital_ot = spark.read.csv(hospital_ot_path, header=True)
    # df_raw_data = df_raw_data.join(hospital_ot, on="ID", how="left_anti")
    
    # 4、匹配：通用cpa_pha_mapping_common  df_raw_data，当df_raw_data的PHA是空的重新匹配补充
    df_cpa_pha_mapping_common = df_cpa_pha_mapping_common.withColumnRenamed("PHA", "PHA_COMMON")
    df_raw_data = df_raw_data.join(df_cpa_pha_mapping_common, on="ID", how="left")
    df_raw_data = df_raw_data.withColumn("PHA", func.when(col('PHA').isNull(), col('PHA_COMMON')).otherwise(col('PHA'))) \
                                .drop("PHA_COMMON")
    
    # 5、df_raw_data 包含的医院列表
    df_raw_data_PHA = df_raw_data.select("PHA", "DATE").distinct()
    
    # 6、匹配：ID_Bedsize 
    df_raw_data = df_raw_data.join(df_ID_Bedsize, on="ID", how="left")
    
    # 7、筛选：all_models 
    df_raw_data = df_raw_data.where(col('MARKET') == g_market)
    
    # 8、筛选：ID_Bedsize
    if g_project_name != "Janssen":
        if g_bedsize == "True":
            df_raw_data = df_raw_data.where(col('BEDSIZE') > 99)
    
    # 9、计算：groupby        
    if g_hospital_level == "True":
        df_raw_data_city = df_raw_data \
            .groupBy("PROVINCE", "CITY", "DATE", "MIN_STD", "PANEL", "MARKET", "MOLECULE_STD", "PHA", "PACK_ID") \
            .agg({"SALES":"sum", "UNITS":"sum"}) \
            .withColumnRenamed("sum(SALES)", "PREDICT_SALES") \
            .withColumnRenamed("sum(UNITS)", "PREDICT_UNIT")
    else:
        df_raw_data_city = df_raw_data \
            .groupBy("PROVINCE", "CITY", "DATE", "MIN_STD", "PANEL", "MARKET", "MOLECULE_STD", "PACK_ID") \
            .agg({"SALES":"sum", "UNITS":"sum"}) \
            .withColumnRenamed("sum(SALES)", "PREDICT_SALES") \
            .withColumnRenamed("sum(UNITS)", "PREDICT_UNIT")

    # %%
    # 二. max文件处理
    #（以前是每个市场进行循环，目前只分析一个市场一个时间单位的数据）
    # df_max_result = spark.read.parquet(p_max_weight_result)
    strcut_type_max_weight_result = StructType([ StructField('PHA', StringType(), True),
                                                    StructField('PROVINCE', StringType(), True),
                                                    StructField('CITY', StringType(), True),
                                                    StructField('DATE', IntegerType(), True),
                                                    StructField('MOLECULE_STD', StringType(), True),
                                                    StructField('MIN_STD', StringType(), True),
                                                    StructField('PACK_ID', StringType(), True),
                                                    StructField('BEDSIZE', DoubleType(), True),
                                                    StructField('PANEL', DoubleType(), True),
                                                    StructField('SEG', DoubleType(), True),
                                                    StructField('PREDICT_SALES', DoubleType(), True),
                                                    StructField('PREDICT_UNIT', DoubleType(), True) ])
    df_max_result = spark.read.format("parquet").load(p_max_weight_result, schema=strcut_type_max_weight_result)
    
    # 日期控制
    if g_monthly_update == 'True':
        df_max_result = df_max_result.where(col('DATE') == (g_year*100 + g_month))
    elif g_monthly_update == 'False':
        df_max_result = df_max_result.where((col('DATE')/100).cast(IntegerType()) == g_year)
    
    
    # 1、max_result 筛选 BEDSIZE > 99， 且医院不在df_raw_data_PHA 中
    if g_bedsize == "True":
        df_max_result = df_max_result.where(col('BEDSIZE') > 99)
    
    # 2、max 结果中有panel=1和panel=0两部分数据，去掉的数据包括panel=1完全与raw一致的数据，还有panel=0被放大，但是df_raw_data中有真实值的数据
    df_max_result = df_max_result.join(df_raw_data_PHA, on=["PHA", "DATE"], how="left_anti")
    
    if g_hospital_level == "True":
        df_max_result = df_max_result \
            .groupBy("PROVINCE", "CITY", "DATE", "MIN_STD", "PANEL", "MOLECULE_STD", "PHA", 'PACK_ID') \
            .agg({"PREDICT_SALES":"sum", "PREDICT_UNIT":"sum"}) \
            .withColumnRenamed("sum(PREDICT_SALES)", "PREDICT_SALES") \
            .withColumnRenamed("sum(PREDICT_UNIT)", "PREDICT_UNIT")
    else:
        df_max_result = df_max_result \
            .groupBy("PROVINCE", "CITY", "DATE", "MIN_STD", "PANEL", "MOLECULE_STD", 'PACK_ID') \
            .agg({"PREDICT_SALES":"sum", "PREDICT_UNIT":"sum"}) \
            .withColumnRenamed("sum(PREDICT_SALES)", "PREDICT_SALES") \
            .withColumnRenamed("sum(PREDICT_UNIT)", "PREDICT_UNIT")
    
    df_max_result = df_max_result.withColumn("MARKET", func.lit(g_market))
    

    # %%
    # 三. 合并df_raw_data 和 max文件处理
    if g_hospital_level == "True":
        df_raw_data_city = df_raw_data_city.select("PHA", "PROVINCE", "CITY", "DATE", "MIN_STD", "MOLECULE_STD", "PANEL", "MARKET", 
                                                   "PREDICT_SALES", "PREDICT_UNIT", 'PACK_ID')
        max_result_all = df_max_result.select("PHA", "PROVINCE", "CITY", "DATE", "MIN_STD", "MOLECULE_STD", "PANEL", "MARKET", 
                                               "PREDICT_SALES", "PREDICT_UNIT", 'PACK_ID')
    else:
        df_raw_data_city = df_raw_data_city.select("PROVINCE", "CITY", "DATE", "MIN_STD", "MOLECULE_STD", "PANEL", "MARKET", 
                                                   "PREDICT_SALES", "PREDICT_UNIT", 'PACK_ID')
        max_result_all = df_max_result.select("PROVINCE", "CITY", "DATE", "MIN_STD", "MOLECULE_STD", "PANEL", "MARKET", 
                                               "PREDICT_SALES", "PREDICT_UNIT", 'PACK_ID')
    
    max_result_city = max_result_all.union(df_raw_data_city)
    
    # 四. 合并后再进行一次group
    if g_hospital_level == "True":
        max_result_city = max_result_city \
            .groupBy("PROVINCE", "CITY", "DATE", "MIN_STD", "PANEL", "MOLECULE", "PHA", "MARKET", 'PACK_ID') \
            .agg({"PREDICT_SALES":"sum", "PREDICT_UNIT":"sum"}) \
            .withColumnRenamed("sum(PREDICT_SALES)", "PREDICT_SALES") \
            .withColumnRenamed("sum(PREDICT_UNIT)", "PREDICT_UNIT")
        max_result_city = max_result_city.select("PHA", "PROVINCE", "CITY", "DATE", "MIN_STD", "MOLECULE_STD", "PANEL", "MARKET", 
                                                 "PREDICT_SALES", "PREDICT_UNIT", 'PACK_ID')
    else:
        max_result_city = max_result_city \
            .groupBy("PROVINCE", "CITY", "DATE", "MIN_STD", "PANEL", "MOLECULE_STD", "MARKET", 'PACK_ID') \
            .agg({"PREDICT_SALES":"sum", "PREDICT_UNIT":"sum"}) \
            .withColumnRenamed("sum(PREDICT_SALES)", "PREDICT_SALES") \
            .withColumnRenamed("sum(PREDICT_UNIT)", "PREDICT_UNIT")
        max_result_city = max_result_city.select("PROVINCE", "CITY", "DATE", "MIN_STD", "MOLECULE_STD", "PANEL", "MARKET", 
                                                 "PREDICT_SALES", "PREDICT_UNIT", 'PACK_ID')
        
    max_result_city = max_result_city.withColumnRenamed("MOLECULE_STD", "MOLECULE") \
                                    .withColumn("DATE", col("DATE").cast("int"))

    # %%
    # 输出
    max_result_city = max_result_city.repartition(1)
    max_result_city.write.format("parquet").partitionBy("DATE") \
                        .mode("append").save(p_max_result_city)

    # %%
    # df = spark.read.parquet('s3a://ph-max-auto/2020-08-11/Max/refactor/runs/max_test_beida_202012/max_city/max_city_result')
    # df.where(col('DATE') <202000).agg(func.sum('PREDICT_SALES'),func.sum('PREDICT_UNIT')).show()

    # %%
    # 月更
    df=spark.read.parquet('s3a://ph-max-auto/v0.0.1-2020-06-08/贝达/202012_test/MAX_result/MAX_result_202001_202012_city_level/')
    df = df.where(df.Date==202012)
    df.agg(func.sum('Predict_Sales'),func.sum('Predict_Unit')).show()
    
    max_result_city.agg(func.sum('Predict_Sales'),func.sum('Predict_Unit')).show()
    
    logger.debug(df.count(), max_result_city.count() )
    # %%
    # 模型
    # df=spark.read.parquet('s3a://ph-max-auto/v0.0.1-2020-06-08/贝达/201912_test/MAX_result/MAX_result_201701_201912_city_level')
    # df.where(col('Date')>=201901).where(col('Date')<=201912).agg(func.sum('Predict_Sales'), func.sum('Predict_Unit')).show()
    # 

