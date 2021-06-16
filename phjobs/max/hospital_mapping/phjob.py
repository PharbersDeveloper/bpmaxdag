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
    if_others = kwargs['if_others']
    g_out_dir = kwargs['g_out_dir']
    g_max_path = kwargs['g_max_path']
    g_base_path = kwargs['g_base_path']
    ### input args ###
    
    ### output args ###
    g_hospital_mapping_out = kwargs['g_hospital_mapping_out']
    ### output args ###

    
    
    
    
    
    import os
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col    
    
    # %%
    #测试用
    
    # g_project_name = '贝达'
    # g_out_dir = '202012'
    # dag_name = 'Max'
    # run_id = 'max_test_beida_202012'
    # result_path_prefix = get_result_path({"name":job_name, "dag_name":dag_name, "run_id":run_id})
    # depends_path = get_depends_path({"name":job_name, "dag_name":dag_name, 
    #                                   "run_id":run_id})

    # %%
    logger.debug('job1_hospital_mapping')
        
    # 输出    
    p_hospital_mapping_out = result_path_prefix + g_hospital_mapping_out

    # %%
    # jupter-测试使用
    # p_hospital_mapping_out = p_hospital_mapping_out.replace("s3:", "s3a:")
    # %%
    # =========== 数据准备 =============
    
    def createView(company, table_name, sub_path, other = "",
        time="2021-04-06", base_path = g_base_path):
             
            definite_path = "{base_path}/{sub_path}/TIME={time}/COMPANY={company}/{other}"
            path = definite_path.format(
                base_path = base_path,
                sub_path = sub_path,
                time = time,
                company = company,
                other = other
            )
            spark.read.parquet(path).createOrReplaceTempView(table_name)
    # raw_data
    createView(g_project_name, "raw_data_fact", "FACT/RAW_DATA_FACT", time = "2021-04-14")
    createView(g_project_name, "hospital_dimesion", "DIMENSION/HOSPITAL_DIMENSION", time = "2021-04-14")
    createView(g_project_name, "hospital_fact", "FACT/HOSPITAL_FACT", time = "2021-04-14")
    createView(g_project_name, "cpa_gyc_mapping", "DIMENSION/MAPPING/CPA_GYC_MAPPING/STANDARD", time = "2021-04-14")
    createView(g_project_name, "product_dimesion", "DIMENSION/PRODUCT_DIMENSION", time = "2021-04-14")
    createView(g_project_name, "mnf_dimesion", "DIMENSION/MNF_DIMENSION", time = "2021-04-14")
    createView(g_project_name, "product_rel_dimesion", "DIMENSION/PRODUCT_RELATIONSHIP_DIMENSION", time = "2021-04-14")
    
    # base_universe
    createView(g_project_name, "hospital_base_seg_panel_mapping", "DIMENSION/MAPPING/HOSPITAL_BASE_MARKET_SEG_PANEL_MAPPING", time = "2021-04-14")
    createView(g_project_name, "hospital_other_seg_mapping", "DIMENSION/MAPPING/HOSPITAL_OTHER_SEG_MAPPING", time = "2021-04-14")
    
    
    # cpa_pha_mapping
    createView(g_project_name, "cpa_pha_mapping", "DIMENSION/MAPPING/CPA_PHA_MAPPING", time = "2021-04-14")
    # %%
    # =========== 数据准备 测试用=============
    
    # 读取cpa_pha_mapping
    df_cpa_pha_mapping  = spark.sql("SELECT * FROM cpa_pha_mapping")
    df_cpa_pha_mapping = df_cpa_pha_mapping.select(["ID", "PHA"])

    # %%
    
    base_universe_sql = """
        SELECT  PHA, HOSPITAL_ID, HOSP_NAME,
                PROVINCE, CITY, CITY_TIER,
                REGION, TOTAL AS BEDSIZE, SEG, PANEL,
                MEDICINE_RMB AS EST_DRUGINCOME_RMB
        FROM (
            SELECT
                hfct.PANEL_ID AS PHA, hfct.HOSPITAL_ID,
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
    # %%
    
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
    
    if if_others == "True":
    #     raw_data_path = g_max_path + "/" + g_project_name + "/" + g_out_dir + "/raw_data_box"
    #     df_raw_data = spark.read.parquet(raw_data_path)
        raise ValueError("raw_data_box的读取方式还没确定")
    else:
        df_raw_data = spark.sql(raw_data_sql)
    
    df_raw_data = df_raw_data.withColumn('PACK_NUMBER_STD', col('PACK_NUMBER_STD').cast(IntegerType())) \
                        .withColumn('SALES', col('SALES').cast(DoubleType())) \
                        .withColumn('UNITS', col('UNITS').cast(DoubleType())) 
    
    # df_raw_data.select("MANUFACTURER_STD").distinct().show()
    # %%
    # =========== 数据执行 =============
    logger.debug('数据执行-start：hospital_mapping')
    # df_universe
    df_universe = df_universe.select('PHA', 'CITY', 'PROVINCE', 'CITY_TIER').distinct()
        
    # df_cpa_pha_mapping
    df_cpa_pha_mapping = df_cpa_pha_mapping.distinct()
                                    
    # df_raw_data 信息匹配与处理
    df_raw_data = df_raw_data.join(df_cpa_pha_mapping, on='ID', how='left') \
                        .join(df_universe, on='PHA', how='left')
    
    df_raw_data = df_raw_data.withColumn("MONTH", (col('DATE') % 100).cast(IntegerType())) \
                        .withColumn("YEAR", ((col('DATE') - col('MONTH')) / 100).cast(IntegerType()))
    
    df_raw_data = df_raw_data.withColumnRenamed("DATE", "YEAR_MONTH")

    # %%
    # 结果输出
    df_raw_data = df_raw_data.repartition(2)
    df_raw_data.write.format("parquet") \
        .mode("overwrite").save(p_hospital_mapping_out)
    
    logger.debug("输出 hospital_mapping 结果：" + p_hospital_mapping_out)
    logger.debug('数据执行-Finish')

    # %%
    # p_result_rawdata_standard ="s3a://ph-max-auto/v0.0.1-2020-06-08/贝达/test/贝达_rawdata_standard/"
    # df_result_rawdata_standard = spark.read.parquet( p_result_rawdata_standard )
    # # df_result_rawdata_standard.show(1, vertical=True)
    # df_result_rawdata_standard = df_result_rawdata_standard.where(df_result_rawdata_standard.Date==202012)
    # # df_raw_data_standard.select("Date").distinct().show()
    
    # a = ['DATE', 'ID',  'BRAND', 'FORM', 'SPECIFICATIONS', 'PACK_NUMBER', 'MANUFACTURER', 'MOLECULE', 'SALES', 'UNITS', 'UNITS_BOX', 'PHA', 'RAW_HOSP_NAME', 'PROVINCE', 'CITY', 'MIN', 'MARKET', 'MOLECULE_STD', 'BRAND_STD', 'FORM_STD', 'SPECIFICATIONS_STD', 'PACK_NUMBER_STD', 'MANUFACTURER_STD', 'PROVINCE_STD', 'CITY_STD', 'PACK_ID', 'ATC', 'PROJECT', 'DATE_COPY']
    # b = ['Date', 'ID',  'Brand', 'Form', 'Specifications', 'Pack_Number', 'Manufacturer', 'Molecule', 'Sales', 'Units', 'Units_Box', 'PHA', 'PHA医院名称', 'Province', 'City', 'min1', 'DOI', '标准通用名', '标准商品名', '标准剂型', '标准规格', '标准包装数量', '标准生产企业', '标准省份名称', '标准城市名称', 'PACK_ID', 'ATC', 'project', 'Date_copy']
    # df_result_rawdata_standard =  df_result_rawdata_standard.select([col(i).alias(j)   for i,j in zip(b,a)])
    
    
    # old.agg(func.sum("SALES"), func.sum("UNITS")).show()
    # new.agg(func.sum("SALES"), func.sum("UNITS")).show()

    # %% 
    # # 和最早的结果匹配
    
    # standard_result_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/贝达/201912_test/"
    
    # def compareHospitalResult():
        
    #     name = "hospital_mapping_out/"
    #     df_standard_hospital = spark.read.parquet(standard_result_path+name )
    #     df_standard_hospital = df_standard_hospital.select( [col(i).alias(i.upper())  for i in df_standard_hospital.columns])
    #     print("standard-sample-number: %s "%( df_standard_hospital.count() ) )
        
    #     df_new_hospital = df_raw_data
    #     print("new-sample-number: %s"%(  df_new_hospital.count() ) )
    #     df_standard_hospital.agg(func.sum('SALES'), func.sum('UNITS')  ).show()
    #     df_new_hospital.agg(func.sum('SALES'), func.sum('UNITS')  ).show()
    
    
    #     print( df_new_hospital.columns )
    #     print( )
    #     print(df_standard_hospital.columns)
    #     df_new_hospital.show(1, vertical=True)
    #     df_standard_hospital.show(1, vertical=True)
        
    # #     col_list = ["PHA", "ID", "PROVINCE", "CITY", "MONTH", "YEAR", ]
    # #     compare = df_new_hospital.join( df_standard_hospital, on=col_list, how="inner")
    # #     print( compare.count() )
        
        
    # compareHospitalResult()
    

    # %%
    
    # ## 和前几次的结果比较
    # df_raw_data = spark.read.parquet("s3a://ph-max-auto/2020-08-11/Max/refactor/runs/max_test_beida_202012/hospital_mapping/hospital_mapping_out")
    # df_data_old = spark.read.parquet("s3a://ph-max-auto/2020-08-11/Max/refactor/runs/max_test_beida_202012_bk/hospital_mapping/hospital_mapping_out/")
    # df_data_old = df_data_old.withColumn("YEAR_MONTH", col("YEAR_MONTH").cast("int")).distinct()
    # df_data_old = df_data_old.withColumn("UNITS", col("UNITS").cast("double"))\
    #                         .withColumn("SALES", col("SALES").cast("double"))
    
    # # print( df_data_old.select("DATE").distinct().count(), max_result_city.select("DATE").distinct().count() )
    
    # print("OLD: %s      NEW:%s "%(df_data_old.count(), df_raw_data.count() ))
    
    # ### join计算
    # # df_data_old = df_data_old.withColumnRenamed("SALES", "SALES_OLD")\
    # #                             .withColumnRenamed("UNITS", "UNITS_OLD")
    # # compare = df_raw_data.join( df_data_old, on=['PHA', 'ID', 'MANUFACTURER_STD', 'YEAR_MONTH', 'MOLECULE_STD', 
    # #                                                  'BRAND_STD', 'PACK_NUMBER_STD', 'FORM_STD', 'SPECIFICATIONS_STD', 
    # #                                                  'CITY', 'PROVINCE', 'CITY_TIER', 'MONTH', 'YEAR'],how="inner")
    # # print( compare.count())
    # # compare.show(1)
    
    
    # #### 用差集去计算
    # old_col_list = df_data_old.columns
    # raw_col_list = df_raw_data.columns
    # sam_col_list = list( set(old_col_list)&set(raw_col_list)   )
    # df_data_old = df_data_old.select( sam_col_list  )
    # df_raw_data = df_raw_data.select( sam_col_list )
    # # df_raw_data.show(1, vertical=True)
    # subtract_result_1 =  df_data_old.subtract( df_raw_data )
    # subtract_result_1.show()
    # subtract_result_2 =  df_raw_data.subtract( df_data_old )
    # subtract_result_2.show()

    # %%
    #### 匹配的上的有何差别
    # compare_error = compare.withColumn("Error", compare["SALES"]- compare["SALES_OLD"] )\
    #           .select("Error")
    # print(compare_error.distinct().count() )
    
    # print(  compare_error.where( func.abs( compare_error["Error"]) >0.01 ) \
    #           .count() )
    
    # print( compare.withColumn("Error_2", compare["UNITS"]- compare["UNITS_OLD"] ).select("Error_2").distinct().collect() )

    # %%
    # # 匹配不上的行是什么情况
    # df_data_old.join( df_raw_data, on=['PHA', 'ID', 'MANUFACTURER_STD', 'YEAR_MONTH', 'MOLECULE_STD', 
    #                                                  'BRAND_STD', 'PACK_NUMBER_STD', 'FORM_STD', 'SPECIFICATIONS_STD', 
    #                                                  'CITY', 'PROVINCE', 'CITY_TIER', 'MONTH', 'YEAR'],how="anti").show(2, vertical=True)

