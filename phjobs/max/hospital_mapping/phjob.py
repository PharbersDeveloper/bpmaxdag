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
    dag_name = kwargs['dag_name']
    run_id = kwargs['run_id']
    max_path = kwargs['max_path']
    if_others = kwargs['if_others']
    g_out_dir = kwargs['g_out_dir']
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
    # result_path_prefix = get_result_path({"name":job_name, "dag_name":dag_name, "run_id":run_id})
    # depends_path = get_depends_path({"name":job_name, "dag_name":dag_name, 
    #                                   "run_id":run_id})

    # %%
    logger.debug('job1_hospital_mapping')
    # 输入
    # 测试用
    cpa_pha_mapping_path = max_path + "/" + g_project_name + "/cpa_pha_mapping"
    if if_others == "True":
        raw_data_path = max_path + "/" + g_project_name + "/" + g_out_dir + "/raw_data_box"
    else:
        raw_data_path = max_path + "/" + g_project_name + "/" + g_out_dir + "/raw_data"
        
    # 输出    
    p_hospital_mapping_out = result_path_prefix + g_hospital_mapping_out

    # %%
    # =========== 数据准备 测试用=============
    
    def dealIDlength(df):
        # ID不足7位的补足0到6位
        # 国药诚信医院编码长度是7位数字，cpa医院编码是6位数字。
        df = df.withColumn("ID", df["ID"].cast(StringType()))
        # 去掉末尾的.0
        df = df.withColumn("ID", func.regexp_replace("ID", "\\.0", ""))
        df = df.withColumn("ID", func.when(func.length(df.ID) < 7, func.lpad(df.ID, 6, "0")).otherwise(df.ID))
        return df
    
    # df_cpa_pha_mapping
    df_cpa_pha_mapping = spark.read.parquet(cpa_pha_mapping_path)
    df_cpa_pha_mapping = df_cpa_pha_mapping.withColumnRenamed('推荐版本', 'COMMEND')
    df_cpa_pha_mapping = df_cpa_pha_mapping.select('COMMEND', 'ID', 'PHA').where(col("COMMEND") == 1)
    df_cpa_pha_mapping = dealIDlength(df_cpa_pha_mapping)
    

    # %%
    # =========== 数据读取 =============
    
    
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
    # %%
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
    # %%
    
    raw_data_sql = """
            SELECT 
                RW.RAW_CODE AS ID, PRM.VALUE AS PACK_ID,  MD.MNF_NAME AS MANUFACTURER_STD , RW.DATE, 
                PD.MOLE_NAME AS MOLECULE_STD, PD.PROD_NAME_CH AS BRAND_STD, 
                PD.PACK AS PACK_NUMBER_STD, PD.DOSAGE AS FORM_STD, PD.SPEC AS SPECIFICATIONS_STD, 
                RW.SALES, RW.UNITS  
            FROM raw_data_fact AS RW
                LEFT JOIN hospital_dimesion AS HD ON RW.HOSPITAL_ID == HD.ID
                LEFT JOIN product_dimesion AS PD ON RW.PRODUCT_ID == PD.ID
                LEFT JOIN mnf_dimesion AS MD ON PD.MNF_ID == MD.ID
                LEFT JOIN product_rel_dimesion AS PRM ON PD.PACK_ID == PRM.ID
                WHERE PRM.CATEGORY = 'IMS PACKID'
        """
    
    df_raw_data = spark.sql(raw_data_sql)
    df_raw_data = df_raw_data.withColumn('PACK_NUMBER_STD', col('PACK_NUMBER_STD').cast(IntegerType())) \
                        .withColumn('SALES', col('SALES').cast(DoubleType())) \
                        .withColumn('UNITS', col('UNITS').cast(DoubleType())) 
    # %%
    # =========== 数据执行 =============
    logger.debug('数据执行-start：hospital_mapping')
    # df_universe
    df_universe = df_universe.select('PHA', 'CITY', 'PROVINCE', 'CITY_TIER').distinct()
        
    # df_cpa_pha_mapping
    df_cpa_pha_mapping = df_cpa_pha_mapping.select("ID", "PHA").distinct()
                                    
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
    # df = spark.read.parquet(cpa_pha_mapping_path)
    # df = df.withColumnRenamed('推荐版本', 'COMMEND')
    # df = df.select('COMMEND', 'ID', 'PHA').where(col("COMMEND") == 1)
    # df = dealIDlength(df)
    # tmp=df.join(df_cpa_pha_mapping.withColumnRenamed('PHA','PHA_new'), on='ID', how='left')
    # tmp.where(col('PHA') != col('PHA_new')).show()

    # %%
    # tmp.where(col('PHA_new').isNull()).show(100)
    # print( df_raw_data.columns )
    # %%
    
    # df_data_old = spark.read.parquet("s3a://ph-max-auto/2020-08-11/Max/refactor/runs/max_test_beida_202012_bk/hospital_mapping/hospital_mapping_out/")
    # df_data_old = df_data_old.withColumn("YEAR_MONTH", col("YEAR_MONTH").cast("int")).distinct()
    
    # # print( df_data_old.select("DATE").distinct().count(), max_result_city.select("DATE").distinct().count() )
    
    # print("OLD: %s      NEW:%s "%(df_data_old.count(), df_raw_data.count() ))
    
    # df_data_old = df_data_old.withColumnRenamed("SALES", "SALES_OLD")\
    #                             .withColumnRenamed("UNITS", "UNITS_OLD")
    
    # compare = df_raw_data.join( df_data_old, on=['PHA', 'ID', 'MANUFACTURER_STD', 'YEAR_MONTH', 'MOLECULE_STD', 
    #                                                  'BRAND_STD', 'PACK_NUMBER_STD', 'FORM_STD', 'SPECIFICATIONS_STD', 
    #                                                  'CITY', 'PROVINCE', 'CITY_TIER', 'MONTH', 'YEAR'],how="inner")
    
    # print( compare)
    # print( compare.count())
    # %%
    #### 匹配的上的有何差别
    # compare_error = compare.withColumn("Error", compare["SALES"]- compare["SALES_OLD"] )\
    #           .select("Error")
    # print(compare_error.distinct().count() )
    
    # print(  compare_error.where( func.abs( compare_error["Error"]) >0.01 ) \
    #           .count() )
    
    # print( compare.withColumn("Error_2", compare["UNITS"]- compare["UNITS_OLD"] ).select("Error_2").distinct().collect() )

    # %%
    # 匹配不上的行是什么情况
    # df_raw_data.join( df_data_old, on=['PHA', 'ID', 'MANUFACTURER_STD', 'YEAR_MONTH', 'MOLECULE_STD', 
    #                                                  'BRAND_STD', 'PACK_NUMBER_STD', 'FORM_STD', 'SPECIFICATIONS_STD', 
    #                                                  'CITY', 'PROVINCE', 'CITY_TIER', 'MONTH', 'YEAR'],how="anti").show(2, vertical=True)
