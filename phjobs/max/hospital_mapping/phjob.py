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
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col    # %%
    #测试用
    '''
    g_project_name = '贝达'
    g_out_dir = '202012'
    '''
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
    '''
    df_universe = spark.read.parquet(universe_path)
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
    
    
    # df_raw_data
    df_raw_data = spark.read.parquet(raw_data_path)
    df_raw_data = df_raw_data.withColumnRenamed('Form', 'FORM') \
                        .withColumnRenamed('Specifications', 'SPECIFICATIONS') \
                        .withColumnRenamed('Pack_Number', 'PACK_NUMBER') \
                        .withColumnRenamed('Manufacturer', 'MANUFACTURER') \
                        .withColumnRenamed('Molecule', 'MOLECULE') \
                        .withColumnRenamed('Source', 'SOURCE') \
                        .withColumnRenamed('Corp', 'CORP') \
                        .withColumnRenamed('Route', 'ROUTE') \
                        .withColumnRenamed('ORG_Measure', 'ORG_MEASURE') \
                        .withColumnRenamed('Sales', 'SALES') \
                        .withColumnRenamed('Units', 'UNITS') \
                        .withColumnRenamed('Units_Box', 'UNITS_BOX') \
                        .withColumnRenamed('Path', 'PATH') \
                        .withColumnRenamed('Sheet', 'SHEET') \
                        .withColumnRenamed('Raw_Hosp_Name', 'RAW_HOSP_NAME') \
                        .withColumnRenamed('Date', 'DATE') \
                        .withColumnRenamed('Brand', 'BRAND')
    df_raw_data = df_raw_data.withColumn('DATE', col('DATE').cast(IntegerType())) \
                        .withColumn('PACK_NUMBER', col('PACK_NUMBER').cast(IntegerType())) \
                        .withColumn('SALES', col('SALES').cast(DoubleType())) \
                        .withColumn('UNITS', col('UNITS').cast(DoubleType())) \
                        .withColumn('UNITS_BOX', col('UNITS_BOX').cast(DoubleType()))
    df_raw_data = dealIDlength(df_raw_data)
    # %%
    # =========== 数据读取 =============
    time = "2021-04-06"
    company = g_project_name
    base_path = "s3a://ph-max-auto/2020-08-11/data_matching/refactor/data/MAX"
    definite_path = "{base_path}/{model}/TIME={time}/COMPANY={company}"
    dim_path = definite_path.format(
        base_path = base_path,
        model = "DIMENSION/HOSPITAL_DIMENSION",
        time = time,
        company = company
    )
    fact_path = definite_path.format(
        base_path = base_path,
        model = "FACT/HOSPITAL_FACT",
        time = time,
        company = company
    )
    cpa_gyc_mapping_path = definite_path.format(
        base_path = base_path,
        model = "/DIMENSION/MAPPING/CPA_GYC_MAPPING/STANDARD",
        time = time,
        company = company
    )
    
    spark.read.parquet(dim_path).createOrReplaceTempView("hospital_dimesion")
    spark.read.parquet(fact_path).createOrReplaceTempView("hospital_fact")
    spark.read.parquet(cpa_gyc_mapping_path).createOrReplaceTempView("cpa_gyc_mapping")
    
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
    
    mapping_sql = """
            SELECT cgmap.VALUE AS ID, hdim.PANEL_ID AS PHA
            FROM cpa_gyc_mapping AS cgmap 
                INNER JOIN hospital_dimesion AS hdim 
                ON hdim.ID == cgmap.HOSPITAL_ID
        """
    df_universe = spark.sql(base_universe_sql)
    
    # df_cpa_pha_mapping = spark.sql(mapping_sql)
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
