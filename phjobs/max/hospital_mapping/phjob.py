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
    g_out_dir = kwargs['g_out_dir']
    max_path = kwargs['max_path']
    if_others = kwargs['if_others']
    out_path = kwargs['out_path']
    auto_max = kwargs['auto_max']
    need_test = kwargs['need_test']
    dag_name = kwargs['dag_name']
    run_id = kwargs['run_id']
    ### input args ###
    
    ### output args ###
    g_hospital_mapping_out = kwargs['g_hospital_mapping_out']
    ### output args ###

    import os
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col

    logger.debug('job1_hospital_mapping')
    # 输入
    if if_others != "False" and if_others != "True":
        logger.error('wrong input: if_others, False or True') 
        raise ValueError('wrong input: if_others, False or True')
        
    universe_path = max_path + "/" + g_project_name + "/universe_base"
    cpa_pha_mapping_path = max_path + "/" + g_project_name + "/cpa_pha_mapping"
    if if_others == "True":
        raw_data_path = max_path + "/" + g_project_name + "/" + g_out_dir + "/raw_data_box"
    else:
        raw_data_path = max_path + "/" + g_project_name + "/" + g_out_dir + "/raw_data"
        
    # 输出
    if if_others == "True":
        g_out_dir = g_out_dir + "/others_box/"
        
    p_hospital_mapping_out = result_path_prefix + g_hospital_mapping_out

    # =========== 数据准备 测试用=============
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
    df_universe

    def dealIDlength(df):
        # ID不足7位的补足0到6位
        # 国药诚信医院编码长度是7位数字，cpa医院编码是6位数字。
        df = df.withColumn("ID", df["ID"].cast(StringType()))
        # 去掉末尾的.0
        df = df.withColumn("ID", func.regexp_replace("ID", "\\.0", ""))
        df = df.withColumn("ID", func.when(func.length(df.ID) < 7, func.lpad(df.ID, 6, "0")).otherwise(df.ID))
        return df
        
    df_cpa_pha_mapping = spark.read.parquet(cpa_pha_mapping_path)
    df_cpa_pha_mapping = df_cpa_pha_mapping.withColumnRenamed('推荐版本', 'COMMEND')
    df_cpa_pha_mapping = df_cpa_pha_mapping.select('COMMEND', 'ID', 'PHA')
    df_cpa_pha_mapping = dealIDlength(df_cpa_pha_mapping)
    df_cpa_pha_mapping

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
    df_raw_data

    # =========== 数据执行 =============
    logger.debug('数据执行-start')
    # df_universe
    df_universe = df_universe.select('PHA', 'CITY', 'PROVINCE', 'CITY_TIER').distinct()
        
    # df_cpa_pha_mapping
    df_cpa_pha_mapping = df_cpa_pha_mapping.where(col('COMMEND') == 1).select("ID", "PHA").distinct()
                                    
    # df_raw_data
    df_raw_data = df_raw_data.join(df_cpa_pha_mapping, on='ID', how='left') \
                        .join(df_universe, on='PHA', how='left')
    
    df_raw_data = df_raw_data.withColumn("MONTH", (col('DATE') % 100).cast(IntegerType())) \
                        .withColumn("YEAR", ((col('DATE') - col('MONTH')) / 100).cast(IntegerType()))
    
    df_raw_data = df_raw_data.withColumnRenamed("DATE", "YEAR_MONTH")

    # 结果输出
    df_raw_data = df_raw_data.repartition(2)
    df_raw_data.write.format("parquet") \
        .mode("overwrite").save(p_hospital_mapping_out)
    
    logger.debug("输出 hospital_mapping 结果：" + p_hospital_mapping_out)
    logger.debug('数据执行-Finish')

