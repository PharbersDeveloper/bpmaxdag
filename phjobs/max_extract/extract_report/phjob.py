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
    out_suffix = kwargs['out_suffix']
    data_type = kwargs['data_type']
    atc = kwargs['atc']
    molecule = kwargs['molecule']
    out_path = kwargs['out_path']
    run_id = kwargs['run_id']
    owner = kwargs['owner']
    g_input_version = kwargs['g_input_version']
    g_database_input = kwargs['g_database_input']
    g_database_result = kwargs['g_database_result']
    ### input args ###
    
    ### output args ###
    c = kwargs['c']
    d = kwargs['d']
    ### output args ###

    from pyspark.sql import SparkSession, Window
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    import os
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col
    import json
    import boto3
    import time
    
    # %%
    # time_left = "202001"
    # time_right = "202010"
    # project = "AZ"
    # doi = "Respules_Market, HTN_Market"
    # out_suffix = "test"
    # market_define = "AZ"
    
    # time_left = "201801"
    # time_right = "201912"
    # project = "Servier"
    # molecule = "二甲双胍, 格列喹酮"
    # out_suffix = "test_2"    # %%
    # a. 输入
    if data_type != "max" and data_type != "raw":
        phlogger.error('wrong input: data_type, max or raw') 
        raise ValueError('wrong input: data_type, max or raw')
    
    if data_type == 'max':
        g_extract_table = 'max_result'
    elif data_type == 'raw':
        g_extract_table = 'rawdata_standard'
    
    if out_suffix == "Empty":
        raise ValueError('out_suffix: missing')
    
    if atc == "Empty":
        atc = []
    else:
        atc = atc.replace(" ","").split(",")
        
    # b. 输出
    outdir = run_id + "_" + out_suffix
    
    out_extract_data_path = out_path + "/" + outdir + "/" + run_id + "_" + out_suffix + '.csv'
    report_b_path = out_path + "/" + outdir + "/report_ATC.csv"
    report_c_path = out_path + "/" + outdir + "/report_ATC_molecule.csv"
    report_d_path = out_path + "/" + outdir + "/report_molecule.csv"
    

    # %% 
    # 输入数据读取
    dict_input_version = json.loads(g_input_version)
    logger.debug(dict_input_version)
    
    df_ims_mapping =  spark.sql("SELECT * FROM %s.ims_flat_files WHERE provider='%s' AND version='%s'" 
                             %(g_database_input, 'common', dict_input_version['ims_mapping'])) \
                            .drop('owner', 'provider', 'version')
    
    df_ims_sales =  spark.sql("SELECT * FROM %s.ims_flat_files WHERE provider='%s' AND version='%s'" 
                             %(g_database_input, 'common', dict_input_version['cn_IMS_Sales_Fdata'])) \
                            .drop('owner', 'provider', 'version')
    
    df_product_map_all_atc =  spark.sql("SELECT * FROM %s.product_map_all_ATC WHERE provider='%s' AND version='%s'" 
                             %(g_database_input, 'common', dict_input_version['product_map_all_ATC'])) \
                            .drop('owner', 'provider', 'version')
    
    df_master_data_map =  spark.sql("SELECT * FROM %s.master_data_map WHERE provider='%s' AND version='%s'" 
                             %(g_database_input, 'common', dict_input_version['master_data_map'])) \
                            .drop('owner', 'provider', 'version')
    
    df_extract_data = spark.read.csv(out_extract_data_path, header=True)
    # %%
    # ================ 数据执行 ==================
    '''
    raw 和 max 区别：
    1. raw：Sales，Units 不处理，就用原始的数据
    2. report A 去重依据：
        max：月份数-项目排名
        raw: 月份数-医院数-项目排名；增加了source来源统计
    3. 提数结果
        raw：增加了医院ID，原始医院名，PHA，PHA医院名
    '''
    
    # 一. 文件准备
    
    # 1. 满足率文件准备
    # 通用名中英文对照
    df_molecule_ACT = df_product_map_all_atc.select("MOLE_NAME_EN", "MOLE_NAME_CH").distinct()
    df_packID_ACT_map = df_master_data_map.select("MOLE_NAME_EN", "MOLE_NAME_CH").distinct()
    df_molecule_name_map = df_molecule_ACT.union(df_packID_ACT_map).distinct()
    
    # ims mapping:ATC - Molecule - Pack_Id
    df_ims_mapping = df_ims_mapping.select("Pack_Id0", "ATC4_Code", "Molecule_Composition").distinct() \
                        .withColumn("Pack_Id0", col('Pack_Id0').cast(IntegerType())) \
                        .withColumnRenamed("Pack_Id0", "PACK_ID") \
                        .withColumnRenamed("ATC4_Code", "ATC") \
                        .withColumnRenamed("Molecule_Composition", "MOLE_NAME_EN")
    
    # 2019年全国的ims销售数据
    df_ims_sales = df_ims_sales.where(func.substring(col('Period_Code'), 0, 4) == '2019') \
                        .where(col('Geography_id') == 'CHT') \
                        .groupby("Pack_ID").agg(func.sum("LC").alias("Sales_ims")) \
                        .withColumn("Pack_ID", col('Pack_ID').cast(IntegerType())) \
                        .withColumnRenamed("Pack_ID", "PACK_ID")
    
    df_ims_sales = df_ims_sales.join(df_ims_mapping, on="Pack_ID", how="left")
    df_ims_sales = df_ims_sales.join(df_molecule_name_map, on="MOLE_NAME_EN", how="left").distinct()
    
    if atc and max([len(i) for i in atc]) == 3:
        df_ims_sales = df_ims_sales.withColumn("ATC", func.substring(col('ATC'), 0, 3)).distinct()
    elif atc and max([len(i) for i in atc]) == 4:
        df_ims_sales = df_ims_sales.withColumn("ATC", func.substring(col('ATC'), 0, 4)).distinct()
    

    # %%
    # report_c
    df_months_num = df_extract_data.select("project","project_score","标准通用名", "ATC", "Date").distinct() \
                            .groupby(["标准通用名", "ATC", "project"]).count() \
                            .withColumnRenamed("count", "months_num") \
                            .persist()
    
    df_time_range = df_extract_data.select("project","标准通用名", "ATC", "Date").distinct() \
                                .groupby(["project","标准通用名", "ATC"]).agg(func.min("Date").alias("min_time"), func.max("Date").alias("max_time"))
    df_time_range = df_time_range.withColumn("time_range", func.concat(col('min_time'), func.lit("_"), col('max_time'))) \
                            .drop('min_time', 'max_time')
    
    df_extract_sales = df_extract_data.select("project", "PACK_ID", "ATC", "标准通用名").distinct() \
                                    .join(df_ims_sales.select("PACK_ID", "Sales_ims").distinct(), on="PACK_ID", how="left") \
                                    .groupby("project", "ATC", "标准通用名").agg(func.sum("Sales_ims").alias("Sales_ims_extract")).persist()
    
    l_molecule_names = df_extract_data.select("标准通用名").distinct().toPandas()["标准通用名"].values.tolist()
    
    df_molecule_sales = df_ims_sales.select("PACK_ID", "ATC", "MOLE_NAME_CH", "Sales_ims").distinct() \
                                    .where(col('MOLE_NAME_CH').isin(l_molecule_names)) \
                                    .groupby("ATC", "MOLE_NAME_CH").agg(func.sum("Sales_ims").alias("Sales_ims_molecule")) \
                                    .withColumnRenamed("MOLE_NAME_CH", "标准通用名").persist()
    
    report_c = df_extract_sales.join(df_molecule_sales, on=["标准通用名", "ATC"], how="left")
    report_c = report_c.withColumn("Sales_rate", col('Sales_ims_extract')/col('Sales_ims_molecule')) \
                        .join(df_months_num, on=["标准通用名", "ATC", "project"], how="left") \
                        .join(df_time_range, on=["标准通用名", "ATC", "project"], how="left") \
                        .drop("Sales_ims_extract", "Sales_ims_molecule").persist()
    # 列名顺序调整
    report_c = report_c.select("project", "ATC", "标准通用名", "time_range", "months_num", "Sales_rate") \
                        .orderBy(["标准通用名"]) 
    
    report_c = report_c.repartition(1)
    report_c.write.format("csv").option("header", "true") \
        .mode("overwrite").save(report_c_path)
    # %%
    # report_atc
    if atc:
        df_time_range_act = df_extract_data.select("ATC", "Date").distinct() \
                                            .groupby(["ATC"]).agg(func.min("Date").alias("min_time"), func.max("Date").alias("max_time"))
        df_time_range_act = df_time_range_act.withColumn("time_range", func.concat(col('min_time'), func.lit("_"), col('max_time')))
    
        df_extract_sales = df_extract_data.select("PACK_ID", "ATC").distinct() \
                                        .join(df_ims_sales.select("PACK_ID", "Sales_ims").distinct(), on="PACK_ID", how="left") \
                                        .groupby("ATC").agg(func.sum("Sales_ims").alias("Sales_ims_extract")).persist()
        df_atc_sales = df_ims_sales.select("PACK_ID", "ATC", "Sales_ims").distinct() \
                                    .where(df_ims_sales.ATC.isin(atc)) \
                                    .groupby("ATC").agg(func.sum("Sales_ims").alias("Sales_ims_atc")).persist()
        report_b = df_extract_sales.join(df_atc_sales, on="ATC", how="left")
        report_b = report_b.withColumn("Sales_rate", col('Sales_ims_extract')/col('Sales_ims_atc')) \
                            .join(df_time_range_act.select("ATC", "time_range"), on="ATC", how="left") \
                            .drop("Sales_ims_extract", "Sales_ims_atc")
        # 列名顺序调整
        report_b = report_b.select("ATC", "time_range", "Sales_rate")
    
        report_b = report_b.repartition(1)
        report_b.write.format("csv").option("header", "true") \
            .mode("overwrite").save(report_b_path)
    # %%
    # report_molecule        
    if molecule:
        df_time_range = df_extract_data.select("标准通用名", "Date").distinct() \
                                        .groupby(["标准通用名"]).agg(func.min("Date").alias("min_time"), func.max("Date").alias("max_time"))
        df_time_range = df_time_range.withColumn("time_range", func.concat(col('min_time'), func.lit("_"), col('max_time')))
    
        df_extract_sales = df_extract_data.select("PACK_ID", "标准通用名").distinct() \
                                        .join(df_ims_sales.select("PACK_ID", "Sales_ims").distinct(), on="PACK_ID", how="left") \
                                        .groupby("标准通用名").agg(func.sum("Sales_ims").alias("Sales_ims_extract")).persist()
        df_molecule_sales = df_ims_sales.select("PACK_ID", "MOLE_NAME_CH", "Sales_ims").distinct() \
                                        .where(df_ims_sales.MOLE_NAME_CH.isin(molecule)) \
                                        .groupby("MOLE_NAME_CH").agg(func.sum("Sales_ims").alias("Sales_ims_molecule")).persist()
        report_d = df_extract_sales.join(df_molecule_sales, df_extract_sales['标准通用名']==df_molecule_sales['MOLE_NAME_CH'], how="left")
        report_d = report_d.withColumn("Sales_rate", col('Sales_ims_extract')/col('Sales_ims_molecule')) \
                            .join(df_time_range.select("标准通用名", "time_range"), on="标准通用名", how="left") \
                            .drop("Sales_ims_extract", "Sales_ims_molecule", "MOLE_NAME_CH")
        # 列名顺序调整
        report_d = report_d.select("标准通用名", "time_range", "Sales_rate")
    
        report_d = report_d.repartition(1)
        report_d.write.format("csv").option("header", "true") \
            .mode("overwrite").save(report_d_path)
