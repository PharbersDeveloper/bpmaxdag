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
    g_need_cleaning_cols = kwargs['g_need_cleaning_cols']
    depend_job_names_keys = kwargs['depend_job_names_keys']
    dag_name = kwargs['dag_name']
    run_id = kwargs['run_id']
    max_path = kwargs['max_path']
    g_out_dir = kwargs['g_out_dir']
    ### input args ###
    
    ### output args ###
    g_product_mapping_out = kwargs['g_product_mapping_out']
    g_need_cleaning_out = kwargs['g_need_cleaning_out']
    ### output args ###

    import os
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col    
    # %%
    # 测试
    '''
    g_project_name = '贝达'
    g_out_dir = '202012_test'
    g_minimum_product_sep='|'
    '''

    # %%
    logger.debug('job2_product_mapping')
    # 注意：
    # Mylan不做Brand判断，写死了
    # Mylan不重新生成g_minimum_product_newname: MIN
    
    # 输入
    p_hospital_mapping_out = depends_path['hospital_mapping_out']
    g_need_cleaning_cols = g_need_cleaning_cols.replace(" ","").split(",")
    g_minimum_product_columns = g_minimum_product_columns.replace(" ","").split(",")
    if g_minimum_product_sep == "kong":
        g_minimum_product_sep = ""
    
    # 测试用
    product_map_path = max_path + "/" + g_project_name + '/' + g_out_dir + "/prod_mapping"
    
    # 输出
    p_product_mapping_out = result_path_prefix + g_product_mapping_out
    p_need_cleaning = result_path_prefix + g_need_cleaning_out

    # %%
    # =========== 数据准备 测试用=============
    df_product_map = spark.read.parquet(product_map_path)
    for i in df_product_map.columns:
        if i in ["标准通用名", "通用名_标准", "药品名称_标准", "S_Molecule_Name"]:
            df_product_map = df_product_map.withColumnRenamed(i, "通用名")
        if i in ["商品名_标准", "S_Product_Name"]:
            df_product_map = df_product_map.withColumnRenamed(i, "标准商品名")
        if i in ["标准途径"]:
            df_product_map = df_product_map.withColumnRenamed(i, "std_route")
        if i in ["min1_标准"]:
            df_product_map = df_product_map.withColumnRenamed(i, "min2")
    if "std_route" not in df_product_map.columns:
        df_product_map = df_product_map.withColumn("std_route", func.lit(''))
        
    df_product_map = df_product_map.withColumnRenamed('min1', 'MIN') \
                        .withColumnRenamed('min2', 'MIN_STD') \
                        .withColumnRenamed('通用名', 'MOLECULE_STD') \
                        .withColumnRenamed('标准商品名', 'BRAND_STD') \
                        .withColumnRenamed('标准剂型', 'FORM_STD') \
                        .withColumnRenamed('标准规格', 'SPECIFICATIONS_STD') \
                        .withColumnRenamed('标准包装数量', 'PACK_NUMBER_STD') \
                        .withColumnRenamed('标准生产企业', 'MANUFACTURER_STD') \
                        .withColumnRenamed('标准集团', 'CORP_STD') \
                        .withColumnRenamed('std_route', 'ROUTE_STD') \
                        .withColumnRenamed('pfc', 'PACK_ID')
    df_product_map = df_product_map.withColumn('PACK_NUMBER_STD', col('PACK_NUMBER_STD').cast(IntegerType())) \
                            .withColumn('PACK_ID', col('PACK_ID').cast(IntegerType()))
    

    # %%
    # =========== 数据执行 =============
    logger.debug('数据执行-start：product_mapping')
    
    # df_raw_data = spark.read.parquet(p_hospital_mapping_out)
    
    struct_type = StructType([ StructField('PHA', StringType(), True),
                                StructField('ID', StringType(), True),
                                StructField('YEAR_MONTH', IntegerType(), True),
                                StructField('RAW_HOSP_NAME', StringType(), True),
                                StructField('BRAND', StringType(), True),
                                StructField('FORM', StringType(), True),
                                StructField('SPECIFICATIONS', StringType(), True),
                                StructField('PACK_NUMBER', IntegerType(), True),
                                StructField('MANUFACTURER', StringType(), True),
                                StructField('MOLECULE', StringType(), True),
                                StructField('SOURCE', StringType(), True),
                                StructField('CORP', StringType(), True),
                                StructField('ROUTE', StringType(), True),
                                StructField('ORG_MEASURE', StringType(), True),
                                StructField('SALES', DoubleType(), True),
                                StructField('UNITS', DoubleType(), True),
                                StructField('UNITS_BOX', DoubleType(), True),
                                StructField('PATH', StringType(), True),
                                StructField('SHEET', StringType(), True),
                                StructField('CITY', StringType(), True),
                                StructField('PROVINCE', StringType(), True),
                                StructField('CITY_TIER', DoubleType(), True),
                                StructField('MONTH', IntegerType(), True),
                                StructField('YEAR', IntegerType(), True) ]
                                )
    df_raw_data = spark.read.format("parquet").load(p_hospital_mapping_out, schema=struct_type)
    
    
    # if g_project_name != "Mylan":
    df_raw_data = df_raw_data.withColumn("BRAND", func.when((col('BRAND').isNull()) | (col('BRAND') == 'NA'), col('MOLECULE')).
                                   otherwise(col('BRAND')))
        
    df_raw_data = df_raw_data.withColumn('PACK_NUMBER', col('PACK_NUMBER').cast(StringType()))
    
    # MIN 生成
    df_raw_data = df_raw_data.withColumn('tmp', func.concat_ws(g_minimum_product_sep, 
                                *[func.when(col(i).isNull(), func.lit("NA")).otherwise(col(i)) for i in g_minimum_product_columns]))
       
    # Mylan不重新生成 MIN，其他项目生成MIN（遗留问题，测试后可与其他项目一样）
    # if g_project_name == "Mylan":
    #     df_raw_data = df_raw_data.drop("tmp")
    # else:
    df_raw_data = df_raw_data.withColumnRenamed("tmp", g_minimum_product_newname)
        
    # df_product_map
    df_product_map_for_needclean = df_product_map.select("MIN").distinct()
    df_product_map_for_rawdata = df_product_map.select("MIN", "MIN_STD", "MOLECULE_STD", "ROUTE_STD", "BRAND_STD").distinct()
    
    
    # df_raw_data 待清洗数据
    df_need_cleaning = df_raw_data.join(df_product_map_for_needclean, on="MIN", how="left_anti") \
                        .select(g_need_cleaning_cols) \
                        .distinct()
    logger.debug('待清洗行数: ' + str(df_need_cleaning.count()))
    
    # df_raw_data 信息匹配
    df_raw_data = df_raw_data.join(df_product_map_for_rawdata, on="MIN", how="left")

    # %%
    # =========== 输出 =============
    df_need_cleaning = df_need_cleaning.repartition(2)
    df_need_cleaning.write.format("parquet") \
        .mode("overwrite").save(p_need_cleaning)
    logger.debug("已输出待清洗文件至: " + p_need_cleaning)
        
        
    df_raw_data = df_raw_data.repartition(2)
    df_raw_data.write.format("parquet") \
        .mode("overwrite").save(p_product_mapping_out)
    logger.debug("输出 product_mapping 结果：" + p_product_mapping_out)
    
    logger.debug('数据执行-Finish')

