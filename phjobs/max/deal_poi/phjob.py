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
    depend_job_names_keys = kwargs['depend_job_names_keys']
    g_max_path = kwargs['g_max_path']
    ### input args ###
    
    ### output args ###
    g_deal_poi = kwargs['g_deal_poi']
    ### output args ###

    
    
    import pandas as pd
    import os
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col        # %%
    #测试用
    # g_project_name='贝达'
    # result_path_prefix=get_result_path({"name":job_name, "dag_name":dag_name, "run_id":run_id})
    # depends_path=get_depends_path({"name":job_name, "dag_name":dag_name, 
    #                                  "run_id":run_id, "depend_job_names_keys":depend_job_names_keys })
    # %%
    logger.debug('数据执行-start:产品筛选')
    # 测试输入
    products_of_interest_path = g_max_path + "/" + g_project_name + "/poi.csv"
    
    # 输入
    p_product_mapping_out = depends_path['product_mapping_out']
            
    # 输出
    p_deal_poi = result_path_prefix + g_deal_poi

    # %%
    # =========== 数据准备，测试用 =============
    df_products_of_interest = spark.read.csv(products_of_interest_path, header=True)
    df_products_of_interest = df_products_of_interest.withColumnRenamed('poi', 'POI')

    # %%
    # =========== 数据执行 =============
    # df_raw_data = spark.read.parquet(p_product_mapping_out)
    
    struct_type = StructType( [ StructField('PHA', StringType(), True),
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
                                StructField('MIN_STD', StringType(), True) ])
    df_raw_data = spark.read.format("parquet").load(p_product_mapping_out, schema=struct_type)
    
    g_products_of_interest = df_products_of_interest.toPandas()["POI"].values.tolist()
    
    df_raw_data = df_raw_data.withColumn("MOLECULE_STD_FOR_GR",
                                   func.when(col("BRAND_STD").isin(g_products_of_interest), col("BRAND_STD")).
                                   otherwise(col('MOLECULE_STD')))

    # %%
    # =========== 输出 =============
    df_raw_data = df_raw_data.repartition(2)
    df_raw_data.write.format("parquet") \
        .mode("overwrite").save(p_deal_poi)
    
    logger.debug("输出：" + p_deal_poi)

