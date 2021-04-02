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
    depend_job_names_keys = kwargs['depend_job_names_keys']
    dag_name = kwargs['dag_name']
    run_id = kwargs['run_id']
    max_path = kwargs['max_path']
    ### input args ###
    
    ### output args ###
    g_price = kwargs['g_price']
    g_price_city = kwargs['g_price_city']
    ### output args ###

    import pandas as pd
    import os
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col

    logger.debug('数据执行-start:价格计算')
    # 测试输入
    products_of_interest_path = max_path + "/" + g_project_name + "/poi.csv"
    
    # 输入
    p_product_mapping_out = depends_path['product_mapping_out']
            
    # 输出
    p_price = result_path_prefix + g_price
    p_price_city = result_path_prefix + g_price_city

    # =========== 数据准备，测试用 =============
    df_products_of_interest = spark.read.csv(products_of_interest_path, header=True)
    df_products_of_interest = df_products_of_interest.withColumnRenamed('poi', 'POI')

    # =========== 数据执行 =============
    df_raw_data = spark.read.parquet(p_product_mapping_out)
    
    g_products_of_interest = df_products_of_interest.toPandas()["POI"].values.tolist()
    
    df_raw_data = df_raw_data.withColumn("MOLECULE_STD_FOR_GR",
                                   func.when(col("BRAND_STD").isin(g_products_of_interest), col("BRAND_STD")).
                                   otherwise(col('MOLECULE_STD')))

    # 1 价格计算：补数部分的数量需要用价格得出
    # 1.1 CITY_TIER 层面的价格
    df_price = df_raw_data.groupBy("MIN2", "YEAR_MONTH", "CITY_TIER") \
                            .agg((func.sum("SALES") / func.sum("UNITS")).alias("PRICE"))
    
    df_price2 = df_raw_data.groupBy("MIN2", "YEAR_MONTH") \
                            .agg((func.sum("SALES") / func.sum("UNITS")).alias("PRICE2"))
    
    df_price = df_price.join(df_price2, on=["MIN2", "YEAR_MONTH"], how="left")
    
    df_price = df_price.withColumn("PRICE", func.when(func.isnull(col('PRICE')), col('PRICE2')).otherwise(col('PRICE')))
    df_price = df_price.withColumn("PRICE", func.when(func.isnull(col('PRICE')), func.lit(0)).otherwise(col('PRICE'))) \
                        .drop("PRICE2")
    
    # 1.2 城市层面 的价格
    df_price_city = df_raw_data.groupBy("MIN2", "YEAR_MONTH", 'CITY', 'PROVINCE') \
                                .agg((func.sum("SALES") / func.sum("UNITS")).alias("PRICE"))
    df_price_city = df_price_city.where(~col('PRICE').isNull())
    
    # 输出
    df_price = df_price.repartition(2)
    df_price.write.format("parquet") \
        .mode("overwrite").save(p_price)
    
    df_price_city = df_price_city.repartition(2)
    df_price_city.write.format("parquet") \
        .mode("overwrite").save(p_price_city)
    
    logger.debug("输出 price：" + p_price)
    logger.debug("输出 price_city：" + p_price_city)

    #df_price.agg(func.sum('PRICE')).show()

    #df_price_city.agg(func.sum('PRICE')).show()

    #df=spark.read.parquet('s3a://ph-max-auto/v0.0.1-2020-06-08/贝达/202012/price/')
    #df.agg(func.sum('Price')).show()

    #df=spark.read.parquet('s3a://ph-max-auto/v0.0.1-2020-06-08/贝达/202012/price_city/')
    #df.agg(func.sum('Price')).show()



