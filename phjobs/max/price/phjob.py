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
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col        # %%
    #测试用
    
    # g_project_name='贝达'
    # result_path_prefix=get_result_path({"name":job_name, "dag_name":dag_name, "run_id":run_id})
    # depends_path=get_depends_path({"name":job_name, "dag_name":dag_name, 
    #                                  "run_id":run_id, "depend_job_names_keys":depend_job_names_keys })

    # %%
    logger.debug('数据执行-start:价格计算')
    # 测试输入
    products_of_interest_path = max_path + "/" + g_project_name + "/poi.csv"
    
    # 输入
    p_product_mapping_out = depends_path['deal_poi_out']
            
    # 输出
    p_price = result_path_prefix + g_price
    p_price_city = result_path_prefix + g_price_city

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
    # %%
    # 1 价格计算：补数部分的数量需要用价格得出
    # 1.1 CITY_TIER 层面的价格
    df_price = df_raw_data.groupBy("MIN_STD", "YEAR_MONTH", "CITY_TIER") \
                            .agg((func.sum("SALES") / func.sum("UNITS")).alias("PRICE"))
    
    df_price2 = df_raw_data.groupBy("MIN_STD", "YEAR_MONTH") \
                            .agg((func.sum("SALES") / func.sum("UNITS")).alias("PRICE2"))
    
    df_price = df_price.join(df_price2, on=["MIN_STD", "YEAR_MONTH"], how="left")
    
    df_price = df_price.withColumn("PRICE", func.when(func.isnull(col('PRICE')), col('PRICE2')).otherwise(col('PRICE')))
    df_price = df_price.withColumn("PRICE", func.when(func.isnull(col('PRICE')), func.lit(0)).otherwise(col('PRICE'))) \
                        .drop("PRICE2")
    
    # 1.2 城市层面 的价格
    df_price_city = df_raw_data.groupBy("MIN_STD", "YEAR_MONTH", 'CITY', 'PROVINCE') \
                                .agg((func.sum("SALES") / func.sum("UNITS")).alias("PRICE"))
    df_price_city = df_price_city.where(~col('PRICE').isNull())

    # %%
    # =========== 输出 =============
    df_price = df_price.repartition(2)
    df_price.write.format("parquet") \
        .mode("overwrite").save(p_price)
    
    df_price_city = df_price_city.repartition(2)
    df_price_city.write.format("parquet") \
        .mode("overwrite").save(p_price_city)
    
    logger.debug("输出 price：" + p_price)
    logger.debug("输出 price_city：" + p_price_city)

    # %%
    # df_price.agg(func.sum('PRICE')).show()

    # %%
    # df_price_city.agg(func.sum('PRICE')).show()

    # %%
    # df=spark.read.parquet('s3a://ph-max-auto/v0.0.1-2020-06-08/贝达/202012_test/price/')
    # df.agg(func.sum('Price')).show()

    # %%
    # df=spark.read.parquet('s3a://ph-max-auto/v0.0.1-2020-06-08/贝达/202012_test/price_city/')
    # df.agg(func.sum('Price')).show()

    # %%
    # test_path = "s3a://ph-max-auto/2020-08-11/Max/refactor/runs/max_test_beida_202012_bk/price/price_city"
    # df_test = spark.read.parquet(test_path).withColumnRenamed("PRICE", "PRICE_OLD")
    # df_test.show(1, vertical=True)
    
    # df_test = df_test.filter( df_test.CITY.isNotNull() )
    # df_price_city = df_price_city.filter( df_price_city.CITY.isNotNull() )
    # print(df_test.count(), df_price_city.count() )
    
    
    # compare = df_price_city.join(df_test, on=["MIN_STD", "YEAR_MONTH", "CITY", "PROVINCE" ], how="inner")
    # print(compare.count())

    # %%
    
    ## 比较 PRICE 是否一致
    # compare_error = compare.withColumn("Error", compare["PRICE"]-compare["PRICE_OLD"])
    # compare_error.where( func.abs(compare_error["Error"])>0.01 ).count() 

    # %% 
    # 找到CITY 中有 NULL 列
    # df_price_city.filter( df_price_city.CITY.isNull() ).show()
    # df_test.filter( df_test.CITY.isNull() ).show()

    # %%
    ## 找两张表的差
    # compare = df_price_city.join(df_test, on=["MIN_STD", "YEAR_MONTH", "CITY", "PROVINCE" ], how="anti")
    # compare = df_test.join(df_price_city, on=["MIN_STD", "YEAR_MONTH", "CITY", "PROVINCE" ], how="anti")
    # compare.show()

    # %%
    ## 比较旧结果和新结果在 price 上差异
    # compare = df_price_city.join(df_test, on=["MIN_STD", "YEAR_MONTH", "CITY", "PROVINCE" ], how="inner")
    # compare.withColumn("Error", compare.PRICE-compare.PRICE_OLD).select("Errow").distinct().show()

