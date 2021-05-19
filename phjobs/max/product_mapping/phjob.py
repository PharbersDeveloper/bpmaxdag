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
    
    
    # # dag_name = "Test_Max_Model_ssh"
    # # run_id = "Test_Max_Model_ssh_Test_Max_Model_ssh_2021-05-12_17-17-25"
    
    # dag_name = 'Max'
    # run_id = 'max_test_beida_202012'
    # g_project_name = '贝达'
    # g_out_dir = '202012_test'
    # g_minimum_product_sep='|'
    # result_path_prefix = get_result_path({"name":job_name, "dag_name":dag_name, "run_id":run_id})
    # depends_path = get_depends_path({"name":job_name, "dag_name":dag_name, 
    #                                  "run_id":run_id, "depend_job_names_keys":depend_job_names_keys })

    # %%
    logger.debug('job2_product_mapping')
    
    
    # 输入
    p_hospital_mapping_out = depends_path['hospital_mapping_out']
    if g_minimum_product_sep == "kong":
        g_minimum_product_sep = ""
    
    # 输出
    p_product_mapping_out = result_path_prefix + g_product_mapping_out
    # %%
    # jupyter测试
    
    # p_hospital_mapping_out = p_hospital_mapping_out.replace("s3:", "s3a:")
    # p_product_mapping_out = p_product_mapping_out.replace("s3:", "s3a:")
    # %%
    # =========== 数据执行 =============
    logger.debug('数据执行-start：product_mapping')
    
    # df_raw_data = spark.read.parquet(p_hospital_mapping_out)
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
                                StructField('YEAR', IntegerType(), True) ])
    df_raw_data = spark.read.format("parquet").load(p_hospital_mapping_out, schema=struct_type)
    
    ## 生成 MIN_STD列
    df_raw_data = df_raw_data.withColumn("MIN_STD", func.format_string("%s|%s|%s|%s|%s", "BRAND_STD","FORM_STD",
                                            "SPECIFICATIONS_STD", "PACK_NUMBER_STD", "MANUFACTURER_STD"))

    # %%
    # =========== 输出 =============
    
    df_raw_data = df_raw_data.repartition(2)
    df_raw_data.write.format("parquet") \
        .mode("overwrite").save(p_product_mapping_out)
    logger.debug("输出 product_mapping 结果：" + p_product_mapping_out)
    
    logger.debug('数据执行-Finish')

# df_raw_data

# raw_data_path = "s3a://ph-max-auto/2020-08-11/Max/refactor/runs/max_test_beida_202012/product_mapping/product_mapping_out"
# df_raw_data = spark.read.parquet( raw_data_path )

# print( df_raw_data.where( df_raw_data["PACK_ID"].isNull() ).count() )

# # old_path = "s3a://ph-max-auto/2020-08-11/Max/refactor/runs/max_test_beida_202012_bk/product_mapping/product_mapping_out/"
# # old_path = "s3a://ph-max-auto/2020-08-11/Test_Max_Model_ssh/refactor/runs/Test_Max_Model_ssh_Test_Max_Model_ssh_2021-05-12_17-17-25/product_mapping/product_mapping_out/"
# old_path = "s3a://ph-max-auto/2020-08-11/Max_test2/refactor/runs/manual__2021-05-06T09_58_09.076641+00_00/product_mapping/product_mapping_out/"
# df_data_old = spark.read.parquet( old_path)
# print( df_data_old.where( df_data_old["PACK_ID"].isNull() ).count()  )
    # %%
    # old_path = "s3a://ph-max-auto/2020-08-11/Max/refactor/runs/max_test_beida_202012_bk/product_mapping/product_mapping_out/"
    # df_data_old = spark.read.parquet( old_path)
    
    # # df_data_old = df_data_old.withColumn("YEAR_MONTH", col("YEAR_MONTH").cast("int"))
    
    
    # print("OLD: %s      NEW:%s "%(df_data_old.count(), df_raw_data.count() ))
    
    # # df_data_old = df_data_old.withColumnRenamed("SALES", "SALES_OLD")\
    # #                             .withColumnRenamed("UNIT", "UNIT_OLD")
    
    # ### 都匹配
    # # join_list = ['PHA', 'ID', 'YEAR_MONTH', 'MIN_STD',  'CITY', 'PROVINCE', 'CITY_TIER'  ]
    # # compare = df_raw_data.join( df_data_old, on=join_list, how="inner")
    
    # ## 存在CITY为NUll的时候
    # # df_data_old = df_data_old.where( df_data_old["CITY"].isNull() )
    # # df_raw_data = df_raw_data.where(df_raw_data["CITY"].isNull() )
    # # compare = df_raw_data.join( df_data_old, on=["ID", "MIN_STD",  "YEAR_MONTH"],how="inner")
    
    # # join_list = ['PHA', 'ID', 'YEAR_MONTH',  'CITY', 'PROVINCE',  ]
    # # compare = df_raw_data.join( df_data_old, on=join_list, how="anti")
    
    # # compare.show(3, vertical=True)
    # # print("join-inner-number: ", compare.count())
    
    # # print( df_data_old )
    # # print(df_raw_data)
    # # df_data_old.show(1,vertical=True)
    # # df_raw_data.show(1, vertical=True)
    # # # df_raw_data.show(4)
    # # df_raw_data.select("MIN_STD").limit(10).collect( )
    
    
    # #### 用差集去计算
    # old_col_list = df_data_old.columns
    # raw_col_list = df_raw_data.columns
    # sam_col_list = list( set(old_col_list)&set(raw_col_list)   )
    # print(sam_col_list)
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
    # print(compare_error.count() )
    
    # print(  compare_error.where( func.abs( compare_error["Error"]) >0.01 ) \
    #           .count() )
    
    # print( compare.withColumn("Error_2", compare["PREDICT_UNIT"]- compare["PREDICT_UNIT_OLD"] ).select("Error_2").distinct().collect() )

    # %%
    ## 匹配不上是因为什么
    # cant_mapping = df_data_old.join( df_data_old, on=['PHA', 'ID', 'YEAR_MONTH', 'MIN_STD', 
    #                                                  'CITY', 'PROVINCE', 'CITY_TIER', 'MONTH', 'YEAR'],how="anti")
    # print(cant_mapping.count())
    # print(cant_mapping.select("ID", "MIN_STD",  "YEAR_MONTH").distinct().count())
    # cant_mapping.show(1,vertical=True)

