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
    g_base_path = kwargs['g_base_path']
    ### input args ###
    
    ### output args ###
    g_deal_poi = kwargs['g_deal_poi']
    ### output args ###

    
    
    import pandas as pd
    import os
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col            # %%
    #测试用
    
    # dag_name = 'Max'
    # run_id = 'max_test_beida_202012'
    # g_project_name='贝达'
    # result_path_prefix=get_result_path({"name":job_name, "dag_name":dag_name, "run_id":run_id})
    # depends_path=get_depends_path({"name":job_name, "dag_name":dag_name, 
    #                                  "run_id":run_id, "depend_job_names_keys":depend_job_names_keys })

    # %%
    logger.debug('数据执行-start:产品筛选')
    
    # 输入
    p_product_mapping_out = depends_path['product_mapping_out']
            
    # 输出
    p_deal_poi = result_path_prefix + g_deal_poi

    # %%
    ## jupyter测试需要
    
    # p_product_mapping_out = p_product_mapping_out.replace("s3:", "s3a:")
    # p_deal_poi = p_deal_poi.replace("s3:", "s3a:")
    # print(p_product_mapping_out)
    # %%
    # =========== 数据准备，测试用 =============
    # 测试输入
    # products_of_interest_path = g_max_path + "/" + g_project_name + "/poi.csv"
    # df_products_of_interest = spark.read.csv(products_of_interest_path, header=True)
    # df_products_of_interest = df_products_of_interest.withColumnRenamed('poi', 'POI')
    # df_products_of_interest.show()
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
    
    
    def createView(company, table_name, sub_path, other = "",
        time="2021-04-06",
        base_path = g_base_path):
             
            definite_path = "{base_path}/{sub_path}/TIME={time}/COMPANY={company}/{other}"
            path = definite_path.format(
                base_path = base_path,
                sub_path = sub_path,
                time = time,
                company = company,
                other = other
            )
            spark.read.parquet(path).createOrReplaceTempView(table_name)
    
    createView(g_project_name, "poi", "DIMENSION/MAPPING/POI", time = "2021-04-14")
    poi_sql = """
            SELECT * FROM poi
        """
    df_products_of_interest = spark.sql(poi_sql)
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

    # %%
    ## 和之前的结果比较
    # old_path = "s3a://ph-max-auto/2020-08-11/Test_Max_Model_ssh/refactor/runs/Test_Max_Model_ssh_Test_Max_Model_ssh_2021-05-12_17-17-25/deal_poi/deal_poi_out/"
    # old_path = "s3a://ph-max-auto/2020-08-11/Max_test2/refactor/runs/manual__2021-05-06T09_58_09.076641+00_00/deal_poi/deal_poi_out/"
    # df_data_old = spark.read.parquet( old_path)
    # df_data_old = df_data_old.withColumn("YEAR_MONTH", col("YEAR_MONTH").cast("int"))
    
    
    # print("OLD: %s      NEW:%s "%(df_data_old.count(), df_raw_data.count() ))
    
    
    
    
    # #### 用差集去计算
    # old_col_list = df_data_old.columns
    # raw_col_list = df_raw_data.columns
    # # sam_col_list = list( set(old_col_list)&set(raw_col_list)   )
    # sam_col_list = ['FORM_STD', 'MANUFACTURER_STD', 'YEAR_MONTH',  'ID', 
    #  'MOLECULE_STD', 'PHA', 'MOLECULE_STD_FOR_GR', 'YEAR', 'UNITS', 
    #  'BRAND_STD', 'MIN_STD', 'SPECIFICATIONS_STD', 'CITY', 'SALES', 
    #  'PROVINCE', 'MONTH', 'PACK_NUMBER_STD', 'CITY_TIER']
    
    # print(sam_col_list )
    # df_data_old_sam_col = df_data_old.select( sam_col_list  )
    # df_raw_data_sam_col = df_raw_data.select( sam_col_list )
    # # df_raw_data.show(1, vertical=True)
    # subtract_result_1 =  df_data_old_sam_col.subtract( df_raw_data_sam_col )
    # subtract_result_1.show(1)
    # print("subtract-number:  ", subtract_result_1.count() )
    # subtract_result_2 =  df_raw_data_sam_col.subtract( df_data_old_sam_col )
    
    # subtract_result_2.show(2)
    # print("subtract-number:  ", subtract_result_2.count() )
    # %%
    # subtract_result_1.where( subtract_result_1["PACK_ID"].isNull() ).count()
    
    # print( df_raw_data.where( df_raw_data["PACK_ID"].isNull() ).count() )
    # print( df_data_old.where( df_data_old["PACK_ID"].isNull() ).count() )
