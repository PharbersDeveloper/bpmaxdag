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
    g_month = kwargs['g_month']
    g_year = kwargs['g_year']
    depend_job_names_keys = kwargs['depend_job_names_keys']
    g_current_month = kwargs['g_current_month']
    g_monthly_update = kwargs['g_monthly_update']
    dag_name = kwargs['dag_name']
    run_id = kwargs['run_id']
    max_path = kwargs['max_path']
    not_arrived_path = kwargs['not_arrived_path']
    published_path = kwargs['published_path']
    ### input args ###
    
    ### output args ###
    g_growth_rate = kwargs['g_growth_rate']
    ### output args ###

    
    import pandas as pd
    import os
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col    
    # %%
    # 测试用
    
    # g_project_name = '贝达'
    # g_month = "12"
    # g_year = "2020"
    # g_current_month = "12"
    # result_path_prefix=get_result_path({"name":job_name, "dag_name":dag_name, "run_id":run_id})
    # depends_path=get_depends_path({"name":job_name, "dag_name":dag_name, 
    #                                  "run_id":run_id, "depend_job_names_keys":depend_job_names_keys })

    # %%
    logger.debug('数据执行-start：计算增长率-月更新')
    # 是否运行此job
    if g_monthly_update == "False":
         return
        
    # =========== 输入 输出 =============   
    # 输入
    g_year = int(g_year)
    g_month = int(g_month)
    g_current_month = int(g_current_month)
    
    
    if not_arrived_path == "Empty":    
        p_not_arrived = max_path + "/Common_files/Not_arrived" + str(g_year*100 + g_current_month) + ".csv"
    if published_path == "Empty":
        p_published_right = max_path + "/Common_files/Published" + str(g_year) + ".csv"
        p_published_left = max_path + "/Common_files/Published" + str(g_year - 1) + ".csv"
    else:
        published_path  = published_path.replace(" ","").split(",")
        p_published_left = published_path[0]
        p_published_right = published_path[1]
    
    
    # 输入
    p_product_mapping_out = depends_path['deal_poi_out']
            
    # 输出
    p_growth_rate = result_path_prefix + g_growth_rate

    # %%
    # =========== 数据准备，测试用 =============
    df_published_left = spark.read.csv(p_published_left, header=True)
    df_published_left = df_published_left.withColumnRenamed('Source', 'SOURCE')
    
    df_published_right = spark.read.csv(p_published_right, header=True)
    df_published_right = df_published_right.withColumnRenamed('Source', 'SOURCE')
    
    df_not_arrived =  spark.read.csv(p_not_arrived, header=True)
    df_not_arrived = df_not_arrived.withColumnRenamed('Date', 'DATE')

    # %%
    # =========== 数据执行 =============
    # raw_data 处理（products_of_interest 可以写成参数，在前一个job筛选）
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
                                StructField('MIN_STD', StringType(), True)])
    df_raw_data = spark.read.format("parquet").load(p_product_mapping_out, schema=struct_type)

    # %%
    # 计算样本分子增长率(月更和模型函数相同)
    def calculateGrowth(df_raw_data, g_max_month=12):
        # TODO: 完整年用完整年增长，不完整年用不完整年增长
        df_raw_data = df_raw_data.where(df_raw_data.MONTH <= g_max_month)
    
        # df_raw_data 处理
        df_growth_raw_data = df_raw_data.na.fill({"CITY_TIER": 5.0})
        df_growth_raw_data = df_growth_raw_data.withColumn("CITYGROUP", col('CITY_TIER'))
    
        # 增长率计算过程
        df_growth_calculating = df_growth_raw_data.groupBy("MOLECULE_STD_FOR_GR", "CITYGROUP", "YEAR") \
                                                .agg(func.sum(df_growth_raw_data.SALES).alias("VALUE"))
    
        years = df_growth_calculating.select("YEAR").distinct().toPandas()["YEAR"].sort_values().values.tolist()
        years = [str(i) for i in years]
        years_name = ["YEAR_" + i for i in years]
        # 数据长变宽
        df_growth_calculating = df_growth_calculating.groupBy("MOLECULE_STD_FOR_GR", "CITYGROUP").pivot("YEAR").agg(func.sum('VALUE')).fillna(0)
        df_growth_calculating = df_growth_calculating.select(["MOLECULE_STD_FOR_GR", "CITYGROUP"] + years)
        # 对year列名修改
        for i in range(0, len(years)):
            df_growth_calculating = df_growth_calculating.withColumnRenamed(years[i], years_name[i])
    
        # 计算得到年增长
        for i in range(0, len(years) - 1):
            df_growth_calculating = df_growth_calculating.withColumn("GR" + years[i][2:4] + years[i + 1][2:4],
                                                        col(years_name[i + 1]) / col(years_name[i]))
        df_growth_rate = df_growth_calculating     
        # 增长率的调整: 空值/大于10/小于0.1 调整为1
        for y in [name for name in df_growth_rate.columns if name.startswith("GR")]:
            df_growth_rate = df_growth_rate.withColumn(y, func.when(func.isnull(col(y)) | (col(y) > 10) | (col(y) < 0.1), 1).
                                                 otherwise(col(y)))
        return df_growth_rate

    # %%
    # 近两年发表医院减去当月未到医院，用这些医院数据计算增长率
    df_published_left = df_published_left.select('ID').distinct()
    df_published_right = df_published_right.select('ID').distinct()
    df_not_arrived = df_not_arrived.select('ID', 'DATE').distinct()
    
    l_month_hospital = df_published_left.intersect(df_published_right) \
                        .exceptAll(df_not_arrived.where(df_not_arrived.DATE == g_year * 100 + g_month).select("ID")) \
                        .toPandas()["ID"].tolist()
    
    df_raw_data_month = df_raw_data.where(col('MONTH') == g_month) \
                                   .where(col('ID').isin(l_month_hospital))
    
    # 执行函数
    df_growth_rate_month = calculateGrowth(df_raw_data_month)
    # 标记是哪个月的growth_rate
    df_growth_rate_month = df_growth_rate_month.withColumn("MONTH_FOR_ADD", func.lit(g_month))
    df_growth_rate_month = df_growth_rate_month.withColumn("YEAR_FOR_ADD", func.lit(g_year))

    # %%
    # 输出
    df_growth_rate_month = df_growth_rate_month.repartition(1)
    df_growth_rate_month.write.format("parquet") \
        .mode("overwrite").save(p_growth_rate)
    
    logger.debug('数据执行-Finish')

    # %%
    '''
    df_growth_rate_month.agg(func.sum('YEAR_2017'),func.sum('YEAR_2018'),func.sum('YEAR_2019'),func.sum('YEAR_2020'),
                             func.sum('GR1718'),func.sum('GR1819'),func.sum('GR1920')).show()
    '''

    # %%
    '''
    check = spark.read.parquet('s3a://ph-max-auto/v0.0.1-2020-06-08/贝达/202012_test/growth_rate/')
    logger.debug(check.count())
    check.where(col('month_for_monthly_add') == 12).agg(func.sum('Year_2017'),func.sum('Year_2018'),func.sum('Year_2019'),func.sum('Year_2020'),
                             func.sum('GR1718'),func.sum('GR1819'),func.sum('GR1920')).show()
    '''

    # %%
    ## 比较结果
    # df_data_old = spark.read.parquet('s3a://ph-max-auto/2020-08-11/Max/refactor/runs/max_test_beida_202012_bk/growth_monthly/growth_rate')
    # df_data_old = df_data_old.withColumnRenamed('GR1718', 'GR1718_OLD')\
    #                         .withColumnRenamed('GR1819', 'GR1819_OLD')\
    #                         .withColumnRenamed('GR1920', 'GR1920_OLD')
    
    # compare = df_growth_rate_month.join(df_data_old, on=['MOLECULE_STD_FOR_GR', 'CITYGROUP', 'YEAR_2017', 'YEAR_2018', 'YEAR_2019', 'YEAR_2020', 
    #                                     'MONTH_FOR_ADD', 'YEAR_FOR_ADD'], how="anti")
    # print(df_growth_rate_month.count(), df_data_old.count(), compare.count() )
    # compare.show()
    
    # compare_2 = df_data_old.join(df_growth_rate_month, on=['MOLECULE_STD_FOR_GR', 'CITYGROUP', 'YEAR_2017', 'YEAR_2018', 'YEAR_2019', 'YEAR_2020', 
    #                                     'MONTH_FOR_ADD', 'YEAR_FOR_ADD'], how="anti")
    # compare_2.show()
    
    
    # compare = df_growth_rate_month.join(df_data_old, on=['MOLECULE_STD_FOR_GR', 'CITYGROUP', 'MONTH_FOR_ADD', 'YEAR_FOR_ADD'], how="inner")
    # print(df_growth_rate_month.count(), df_data_old.count(), compare.count() )
    
    # for i in ["GR1718", "GR1819", "GR1920"]:
    #     s = compare.withColumn("Error", compare[i]-compare[i+"_OLD"]).select("Error").distinct().collect()
    #     print(s)

    # %%
    # df_growth_rate_month.count()
    # df_growth_rate_month.show(1, vertical=True)
    # compare_2 = df_data_old.join(df_growth_rate_month, on=['MOLECULE_STD_FOR_GR', 'CITYGROUP', 'YEAR_2017', 'YEAR_2018', 'YEAR_2019', 'YEAR_2020', 
    #                                     'MONTH_FOR_ADD', 'YEAR_FOR_ADD'], how="anti")
    
    # compare_2 = df_data_old.join(df_growth_rate_month, on=['MOLECULE_STD_FOR_GR', 'CITYGROUP', 'MONTH_FOR_ADD', 'YEAR_FOR_ADD'], how="anti")
    # compare_2.select("MOLECULE_STD_FOR_GR", "CITYGROUP").show()
    # compare_2.show(1, vertical=True)

