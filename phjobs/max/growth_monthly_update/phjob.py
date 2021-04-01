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
    max_month = kwargs['max_month']
    g_month = kwargs['g_month']
    g_year = kwargs['g_year']
    depend_job_names_keys = kwargs['depend_job_names_keys']
    dag_name = kwargs['dag_name']
    run_id = kwargs['run_id']
    ### input args ###
    
    ### output args ###
    g_growth_rate = kwargs['g_growth_rate']
    ### output args ###

    import pandas as pd
    import os
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col

    #测试用
    g_project_name = '贝达'
    g_month = "12"
    g_year = "2020"
    
    max_path = 's3a://ph-max-auto/v0.0.1-2020-06-08/'
    not_arrived_path = 'Empty'
    published_path = 'Empty'
    current_month = '12'
    current_month = int(current_month)

    logger.debug('计算增长率-月更新')
    
    # =========== 输入 输出 =============   
    # 输入
    max_month = int(max_month)
    g_year = int(g_year)
    g_month = int(g_month)
    
    if not_arrived_path == "Empty":    
        p_not_arrived = max_path + "/Common_files/Not_arrived" + str(g_year*100 + current_month) + ".csv"
    if published_path == "Empty":
        p_published_right = max_path + "/Common_files/Published" + str(g_year) + ".csv"
        p_published_left = max_path + "/Common_files/Published" + str(g_year - 1) + ".csv"
    else:
        published_path  = published_path.replace(" ","").split(",")
        p_published_left = published_path[0]
        p_published_right = published_path[1]
    
    
    # 输入
    products_of_interest_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/" + g_project_name + "/poi.csv"
    p_product_mapping_out = depends_path['product_mapping_out']
            
    # 输出
    p_growth_rate = result_path_prefix + g_growth_rate

    # =========== 数据准备，测试用 =============
    df_published_left = spark.read.csv(p_published_left, header=True)
    df_published_left = df_published_left.withColumnRenamed('Source', 'SOURCE')
    
    df_published_right = spark.read.csv(p_published_right, header=True)
    df_published_right = df_published_right.withColumnRenamed('Source', 'SOURCE')
    
    df_not_arrived =  spark.read.csv(p_not_arrived, header=True)
    df_not_arrived = df_not_arrived.withColumnRenamed('Date', 'DATE')

    # =========== 数据执行 =============
    logger.debug('数据执行-start')
    
    # raw_data 处理
    df_raw_data = spark.read.parquet(p_product_mapping_out)
    
    df_products_of_interest = spark.read.csv(products_of_interest_path, header=True)
    g_products_of_interest = df_products_of_interest.toPandas()["poi"].values.tolist()
    
    df_raw_data = df_raw_data.withColumn("MOLECULE_STD_FOR_GR",
                                   func.when(col("BRAND_STD").isin(g_products_of_interest), col("BRAND_STD")).
                                   otherwise(col('MOLECULE_STD')))

    # 3 计算样本分子增长率: cal_growth
    def calculate_growth(df_raw_data, max_month=12):
        # TODO: 完整年用完整年增长，不完整年用不完整年增长
        if max_month < 12:
            df_raw_data = df_raw_data.where(df_raw_data.Month <= max_month)
    
        # df_raw_data 处理
        growth_raw_data = df_raw_data.na.fill({"CITY_TIER": 5.0})
        growth_raw_data = growth_raw_data.withColumn("CITYGROUP", growth_raw_data.CITY_TIER)
    
        # 增长率计算过程
        growth_calculating = growth_raw_data.groupBy("MOLECULE_STD_FOR_GR", "CITYGROUP", "YEAR") \
            .agg(func.sum(growth_raw_data.SALES).alias("VALUE"))
    
        years = growth_calculating.select("YEAR").distinct().toPandas()["YEAR"].sort_values().values.tolist()
        years = [str(i) for i in years]
        years_name = ["YEAR_" + i for i in years]
        # 数据长变宽
        growth_calculating = growth_calculating.groupBy("MOLECULE_STD_FOR_GR", "CITYGROUP").pivot("YEAR").agg(func.sum('VALUE')).fillna(0)
        growth_calculating = growth_calculating.select(["MOLECULE_STD_FOR_GR", "CITYGROUP"] + years)
        # 对year列名修改
        for i in range(0, len(years)):
            growth_calculating = growth_calculating.withColumnRenamed(years[i], years_name[i])
    
        # 计算得到年增长： add_gr_cols
        for i in range(0, len(years) - 1):
            growth_calculating = growth_calculating.withColumn("GR" + years[i][2:4] + years[i + 1][2:4],
                                                        growth_calculating[years_name[i + 1]] / growth_calculating[years_name[i]])
        growth_rate = growth_calculating     
        # 增长率的调整：modify_gr
        for y in [name for name in growth_rate.columns if name.startswith("GR")]:
            growth_rate = growth_rate.withColumn(y, func.when(func.isnull(growth_rate[y]) | (growth_rate[y] > 10) | (growth_rate[y] < 0.1), 1).
                                                 otherwise(growth_rate[y]))
        return growth_rate

    logger.debug('3 增长率计算')
    df_published_left = df_published_left.select('ID').distinct()
    df_published_right = df_published_right.select('ID').distinct()
    df_not_arrived = df_not_arrived.select('ID', 'DATE').distinct()
    
    l_month_hospital = df_published_left.intersect(df_published_right) \
                        .exceptAll(df_not_arrived.where(df_not_arrived.DATE == g_year * 100 + g_month).select("ID")) \
                        .toPandas()["ID"].tolist()
    
    df_raw_data_month = df_raw_data.where(col('MONTH') == g_month) \
                                   .where(col('ID').isin(l_month_hospital))
    
    # 补数
    df_growth_rate_month = calculate_growth(df_raw_data_month)
    # 标记是哪个月补数要用的growth_rate
    df_growth_rate_month = df_growth_rate_month.withColumn("MONTH_FOR_ADD", func.lit(g_month))
    df_growth_rate_month = df_growth_rate_month.withColumn("YEAR_FOR_ADD", func.lit(g_year))

    # 输出
    df_growth_rate_month = df_growth_rate_month.repartition(1)
    df_growth_rate_month.write.format("parquet") \
        .mode("append").save(p_growth_rate)



    '''
    check = spark.read.parquet('s3a://ph-max-auto/v0.0.1-2020-06-08/贝达/202012/growth_rate/')
    logger.debug(check.count())
    check.where(col('month_for_monthly_add') == 12).agg(func.sum('Year_2017'),func.sum('Year_2018'),func.sum('Year_2019'),func.sum('Year_2020'),
                             func.sum('GR1718'),func.sum('GR1819'),func.sum('GR1920')).show()
    '''

    '''
    df_growth_rate_month.agg(func.sum('YEAR_2017'),func.sum('YEAR_2018'),func.sum('YEAR_2019'),func.sum('YEAR_2020'),
                             func.sum('GR1718'),func.sum('GR1819'),func.sum('GR1920')).show()
    '''



