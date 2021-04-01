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
    model_month_right = kwargs['model_month_right']
    max_month = kwargs['max_month']
    year_missing = kwargs['year_missing']
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
    model_month_right = "201912"

    logger.debug('计算增长率-模型年历史数据')
    
    # =========== 输入 输出 =============
    
    # 输入
    if year_missing:
        year_missing = year_missing.replace(" ","").split(",")
    else:
        year_missing = []
    year_missing = [int(i) for i in year_missing]
    model_month_right = int(model_month_right)
    max_month = int(max_month)
    p_product_mapping_out = depends_path['product_mapping_out']
    
    # 测试输入
    products_of_interest_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/" + g_project_name + "/poi.csv"
    
    # 输出
    p_growth_rate = result_path_prefix + g_growth_rate

    # =========== 数据执行 =============
    logger.debug('数据执行-start')
    
    # raw_data 处理
    df_raw_data = spark.read.parquet(p_product_mapping_out)
    
    df_products_of_interest = spark.read.csv(products_of_interest_path, header=True)
    g_products_of_interest = df_products_of_interest.toPandas()["poi"].values.tolist()
    
    df_raw_data = df_raw_data.withColumn("MOLECULE_STD_FOR_GR",
                                   func.when(col("BRAND_STD").isin(g_products_of_interest), col("BRAND_STD")).
                                   otherwise(col('MOLECULE_STD')))
    
    df_raw_data = df_raw_data.where(col('YEAR') < ((model_month_right // 100) + 1))

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
    # AZ-Sanofi 要特殊处理
    if g_project_name != "Sanofi" and g_project_name != "AZ":
        growth_rate = calculate_growth(df_raw_data)
    else:
        year_missing_df = pd.DataFrame(year_missing, columns=["Year"])
        year_missing_df = spark.createDataFrame(year_missing_df)
        year_missing_df = year_missing_df.withColumn("Year", year_missing_df["Year"].cast(IntegerType()))
        # 完整年
        growth_rate_p1 = calculate_growth(df_raw_data.join(year_missing_df, on=["Year"], how="left_anti"))
        # 不完整年
        growth_rate_p2 = calculate_growth(df_raw_data.where(df_raw_data.Year.isin(year_missing + [y - 1 for y in year_missing] + [y + 1 for y in year_missing])), max_month)
    
        growth_rate = growth_rate_p1.select("MOLECULE_STD_FOR_GR", "CITYGROUP") \
            .union(growth_rate_p2.select("MOLECULE_STD_FOR_GR", "CITYGROUP")) \
            .distinct()
        growth_rate = growth_rate.join(
            growth_rate_p1.select(["MOLECULE_STD_FOR_GR", "CITYGROUP"] + [name for name in growth_rate_p1.columns if name.startswith("GR")]),
            on=["MOLECULE_STD_FOR_GR", "CITYGROUP"],
            how="left")
        growth_rate = growth_rate.join(
            growth_rate_p2.select(["MOLECULE_STD_FOR_GR", "CITYGROUP"] + [name for name in growth_rate_p2.columns if name.startswith("GR")]),
            on=["MOLECULE_STD_FOR_GR", "CITYGROUP"],
            how="left")

    growth_rate = growth_rate.repartition(2)
    growth_rate.write.format("parquet") \
        .mode("overwrite").save(p_growth_rate)
    logger.debug("输出 growth_rate：" + p_growth_rate)

    '''
    growth_rate.agg(func.sum('YEAR_2017'),func.sum('YEAR_2018'),func.sum('YEAR_2019'),
                             func.sum('GR1718'),func.sum('GR1819')).show()
    '''

    '''
    check = spark.read.parquet('s3a://ph-max-auto/v0.0.1-2020-06-08/贝达/201912/growth_rate/')
    check.agg(func.sum('Year_2017'),func.sum('Year_2018'),func.sum('Year_2019'),
                             func.sum('GR1718'),func.sum('GR1819')).show()
    '''



