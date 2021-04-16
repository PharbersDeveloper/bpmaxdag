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
    g_model_month_right = kwargs['g_model_month_right']
    g_max_month = kwargs['g_max_month']
    g_year_missing = kwargs['g_year_missing']
    g_monthly_update = kwargs['g_monthly_update']
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
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col    # %%
    # 测试用
    '''
    g_project_name = '贝达'
    g_model_month_right = "201912"
    '''
    # %%
    logger.debug('数据执行-start：计算增长率-模型年历史数据')
    # 是否运行此job
    if g_monthly_update == "True":
         return
    
    # =========== 输入 输出 =============
    # 输入
    if g_year_missing:
        g_year_missing = g_year_missing.replace(" ","").split(",")
    else:
        g_year_missing = []
    g_year_missing = [int(i) for i in g_year_missing]
    g_model_month_right = int(g_model_month_right)
    g_max_month = int(g_max_month)
    p_product_mapping_out = depends_path['product_mapping_out']
    
    # 测试输入
    products_of_interest_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/" + g_project_name + "/poi.csv"
    
    # 输出
    p_growth_rate = result_path_prefix + g_growth_rate
    # %%
    # =========== 数据执行 =============
    # raw_data 处理（products_of_interest 可以写成参数，在前一个job筛选）
    df_raw_data = spark.read.parquet(p_product_mapping_out)
    
    df_products_of_interest = spark.read.csv(products_of_interest_path, header=True)
    g_products_of_interest = df_products_of_interest.toPandas()["poi"].values.tolist()
    
    df_raw_data = df_raw_data.withColumn("MOLECULE_STD_FOR_GR",
                                   func.when(col("BRAND_STD").isin(g_products_of_interest), col("BRAND_STD")).
                                   otherwise(col('MOLECULE_STD')))
    
    df_raw_data = df_raw_data.where(col('YEAR') < ((g_model_month_right // 100) + 1))
    # %%
    # 计算样本分子增长率
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
    # AZ-Sanofi 要特殊处理
    if g_project_name != "Sanofi" and g_project_name != "AZ":
        df_growth_rate = calculateGrowth(df_raw_data)
    else:
        year_missing_df = pd.DataFrame(g_year_missing, columns=["Year"])
        year_missing_df = spark.createDataFrame(year_missing_df)
        year_missing_df = year_missing_df.withColumn("Year", col("Year").cast(IntegerType()))
        # 完整年
        df_growth_rate_p1 = calculateGrowth(df_raw_data.join(year_missing_df, on=["Year"], how="left_anti"))
        # 不完整年
        df_growth_rate_p2 = calculateGrowth(
            df_raw_data.where(df_raw_data.Year.isin(g_year_missing + [y - 1 for y in g_year_missing] + [y + 1 for y in g_year_missing])), 
            g_max_month)
    
        df_growth_rate = df_growth_rate_p1.select("MOLECULE_STD_FOR_GR", "CITYGROUP") \
            .union(df_growth_rate_p2.select("MOLECULE_STD_FOR_GR", "CITYGROUP")) \
            .distinct()
        df_growth_rate = df_growth_rate.join(
            df_growth_rate_p1.select(["MOLECULE_STD_FOR_GR", "CITYGROUP"] + [name for name in df_growth_rate_p1.columns if name.startswith("GR")]),
            on=["MOLECULE_STD_FOR_GR", "CITYGROUP"],
            how="left")
        df_growth_rate = df_growth_rate.join(
            df_growth_rate_p2.select(["MOLECULE_STD_FOR_GR", "CITYGROUP"] + [name for name in df_growth_rate_p2.columns if name.startswith("GR")]),
            on=["MOLECULE_STD_FOR_GR", "CITYGROUP"],
            how="left")
    # %%
    df_growth_rate = df_growth_rate.repartition(2)
    df_growth_rate.write.format("parquet") \
        .mode("overwrite").save(p_growth_rate)
    logger.debug("输出 growth_rate：" + p_growth_rate)
    
    logger.debug('数据执行-Finish')
    # %%
    '''
    logger.debug(df_growth_rate)
    logger.debug(df_growth_rate.count())
    df_growth_rate.agg(func.sum('YEAR_2017'),func.sum('YEAR_2018'),func.sum('YEAR_2019'),
                             func.sum('GR1718'),func.sum('GR1819')).show()
    '''
    # %%
    '''
    check = spark.read.parquet('s3a://ph-max-auto/v0.0.1-2020-06-08/贝达/201912_test/growth_rate/')
    check.agg(func.sum('Year_2017'),func.sum('Year_2018'),func.sum('Year_2019'),
                             func.sum('GR1718'),func.sum('GR1819')).show()
    '''
    # %%
    '''
    check2 = check.withColumnRenamed("S_Molecule_for_gr", "MOLECULE_STD_FOR_GR" )\
                    .withColumnRenamed("CITYGROUP", "CITYGROUP") \
                    .withColumnRenamed("GR1819", "GR1819_check")
    compare  = df_growth_rate.select("CITYGROUP", "MOLECULE_STD_FOR_GR", "GR1819") \
                            .join( check2.select("CITYGROUP", "MOLECULE_STD_FOR_GR", "GR1819_check"), 
                                  on=["MOLECULE_STD_FOR_GR", "CITYGROUP"], how="left" )
    compare_result = compare.withColumn("sales_error", compare["GR1819_check"] - compare["GR1819"])
    compare_result.where( compare_result.sales_error !=0.0).count()
    '''
