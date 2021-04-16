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
    g_year = kwargs['g_year']
    g_model_month_right = kwargs['g_model_month_right']
    depend_job_names_keys = kwargs['depend_job_names_keys']
    g_monthly_update = kwargs['g_monthly_update']
    dag_name = kwargs['dag_name']
    run_id = kwargs['run_id']
    max_path = kwargs['max_path']
    ### input args ###
    
    ### output args ###
    g_adding_data = kwargs['g_adding_data']
    g_raw_data_adding_final = kwargs['g_raw_data_adding_final']
    g_new_hospital = kwargs['g_new_hospital']
    ### output args ###

    import pandas as pd
    import os
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col    # %%
    # 测试输入
    '''
    g_project_name = '贝达'
    g_year = "2019"
    g_model_month_right = '201912'
    '''
    # %%
    # 是否运行此job
    if g_monthly_update == "True":
         return
    
    logger.debug('数据执行-start：补数-模型')
    # 输入
    p_product_mapping = depends_path['product_mapping_out']
    p_growth_rate = depends_path['growth_rate']
    p_price = depends_path['price']
    p_price_city = depends_path['price_city']
    g_model_month_right = int(g_model_month_right)
    # 测试输入
    p_products_of_interest = max_path + "/" + g_project_name + "/poi.csv"
    p_cpa_pha_mapping = max_path + "/" + g_project_name + "/cpa_pha_mapping"
    
    # 跑模型年年份要小于等于g_model_month_right，只需要输入哪些年要补数
    g_year = int(g_year)
    
    # 输出
    p_adding_data =  result_path_prefix + g_adding_data
    p_raw_data_adding_final =  result_path_prefix + g_raw_data_adding_final
    p_new_hospital =  result_path_prefix + g_new_hospital
    # %%
    # =========== 数据准备，测试用 =============
    # products_of_interest 文件
    df_products_of_interest = spark.read.csv(p_products_of_interest, header=True)
    df_products_of_interest = df_products_of_interest.withColumnRenamed('poi', 'POI')
    
    def dealIDlength(df):
        # ID不足7位的补足0到6位
        # 国药诚信医院编码长度是7位数字，cpa医院编码是6位数字。
        df = df.withColumn("ID", df["ID"].cast(StringType()))
        # 去掉末尾的.0
        df = df.withColumn("ID", func.regexp_replace("ID", "\\.0", ""))
        df = df.withColumn("ID", func.when(func.length(df.ID) < 7, func.lpad(df.ID, 6, "0")).otherwise(df.ID))
        return df
    
    # cpa_pha_mapping 文件
    df_cpa_pha_mapping = spark.read.parquet(p_cpa_pha_mapping)
    df_cpa_pha_mapping = df_cpa_pha_mapping.withColumnRenamed('推荐版本', 'COMMEND')
    df_cpa_pha_mapping = df_cpa_pha_mapping.select('COMMEND', 'ID', 'PHA').where(col("COMMEND") == 1)
    df_cpa_pha_mapping = dealIDlength(df_cpa_pha_mapping)
    # %%
    # =========== 数据准备 =============
    def unpivot(df, keys):
        # 功能：数据宽变长
        # 参数说明 df:dataframe,  keys 待转换表中需要保留的主键key，以list[]类型传入
        # 转换是为了避免字段类不匹配，统一将数据转换为string类型，如果保证数据类型完全一致，可以省略该句
        df = df.select(*[col(_).astype("string") for _ in df.columns])
        cols = [_ for _ in df.columns if _ not in keys]
        stack_str = ','.join(map(lambda x: "'%s', `%s`" % (x, x), cols))
        # feature, value 转换后的列名，可自定义
        df = df.selectExpr(*keys, "stack(%s, %s) as (feature, value)" % (len(cols), stack_str))
        return df
    
    # 一. 生成 df_original_range_raw（样本中已到的Year、Month、PHA的搭配）
    # 2017到当前年的全量出版医院
    Published_years = list(range(2017, g_year+1, 1))
    for index, eachyear in enumerate(Published_years):
        allmonth = [str(eachyear*100 + i) for i in list(range(1,13,1))]
        published_path = max_path + "/Common_files/Published"+str(eachyear)+".csv"
        published = spark.read.csv(published_path, header=True)
        published = published.where(col('Source') == 'CPA').select('ID').distinct()
        published = dealIDlength(published)
        for i in allmonth:
            published = published.withColumn(i, func.lit(1))
        if index == 0:
            published_full = published
        else:
            published_full = published_full.join(published, on='ID', how='full')
    
    df_published_all = unpivot(published_full, ['ID'])
    df_published_all = df_published_all.where(col('value')==1).withColumnRenamed('feature', 'Date') \
                                .drop('value')
    
    
    # raw_data中每个年月的非CPA医院列表
    df_raw_data = spark.read.parquet(p_product_mapping)        
    df_original_range_raw_noncpa = df_raw_data.where(col('Source') != 'CPA').select('ID', 'YEAR_MONTH').distinct() \
                                        .withColumnRenamed('YEAR_MONTH', 'DATE')
    
    # 模型前之前的未到名单（跑模型年的时候，不去除未到名单） 
    df_original_range_raw = df_published_all
    
    # 与 非CPA医院 合并
    df_original_range_raw = df_original_range_raw.union(df_original_range_raw_noncpa.select(df_original_range_raw.columns))
        
    # 匹配 PHA
    df_cpa_pha_mapping = df_cpa_pha_mapping.select("ID", "PHA").distinct()
    
    df_original_range_raw = df_original_range_raw.join(df_cpa_pha_mapping, on='ID', how='left')
    df_original_range_raw = df_original_range_raw.where(~col('PHA').isNull()) \
                                            .withColumn('YEAR', func.substring(col('DATE'), 0, 4)) \
                                            .withColumn('MONTH', func.substring(col('DATE'), 5, 2).cast(IntegerType())) \
                                            .select('PHA', 'YEAR', 'MONTH').distinct()
    # %%
    # =========== 数据执行 =============
    logger.debug('数据执行-start')
    # 1.数据准备
    df_raw_data = spark.read.parquet(p_product_mapping)
    
    g_products_of_interest = df_products_of_interest.toPandas()["POI"].values.tolist()
    
    # df_raw_data 处理
    df_raw_data = df_raw_data.withColumn("MOLECULE_STD_FOR_GR",
                                   func.when(col("BRAND_STD").isin(g_products_of_interest), col("BRAND_STD")).
                                   otherwise(col('MOLECULE_STD')))
    
    df_price = spark.read.parquet(p_price)
    df_price = df_price.withColumnRenamed('PRICE', 'PRICE_TIER')
    
    df_growth_rate = spark.read.parquet(p_growth_rate)
    df_growth_rate.persist()
    
    df_price_city = spark.read.parquet(p_price_city)
    df_price_city = df_price_city.withColumnRenamed('PRICE', 'PRICE_CITY')
    
    # model df_raw_data 处理
    df_raw_data = df_raw_data.where(col('YEAR') < ((g_model_month_right // 100) + 1))
    # df_raw_data = df_raw_data.where(col('YEAR') == g_year)
    # %%
    # 补数函数
    def addDate(df_raw_data, df_growth_rate):
        # 1. 原始数据格式整理， 用于补数
        df_growth_rate = df_growth_rate.select(["CITYGROUP", "MOLECULE_STD_FOR_GR"] + 
                                               [name for name in df_growth_rate.columns if name.startswith("GR")]).distinct()
        
        df_raw_data_for_add = df_raw_data.where(col('PHA').isNotNull()) \
                                        .orderBy(col('YEAR').desc()) \
                                        .withColumnRenamed("CITY_TIER", "CITYGROUP") \
                                        .join(df_growth_rate, on=["MOLECULE_STD_FOR_GR", "CITYGROUP"], how="left")
        df_raw_data_for_add.persist()
    
        # 2. 获取所有发表医院
        # 原始数据的 PHA-Month-YEAR
        # original_range = df_raw_data_for_add.select("YEAR", "Month", "PHA").distinct()
    
        years = df_raw_data_for_add.select("YEAR").distinct() \
                                .orderBy(df_raw_data_for_add.YEAR) \
                                .toPandas()["YEAR"].values.tolist()
        
        df_original_range = df_original_range_raw.where(col('YEAR').isin(years))
    
        growth_rate_index = [i for i, name in enumerate(df_raw_data_for_add.columns) if name.startswith("GR")]
    
        # 3.对每年的缺失数据分别进行补数
        # 当前年：每月publish的PHA
        df_current_range_pha_month = df_original_range.where(col('YEAR') == g_year) \
                                                    .select("MONTH", "PHA").distinct()
        # 当前年：publish的月份
        df_current_range_month = df_current_range_pha_month.select("MONTH").distinct()
        # 其他年：月份-当前年publish的月份，PHA-当前年没有publish的医院（这些医院需要补数）
        df_other_years_range = df_original_range.where(col('YEAR') != g_year) \
                                            .join(df_current_range_month, on="MONTH", how="inner") \
                                            .join(df_current_range_pha_month, on=["MONTH", "PHA"], how="left_anti")
        # 其他年与当前年的年份差值，比重计算（临近上一年比重为0.5，临近后一年比重为1）
        df_other_years_range = df_other_years_range \
            .withColumn("TIME_DIFF", (col('YEAR') - g_year)) \
            .withColumn("WEIGHT", func.when((col('YEAR') > g_year), (col('YEAR') - g_year - 0.5)).
                        otherwise(col('YEAR') * (-1) + g_year))
        # 选择比重最小的年份：用于补数的 PHA-Month-Year
        df_current_range_for_add = df_other_years_range.repartition(1).orderBy(col('WEIGHT').asc())
        df_current_range_for_add = df_current_range_for_add.groupBy("PHA", "MONTH") \
                                                    .agg(func.first(col('YEAR')).alias("YEAR"))
    
        # 从 rawdata 根据 df_current_range_for_add 获取用于补数的数据
        df_current_raw_data_for_add = df_raw_data_for_add.where(col('YEAR') != g_year) \
                                                .join(df_current_range_for_add, on=["MONTH", "PHA", "YEAR"], how="inner")
        df_current_raw_data_for_add = df_current_raw_data_for_add \
                                            .withColumn("TIME_DIFF", (col('YEAR') - g_year)) \
                                            .withColumn("WEIGHT", func.when((col('YEAR') > g_year), (col('YEAR') - g_year - 0.5)).
                                                        otherwise(col('YEAR') * (-1) + g_year))
    
        # 当前年与(当前年+1)的增长率所在列的index
        base_index = g_year - min(years) + min(growth_rate_index)
        df_current_raw_data_for_add = df_current_raw_data_for_add.withColumn("SALES_BK", col('SALES'))
    
        # 为补数计算增长率
        df_current_raw_data_for_add = df_current_raw_data_for_add \
            .withColumn("MIN_INDEX", func.when((col('YEAR') < g_year), (col('TIME_DIFF') + base_index)).
                        otherwise(base_index)) \
            .withColumn("MAX_INDEX", func.when((col('YEAR') < g_year), (base_index - 1)).
                        otherwise(col('TIME_DIFF') + base_index - 1)) \
            .withColumn("TOTAL_GR", func.lit(1))
        
        # 多年有数的会对增长率进行累计计算
        for i in growth_rate_index:
            col_name = df_current_raw_data_for_add.columns[i]
            df_current_raw_data_for_add = df_current_raw_data_for_add.withColumn(col_name, func.when((col('MIN_INDEX') > i) | (col('MAX_INDEX') < i), 1).
                                                         otherwise(df_current_raw_data_for_add[col_name]))
            df_current_raw_data_for_add = df_current_raw_data_for_add.withColumn(col_name, func.when(col('YEAR') > g_year, col(col_name) ** (-1)).
                                                         otherwise(df_current_raw_data_for_add[col_name]))
            df_current_raw_data_for_add = df_current_raw_data_for_add.withColumn("TOTAL_GR", col('TOTAL_GR') * col(col_name))
    
        df_current_raw_data_for_add = df_current_raw_data_for_add.withColumn("FINAL_GR", func.when(col('TOTAL_GR') < 2, col('TOTAL_GR')).
                                                     otherwise(2))
    
        # 为当前年的缺失数据补数：根据增长率计算 SALES，匹配 price，计算 UNITS=SALES/price
        df_current_adding_data = df_current_raw_data_for_add \
            .withColumn("SALES", col('SALES') * col('FINAL_GR')) \
            .withColumn("YEAR", func.lit(g_year))
        df_current_adding_data = df_current_adding_data.withColumn("YEAR_MONTH", col('YEAR') * 100 + col('MONTH'))
        df_current_adding_data = df_current_adding_data.withColumn("YEAR_MONTH", col("YEAR_MONTH").cast(DoubleType()))
    
        df_current_adding_data = df_current_adding_data.withColumnRenamed("CITYGROUP", "CITY_TIER") \
                                    .join(df_price, on=["MIN_STD", "YEAR_MONTH", "CITY_TIER"], how="inner") \
                                    .join(df_price_city, on=["MIN_STD", "YEAR_MONTH", "CITY", "PROVINCE"], how="left")
    
        df_current_adding_data = df_current_adding_data.withColumn('PRICE', func.when(col('PRICE_CITY').isNull(), 
                                                                                col('PRICE_TIER')) \
                                                                         .otherwise(col('PRICE_CITY')))
    
        df_current_adding_data = df_current_adding_data.withColumn("UNITS", func.when(col('SALES') == 0, 0).
                                                     otherwise(col('SALES') / col('PRICE'))) \
                                                    .na.fill({'UNITS': 0})
    
        return df_current_adding_data, df_original_range
    # %%
    logger.debug('补数')
    # 2. 执行函数 addDate, model补数不按月份
    add_data_out = addDate(df_raw_data, df_growth_rate)
    
    df_adding_data = add_data_out[0]
    df_original_range = add_data_out[1]
    
    # 输出
    df_adding_data = df_adding_data.repartition(1)
    df_adding_data.write.format("parquet") \
        .mode("overwrite").save(p_adding_data)
    
    df_adding_data = spark.read.parquet(p_adding_data)
    # %%
    # 3. 合并补数部分和原始部分:
    df_raw_data_adding = (df_raw_data.withColumn("ADD_FLAG", func.lit(0))) \
                    .union(df_adding_data.withColumn("ADD_FLAG", func.lit(1)).select(df_raw_data.columns + ["ADD_FLAG"]))
    # %%
    # 4. 进一步为最后一年独有的医院补最后一年的缺失月数据:
    years = df_original_range.select("YEAR").distinct() \
                        .orderBy(df_original_range.YEAR) \
                        .toPandas()["YEAR"].values.tolist()
    
    # 只在最新一年出现的医院
    df_new_hospital = (df_original_range.where(col('YEAR') == max(years)).select("PHA").distinct()) \
                        .subtract(df_original_range.where(col('YEAR') != max(years)).select("PHA").distinct())
    logger.debug("以下是最新一年出现的医院:" + str(df_new_hospital.toPandas()["PHA"].tolist()))
    
    # 最新一年没有的月份
    missing_months = (df_original_range.where(col('YEAR') != max(years)).select("MONTH").distinct()) \
                    .subtract(df_original_range.where(col('YEAR') == max(years)).select("MONTH").distinct())
    
    # 如果最新一年有缺失月份，需要处理
    if missing_months.count() == 0:
        logger.debug("missing_months=0")
        df_raw_data_adding_final = df_raw_data_adding
    else:
        number_of_existing_months = 12 - missing_months.count()
        # 用于groupBy的列名：df_raw_data_adding列名去除list中的列名
        group_columns = set(df_raw_data_adding.columns) \
                            .difference(set(['MONTH', 'SALES', 'UNITS', "YEAR_MONTH"]))
        # 补数重新计算
        df_adding_data_new = df_raw_data_adding \
                            .where(col('ADD_FLAG') == 1) \
                            .where(col('PHA').isin(df_new_hospital["PHA"].tolist())) \
                            .groupBy(list(group_columns)).agg({"SALES": "sum", "UNITS": "sum"})
        df_adding_data_new = df_adding_data_new \
                            .withColumn("SALES", col("sum(SALES)") / number_of_existing_months) \
                            .withColumn("UNITS", col("sum(UNITS)") / number_of_existing_months) \
                            .crossJoin(missing_months)
        # 生成最终补数结果
        same_names = list(set(df_raw_data_adding.columns).intersection(set(df_adding_data_new.columns)))
        df_raw_data_adding_final = df_raw_data_adding.select(same_names) \
            .union(df_adding_data_new.select(same_names))
    
    # 保留当前补数年的结果
    df_raw_data_adding_final = df_raw_data_adding.where((col('YEAR') == g_year))
    # %%
    # =========== 输出 =============
    df_new_hospital = df_new_hospital.repartition(2)
    df_new_hospital.write.format("parquet") \
        .mode("overwrite").save(p_new_hospital)
    
    logger.debug("输出 new_hospital：" + p_new_hospital)
    
    
    # 输出补数结果 df_raw_data_adding_final
    df_raw_data_adding_final = df_raw_data_adding_final.repartition(2)
    df_raw_data_adding_final.write.format("parquet") \
        .mode("overwrite").save(p_raw_data_adding_final)
    
    logger.debug("输出 raw_data_adding_final：" + p_raw_data_adding_final)
    
    logger.debug('数据执行-Finish')
    # %%
    # df_raw_data_adding_final.groupby('add_flag').agg(func.sum('SALES'),func.sum('UNITS')).show()
    # %%
    # check = spark.read.parquet('s3a://ph-max-auto/v0.0.1-2020-06-08/贝达/201912_test/raw_data_adding_final/')
    # check = check.where(col('Year')==2019).groupby('add_flag').agg(func.sum('Sales'), func.sum('Units')).show()
