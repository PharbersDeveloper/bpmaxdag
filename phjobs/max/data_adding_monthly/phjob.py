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
    g_month = kwargs['g_month']
    g_year = kwargs['g_year']
    g_current_month = kwargs['g_current_month']
    g_if_add_data = kwargs['g_if_add_data']
    depend_job_names_keys = kwargs['depend_job_names_keys']
    g_monthly_update = kwargs['g_monthly_update']
    g_max_path = kwargs['g_max_path']
    g_base_path = kwargs['g_base_path']
    ### input args ###
    
    ### output args ###
    g_adding_data = kwargs['g_adding_data']
    g_raw_data_adding_final = kwargs['g_raw_data_adding_final']
    ### output args ###

    
    
    
    import pandas as pd
    import os
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col    
    # %%
    # 测试输入
    
    # dag_name = 'Max'
    # run_id = 'max_test_beida_202012'
    # g_project_name = '贝达'
    # g_month = "12"
    # g_year = "2020"
    # g_model_month_right = '201912'
    # g_current_month = '12'
    # result_path_prefix=get_result_path({"name":job_name, "dag_name":dag_name, "run_id":run_id})
    # depends_path=get_depends_path({"name":job_name, "dag_name":dag_name, 
    #                                  "run_id":run_id, "depend_job_names_keys":depend_job_names_keys })

    # %%
    logger.debug('数据执行-start：补数-月更新')
    # 是否运行此job
    # if g_monthly_update == "False":
    #      return
        
    if g_if_add_data != "False" and g_if_add_data != "True":
        logger.error('wrong input: g_if_add_data, False or True') 
        raise ValueError('wrong input: g_if_add_data, False or True')
    
    # 输入
    g_model_month_right = int(g_model_month_right)
    p_product_mapping = depends_path['deal_poi_out']
    p_growth_rate = depends_path['growth_rate']
    p_price = depends_path['price']
    p_price_city = depends_path['price_city']
    
    # 测试输入
    g_current_month = int(g_current_month)
    
    
    # 月更新相关参数
    g_month = int(g_month)
    g_year = int(g_year)
    g_current_year = int(g_year)
    
    
    # 输出
    p_adding_data =  result_path_prefix + g_adding_data
    p_raw_data_adding_final =  result_path_prefix + g_raw_data_adding_final

    # %%
    ### jupyter测试
    p_product_mapping = p_product_mapping.replace("s3:", "s3a:")
    p_growth_rate = p_growth_rate.replace("s3:", "s3a:")
    p_price = p_price.replace("s3:", "s3a:")
    p_price_city = p_price_city.replace("s3:", "s3a:")
    
    p_adding_data = p_adding_data.replace("s3:", "s3a:")
    p_raw_data_adding_final =p_raw_data_adding_final.replace("s3:", "s3a:")
    # %%
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
    
    createView("PHARBERS", "publish_not_arrvier", "DIMENSION/MAPPING/PUBLISH_NOT_ARRIVE", time = "2021-04-14")
    createView( g_project_name, "cpa_pha_mapping", "DIMENSION/MAPPING/CPA_PHA_MAPPING", time = "2021-04-14")
    # %%
    # =========== 数据准备，测试用 =============
    # 读取cpa_pha_mapping
    df_cpa_pha_mapping = spark.sql("SELECT * FROM cpa_pha_mapping")
    # df_cpa_pha_mapping = df_cpa_pha_mapping.select(["ID", "PHA"])
    # %%
    # 读取published 和 not_arrvier
    published_not_arrvier_sql = """
            SELECT * FROM publish_not_arrvier
        """
    df_published_not_arrive= spark.sql(published_not_arrvier_sql)
    df_published_not_arrive = df_published_not_arrive.withColumn("YEAR", (df_published_not_arrive["DATE"].cast("int")/100).cast("int") )

    # %%
    # published
    df_published = df_published_not_arrive.where( col("TYPE")=="PUBLISHED" )
    df_published = df_published.where( col("VALUE")=="CPA" )
    
    # publishe选择小于 g_current_year
    df_published = df_published.where( col("YEAR")<=g_current_year)
    df_published_all = df_published.select(["ID", "DATE"])
    # %%
    # 模型前之前的未到名单（跑模型年的时候，不去除未到名单）      
    # 模型年之前到当前年的数据
    # not_arrived 
    df_not_arrived = df_published_not_arrive.where( (col("TYPE")=="NOT_ARRIVED")  )
    model_year = g_model_month_right//100
    df_not_arrived_all = df_not_arrived.where((df_not_arrived["YEAR"]>model_year)&(df_not_arrived["YEAR"]<=g_current_year)  )
    df_not_arrived_all = df_not_arrived_all.select(["ID", "DATE"])
    # %%
    
    # raw_data中每个年月的非CPA医院列表
    # df_raw_data = spark.read.parquet(p_product_mapping)
    
    #### 新的表里没有 Source这一列了
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
                                StructField('MOLECULE_STD_FOR_GR', StringType(), True)])
    df_raw_data = spark.read.format("parquet").load(p_product_mapping, schema=struct_type)
    df_raw_data = df_raw_data.withColumn("MIN_STD", func.format_string("%s|%s|%s|%s|%s", "BRAND_STD","FORM_STD",
                                            "SPECIFICATIONS_STD", "PACK_NUMBER_STD", "MANUFACTURER_STD"))
    
    df_original_range_raw_noncpa = df_raw_data.select('ID', 'YEAR_MONTH').distinct() \
                                        .withColumnRenamed('YEAR_MONTH', 'DATE')
    
    
    # 出版医院 减去 未到名单(月更)
    df_original_range_raw = df_published_all.join(df_not_arrived_all, on=['ID', 'DATE'], how='left_anti')
    
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
    
    
    ## 读取 price
    # df_price = spark.read.parquet(p_price)
    struct_type_price = StructType( [   StructField('MIN_STD', StringType(), True),
                                        StructField('YEAR_MONTH', IntegerType(), True),
                                        StructField('CITY_TIER', DoubleType(), True),
                                        StructField('PRICE', DoubleType(), True) ])
    df_price = spark.read.format("parquet").load(p_price, schema=struct_type_price )
    df_price = df_price.withColumnRenamed('PRICE', 'PRICE_TIER')
    
    ## 读取 growth_rate
    df_growth_rate = spark.read.parquet(p_growth_rate)
    df_growth_rate = df_growth_rate.where(col('YEAR_MONTH_FOR_ADD') == (g_year*100 + g_month))
    df_growth_rate.persist()
    
    ## 读取 price_city
    # df_price_city = spark.read.parquet(p_price_city)
    struct_type_price_city = StructType( [  StructField('MIN_STD', StringType(), True),
                                            StructField('YEAR_MONTH', IntegerType(), True),
                                            StructField('CITY',StringType(), True),
                                            StructField('PROVINCE', StringType(), True),
                                            StructField('PRICE', DoubleType(), True) ] )
    df_price_city = spark.read.format("parquet").load(p_price_city, schema=struct_type_price_city)
    df_price_city = df_price_city.withColumnRenamed('PRICE', 'PRICE_CITY')

    # %%
    # === 补数函数 === 
    schema = StructType([
                StructField("PHA", StringType(), True),
                StructField("MONTH", IntegerType(), True),
                StructField("YEAR", IntegerType(), True)
                ])
    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def pudf_minWeightYear(pdf):
        minYEAR = int(pdf.sort_values(["WEIGHT"],ascending=True).head(1).reset_index(drop=True)['YEAR'][0])
        PHA = pdf["PHA"][0]
        MONTH = pdf["MONTH"][0]
        return pd.DataFrame([[PHA] + [MONTH] + [minYEAR]], columns=["PHA", "MONTH", "YEAR"])
    
    
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
        # orderBy 后 取 first 在 zhb的机器上跑回有随机取值发生
        # df_current_range_for_add = df_other_years_range.repartition(1).orderBy(col('WEIGHT').asc())
        # df_current_range_for_add = df_current_range_for_add.groupBy("PHA", "MONTH") \
        #                                            .agg(func.first(col('YEAR')).alias("YEAR"))
        df_current_range_for_add = df_other_years_range.select("PHA", "MONTH", "YEAR", "WEIGHT") \
                                                        .groupBy("PHA", "MONTH").apply(pudf_minWeightYear)
    
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
        # df_current_adding_data = df_current_adding_data.withColumn("YEAR_MONTH", col("YEAR_MONTH").cast(DoubleType()))
    
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
    # 2. 执行函数 addDate, 月更新每月分别补数，每次补数1个月
    if g_if_add_data == "True":
        df_raw_data_month = df_raw_data.where(col('MONTH') == g_month)
        df_growth_rate_month = df_growth_rate.where(df_growth_rate.YEAR_MONTH_FOR_ADD == (g_year*100 + g_month))
    
        # 补数：addDate
        df_adding_data = addDate(df_raw_data_month, df_growth_rate_month)[0]
        
        # =========== 输出 =============   
        df_adding_data = df_adding_data.repartition(1)
        df_adding_data.write.format("parquet").partitionBy("YEAR_MONTH") \
                        .mode("append").save(p_adding_data)
        
    #  # 加快计算debug
    # df_adding_data.persist()   
    # df_adding_data.agg(func.sum('SALES'), func.sum('UNITS')).show()
    # %%
    # 3. 合并补数部分和原始部分:: 只有当前年当前月的结果
    if g_if_add_data == "True":
        df_raw_data_adding = (df_raw_data.withColumn("ADD_FLAG", func.lit(0))) \
            .union(df_adding_data.withColumn("ADD_FLAG", func.lit(1)).select(df_raw_data.columns + ["ADD_FLAG"]))
    else:
        df_raw_data_adding = df_raw_data.withColumn("ADD_FLAG", func.lit(0))
    
    df_raw_data_adding_final = df_raw_data_adding \
            .where((col('YEAR') == g_year) & (col('MONTH') == g_month))

    # %%
    # =========== 输出 =============
    df_raw_data_adding_final = df_raw_data_adding_final.repartition(1)
    df_raw_data_adding_final.write.format("parquet").partitionBy("YEAR_MONTH") \
                        .mode("append").save(p_raw_data_adding_final)
    
    logger.debug("输出 raw_data_adding_final：" + p_raw_data_adding_final)
    
    logger.debug('数据执行-Finish')

    # %%
    # check = spark.read.parquet('s3a://ph-max-auto/v0.0.1-2020-06-08/贝达/202012_test/raw_data_adding_final/')
    # check.where(col('Year')==2020).where(col('Month')==12).groupby('add_flag').agg(func.sum('Sales'), func.sum('Units')).show()

    # %%
    # df_raw_data_adding_final.groupby('ADD_FLAG').agg(func.sum('SALES'), func.sum('UNITS')).show()
    # df_raw_data_adding_final.show(1, vertical=True)

    # %%
    # df_data_old = spark.read.parquet("s3a://ph-max-auto/2020-08-11/Max/refactor/runs/max_test_beida_202012_bk/data_adding_monthly/raw_data_adding_final")
    # print (df_data_old.distinct().count() ) 
    # df_data_old.select("YEAR").distinct().show()
    
    # df_data_old = df_data_old.withColumnRenamed("SALES", "SALES_OLD")\
    #                             .withColumnRenamed("UNITS", "UNITS_OLD").distinct()
    
    
    # compare = df_raw_data_adding_final.join( df_data_old, on=["PHA", "ID", "MIN_STD", "YEAR", "MONTH", "PROVINCE", "CITY", "MOLECULE_STD" ] ,how="inner")
    # print(df_raw_data_adding_final.count(), df_data_old.count(), compare.count() )
    # compare.withColumn("Error", compare["SALES"]- compare["SALES_OLD"] ).select("Error").distinct().collect()
    # compare.withColumn("Error", compare["UNITS"]- compare["UNITS_OLD"] ).select("Error").distinct().collect()
    
    ###### 有些会存在 province 和 city 为 NULL的情况
    # compare = df_raw_data_adding_final.join( df_data_old, on=["PHA", "ID", "MIN_STD", "YEAR", "MONTH", "PROVINCE", "CITY", "MOLECULE_STD" ] ,how="anti")
    # compare.show()

