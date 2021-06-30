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
    project_name = kwargs['project_name']
    model_month_right = kwargs['model_month_right']
    max_month = kwargs['max_month']
    year_missing = kwargs['year_missing']
    current_year = kwargs['current_year']
    first_month = kwargs['first_month']
    current_month = kwargs['current_month']
    if_others = kwargs['if_others']
    monthly_update = kwargs['monthly_update']
    if_add_data = kwargs['if_add_data']
    out_path = kwargs['out_path']
    run_id = kwargs['run_id']
    owner = kwargs['owner']
    g_input_version = kwargs['g_input_version']
    g_database_temp = kwargs['g_database_temp']
    g_database_input = kwargs['g_database_input']
    ### input args ###
    
    ### output args ###
    g_out_price = kwargs['g_out_price']
    g_out_price_city = kwargs['g_out_price_city']
    g_out_growth_rate = kwargs['g_out_growth_rate']
    ### output args ###

    
    
    from pyspark.sql import SparkSession, Window
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    import os
    import pandas as pd
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col    
    import json
    import boto3    # %%
    # project_name = 'Empty'
    # model_month_right = 0
    # max_month = 0
    # year_missing = 0
    # current_year = 2020
    # first_month = 'Empty'
    # current_month = 'Empty'
    # if_others = 'False'
    # monthly_update = 'Empty'
    # not_arrived_path = 'Empty'
    # published_path = 'Empty'
    # if_add_data = 'True'
    # %% 
    # 输入参数设置
    logger.debug('job3_data_adding')
    if if_add_data != "False" and if_add_data != "True":
        logger.debug('wrong input: if_add_data, False or True') 
        raise ValueError('wrong input: if_add_data, False or True')
    
    if monthly_update != "False" and monthly_update != "True":
        logger.debug('wrong input: monthly_update, False or True') 
        raise ValueError('wrong input: monthly_update, False or True')
    
    if year_missing:
        year_missing = year_missing.replace(" ","").split(",")
    else:
        year_missing = []    
    year_missing = [int(i) for i in year_missing]
    model_month_right = int(model_month_right)
    max_month = int(max_month)
    
    dict_input_version = json.loads(g_input_version)
    logger.debug(dict_input_version)
    
    # 月更新相关参数
    if monthly_update == "True":
        current_year = int(current_year)
        first_month = int(first_month)
        current_month = int(current_month)
    else:
        current_year = model_month_right//100
        
    # 输出
    p_out_price = out_path + g_out_price
    p_out_price_city = out_path + g_out_price_city
    p_out_growth_rate = out_path + g_out_growth_rate

    # %% 
    # 输入数据读取
    df_raw_data = spark.sql("SELECT * FROM %s.product_mapping_out WHERE version='%s' AND provider='%s' AND  owner='%s'" 
                         %(g_database_temp, run_id, project_name, owner))
    
    df_poi =  spark.sql("SELECT * FROM %s.poi WHERE provider='%s' AND version='%s'" 
                             %(g_database_input, project_name, dict_input_version['poi']))
    
    if monthly_update == "True":   
        df_published =  spark.sql("SELECT * FROM %s.published WHERE provider='common' AND version IN %s" 
                                 %(g_database_input, tuple(dict_input_version['published'].replace(' ','').split(','))))
    
        df_not_arrived =  spark.sql("SELECT * FROM %s.not_arrived WHERE provider='common' AND version IN %s" 
                                 %(g_database_input, tuple(dict_input_version['not_arrived'].replace(' ','').split(','))))

    # %% 
    # =========== 数据清洗 =============
    def deal_ID_length(df):
        # ID不足7位的补足0到6位
        # 国药诚信医院编码长度是7位数字，cpa医院编码是6位数字，其他还有包含字母的ID
        df = df.withColumn("ID", df["ID"].cast(StringType()))
        # 去掉末尾的.0
        df = df.withColumn("ID", func.regexp_replace("ID", "\\.0", ""))
        df = df.withColumn("ID", func.when(func.length(df.ID) < 7, func.lpad(df.ID, 6, "0")).otherwise(df.ID))
        return df
    
    # 1、选择标准列
    df_published = df_published.select('id', 'source', 'year').distinct()
    df_not_arrived = df_not_arrived.select('id', 'date').distinct()
    df_poi = df_poi.select('poi').distinct()
    df_raw_data = df_raw_data.drop('version', 'provider', 'owner')
    
    # 2、ID列补位
    df_published = deal_ID_length(df_published)
    df_not_arrived = deal_ID_length(df_not_arrived)

    # %%
    # =========== 函数定义：输出结果 =============
    def createPartition(p_out):
        # 创建分区
        logger.debug('创建分区')
        Location = p_out + '/version=' + run_id + '/provider=' + project_name + '/owner=' + owner
        g_out_table = p_out.split('/')[-1]
        
        partition_input_list = [{
         "Values": [run_id, project_name,  owner], 
        "StorageDescriptor": {
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
            }, 
            "Location": Location, 
        } 
            }]    
        client = boto3.client('glue')
        glue_info = client.batch_create_partition(DatabaseName=g_database_temp, TableName=g_out_table, PartitionInputList=partition_input_list)
        logger.debug(glue_info)
        
    def outResult(df, p_out):
        df = df.withColumn('version', func.lit(run_id)) \
                .withColumn('provider', func.lit(project_name)) \
                .withColumn('owner', func.lit(owner))
        df.repartition(1).write.format("parquet") \
                 .mode("append").partitionBy("version", "provider", "owner") \
                 .parquet(p_out)

    # %%
    # =========== 数据执行 =============
    logger.debug('数据执行-start')
    
    # 数据读取
    products_of_interest = df_poi.toPandas()["poi"].values.tolist()
    # raw_data 处理
    raw_data = df_raw_data.withColumn("S_Molecule_for_gr",
                                   func.when(col("标准商品名").isin(products_of_interest), col("标准商品名")).
                                   otherwise(col('S_Molecule')))

    # %%
    # ==== 计算价格 ====
    logger.debug('1 价格计算')
    
    # 补数部分的数量需要用价格得出
    # 1、City_Tier 层面：
    price = raw_data.groupBy("min2", "date", "City_Tier_2010") \
        .agg((func.sum("Sales") / func.sum("Units")).alias("Price"))
    price2 = raw_data.groupBy("min2", "date") \
        .agg((func.sum("Sales") / func.sum("Units")).alias("Price2"))
    price = price.join(price2, on=["min2", "date"], how="left")
    price = price.withColumn("Price", func.when(func.isnull(price.Price), price.Price2).
                             otherwise(price.Price))
    price = price.withColumn("Price", func.when(func.isnull(price.Price), func.lit(0)).
                             otherwise(price.Price)) \
        .drop("Price2")
    
    # 2、城市层面
    price_city = raw_data.groupBy("min2", "date", 'City', 'Province') \
                        .agg((func.sum("Sales") / func.sum("Units")).alias("Price"))
    price_city = price_city.where(~price_city.Price.isNull())
    
    # ==== ** 输出 ** ====
    outResult(price, p_out_price)
    outResult(price_city, p_out_price_city)
    
    logger.debug("输出 price：" + p_out_price)
    logger.debug("输出 price_city：" + p_out_price_city)

    # %%
    # ==== 计算增长率 ====
    def calculate_growth(raw_data, max_month=12):
        # TODO: 完整年用完整年增长，不完整年用不完整年增长
        if max_month < 12:
            raw_data = raw_data.where(raw_data.Month <= max_month)
        # raw_data 处理
        growth_raw_data = raw_data.na.fill({"City_Tier_2010": 5.0})
        growth_raw_data = growth_raw_data.withColumn("CITYGROUP", col('City_Tier_2010'))
        # 增长率计算过程
        growth_calculating = growth_raw_data.groupBy("S_Molecule_for_gr", "CITYGROUP", "Year") \
                                                .agg(func.sum('Sales').alias("value"))
        years = growth_calculating.select("Year").distinct().toPandas()["Year"].sort_values().values.tolist()
        years = [str(i) for i in years]
        years_name = ["Year_" + i for i in years]
        # 数据长变宽
        growth_calculating = growth_calculating.groupBy("S_Molecule_for_gr", "CITYGROUP").pivot("Year").agg(func.sum('value')).fillna(0)
        growth_calculating = growth_calculating.select(["S_Molecule_for_gr", "CITYGROUP"] + years)
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

    # %%
    logger.debug('3 增长率计算')
    
    if monthly_update == "False":
        raw_data = raw_data.where(raw_data.Year < ((model_month_right // 100) + 1))
        # AZ-Sanofi 要特殊处理
        if project_name != "Sanofi" and project_name != "AZ":
            growth_rate = calculate_growth(raw_data)
        else:
            year_missing_df = pd.DataFrame(year_missing, columns=["Year"])
            year_missing_df = spark.createDataFrame(year_missing_df)
            year_missing_df = year_missing_df.withColumn("Year", year_missing_df["Year"].cast(IntegerType()))
            # 完整年
            growth_rate_p1 = calculate_growth(raw_data.join(year_missing_df, on=["Year"], how="left_anti"))
            # 不完整年
            growth_rate_p2 = calculate_growth(raw_data.where(col('Year').isin(year_missing + [y - 1 for y in year_missing] + [y + 1 for y in year_missing])), max_month)
    
            growth_rate = growth_rate_p1.select("S_Molecule_for_gr", "CITYGROUP") \
                .union(growth_rate_p2.select("S_Molecule_for_gr", "CITYGROUP")) \
                .distinct()
            growth_rate = growth_rate.join(
                growth_rate_p1.select(["S_Molecule_for_gr", "CITYGROUP"] + [name for name in growth_rate_p1.columns if name.startswith("GR")]),
                on=["S_Molecule_for_gr", "CITYGROUP"],
                how="left")
            growth_rate = growth_rate.join(
                growth_rate_p2.select(["S_Molecule_for_gr", "CITYGROUP"] + [name for name in growth_rate_p2.columns if name.startswith("GR")]),
                on=["S_Molecule_for_gr", "CITYGROUP"],
                how="left")
            
        # ==== **** 输出 **** ====
        outResult(growth_rate_month, p_growth_rate)
            
    elif monthly_update == "True":
        published_right = df_published.where(col('year') == current_year).select('ID').distinct()
        published_left = df_published.where(col('year') == current_year-1 ).select('ID').distinct()
       
        for index, month in enumerate(range(first_month, current_month + 1)):
            
            raw_data_month = raw_data.where(col('Month') == month)
            
            if if_add_data == "False":
                growth_rate_month = calculate_growth(raw_data_month)
            else:
                # publish交集，去除当月未到
                month_hospital = published_left.intersect(published_right) \
                    .exceptAll(df_not_arrived.where(col('Date') == current_year*100 + month).select("ID")) \
                    .toPandas()["ID"].tolist()
                growth_rate_month = calculate_growth(raw_data_month.where(col('ID').isin(month_hospital)))
                # 标记是哪个月补数要用的growth_rate
                
            growth_rate_month = growth_rate_month.withColumn("month_for_monthly_add", func.lit(month))
            
            # ==== **** 输出 **** ====
            outResult(growth_rate_month, p_out_growth_rate)
                    
    logger.debug("输出 growth_rate：" + p_out_growth_rate)
    
    logger.debug('数据执行-Finish')

    # %%
    # =========== 建立分区 =============
    createPartition(p_out_price)
    createPartition(p_out_price_city)
    createPartition(p_out_growth_rate)

