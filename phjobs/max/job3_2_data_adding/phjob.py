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
    g_out_adding_data_tmp = kwargs['g_out_adding_data_tmp']
    g_out_adding_data = kwargs['g_out_adding_data']
    g_out_new_hospital = kwargs['g_out_new_hospital']
    g_out_raw_data_adding_final = kwargs['g_out_raw_data_adding_final']
    ### output args ###

    import pandas as pd
    import os
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
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
    p_out_adding_data = out_path + g_out_adding_data
    p_out_raw_data_adding = out_path + g_out_raw_data_adding
    p_out_new_hospital = out_path + g_out_new_hospital
    p_out_raw_data_adding_final = out_path + g_out_raw_data_adding_final
    # %% 
    # 输入数据读取
    df_raw_data = spark.sql("SELECT * FROM %s.product_mapping_out WHERE version='%s' AND provider='%s' AND  owner='%s'" 
                         %(g_database_temp, run_id, project_name, owner))
    
    df_price = spark.sql("SELECT * FROM %s.price WHERE version='%s' AND provider='%s' AND  owner='%s'" 
                         %(g_database_temp, run_id, project_name, owner))
    
    df_price_city = spark.sql("SELECT * FROM %s.price_city WHERE version='%s' AND provider='%s' AND  owner='%s'" 
                         %(g_database_temp, run_id, project_name, owner))
    
    df_growth_rate = spark.sql("SELECT * FROM %s.growth_rate WHERE version='%s' AND provider='%s' AND  owner='%s'" 
                         %(g_database_temp, run_id, project_name, owner))
    
    df_poi =  spark.sql("SELECT * FROM %s.poi WHERE provider='%s' AND version='%s'" 
                             %(g_database_input, project_name, dict_input_version['poi']))
    
    df_cpa_pha_mapping =  spark.sql("SELECT * FROM %s.cpa_pha_mapping WHERE provider='%s' AND version='%s'" 
                             %(g_database_input, project_name, dict_input_version['cpa_pha_mapping']))
    
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
    df_cpa_pha_mapping = df_cpa_pha_mapping.select('ID', 'PHA', '推荐版本').distinct()
    df_price = df_price.select('min2', 'date', 'city_tier_2010', 'price')
    df_price_city = df_price_city.select('min2', 'date', 'city', 'province', 'price')
    growth_rate = df_growth_rate.drop('version', 'provider', 'owner')
    
    # 2、ID列补位
    df_published = deal_ID_length(df_published)
    df_not_arrived = deal_ID_length(df_not_arrived)
    df_cpa_pha_mapping = deal_ID_length(df_cpa_pha_mapping)
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
    
    
    # 一. 生成 original_range_raw（样本中已到的Year、Month、PHA的搭配）
    # 2017到当前年的全量出版医院
    d = list(map(lambda x: func.lit(col('year')*100 + (x + 1)), range(12)))
    published_all = df_published.where(col("year") <= current_year) \
                            .where(col('Source') == 'CPA') \
                            .withColumn("DATES", func.array(d)) \
                            .withColumn("Date", func.explode(col("DATES")))
    published_all = published_all.select('ID', 'Date')
    
    
    # 模型前之后的未到名单（跑模型年的时候，不去除未到名单） 
    if monthly_update == 'True':
        model_year = model_month_right//100    
        not_arrived_all = df_not_arrived.where((col("Date")/100 > model_year) & (col("Date")/100 <= current_year)  )
        not_arrived_all = not_arrived_all.select(["ID", "Date"])
        
    original_range_raw_noncpa = df_raw_data.where(col('Source') != 'CPA').select('ID', 'Date').distinct()
    
    # 出版医院 减去 未到名单
    if monthly_update == 'True':
        original_range_raw = published_all.join(not_arrived_all, on=['ID', 'Date'], how='left_anti')
    else:
        original_range_raw = published_all
        
    original_range_raw = original_range_raw.union(original_range_raw_noncpa.select(original_range_raw.columns))
    
    # 匹配 PHA
    df_cpa_pha_mapping = df_cpa_pha_mapping.where(col("推荐版本") == 1) \
                                        .select("ID", "PHA").distinct()
    
    original_range_raw = original_range_raw.join(df_cpa_pha_mapping, on='ID', how='left')
    original_range_raw = original_range_raw.where(~col('PHA').isNull()) \
                                            .withColumn('Year', func.substring(col('Date'), 0, 4)) \
                                            .withColumn('Month', func.substring(col('Date'), 5, 2).cast(IntegerType())) \
                                            .select('PHA', 'Year', 'Month').distinct()
    # %%
    # =========== 数据执行 =============
    logger.debug('数据执行-start')
    
    # 数据读取
    products_of_interest = df_poi.toPandas()["poi"].values.tolist()
    # raw_data 处理
    raw_data = df_raw_data.withColumn("S_Molecule_for_gr",
                                   func.when(col("标准商品名").isin(products_of_interest), col("标准商品名")).
                                   otherwise(col('S_Molecule')))
    
    
    price = df_price.withColumnRenamed('Price', 'Price_tier')
    price_city = df_price_city.withColumnRenamed('Price', 'Price_city')
    
    # raw_data 处理
    if monthly_update == "False":
        raw_data = raw_data.where(raw_data.Year < ((model_month_right // 100) + 1))
    # %%
    # === 补数函数 === 
    schema = StructType([
                StructField("PHA", StringType(), True),
                StructField("Month", IntegerType(), True),
                StructField("Year", IntegerType(), True)
                ])
    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def pudf_minWeightYear(pdf):
        minYEAR = int(pdf.sort_values(["weight"],ascending=True).head(1).reset_index(drop=True)['Year'][0])
        PHA = pdf["PHA"][0]
        MONTH = pdf["Month"][0]
        return pd.DataFrame([[PHA] + [MONTH] + [minYEAR]], columns=["PHA", "Month", "Year"])
    
    def add_data(raw_data, growth_rate):
        # 1. 原始数据格式整理， 用于补数
        growth_rate = growth_rate.select(["CITYGROUP", "S_Molecule_for_gr"] + [name for name in growth_rate.columns if name.startswith("gr")]) \
            .distinct()
        raw_data_for_add = raw_data.where(col('PHA').isNotNull()) \
            .orderBy(col('Year').desc()) \
            .withColumnRenamed("City_Tier_2010", "CITYGROUP") \
            .join(growth_rate, on=["S_Molecule_for_gr", "CITYGROUP"], how="left")
        raw_data_for_add.persist()
        
        # 2. 获取所有发表医院
        # 原始数据的 PHA-Month-YEAR
        # original_range = raw_data_for_add.select("Year", "Month", "PHA").distinct()
    
        years = raw_data_for_add.select("Year").distinct() \
            .orderBy(col('Year')) \
            .toPandas()["Year"].values.tolist()
        
        original_range = original_range_raw.where(col('Year').isin(years))
        # print(years)
    
        growth_rate_index = [index for index, name in enumerate(raw_data_for_add.columns) if name.startswith("gr")]
        # print(growth_rate_index)
    
        # 3、对每年的缺失数据分别进行补数
        empty = 0
        for eachyear in years:
            # cal_time_range
            # 当前年：月份-PHA
            current_range_pha_month = original_range.where(col('Year') == eachyear) \
                .select("Month", "PHA").distinct()
            # 当前年：月份
            current_range_month = current_range_pha_month.select("Month").distinct()
            # 其他年：月份-当前年有的月份，PHA-当前年没有的医院
            other_years_range = original_range.where(col('Year') != eachyear) \
                .join(current_range_month, on="Month", how="inner") \
                .join(current_range_pha_month, on=["Month", "PHA"], how="left_anti")
            # 其他年：与当前年的年份差值，比重计算
            other_years_range = other_years_range \
                .withColumn("time_diff", (col('Year') - eachyear)) \
                .withColumn("weight", func.when((col('Year') > eachyear), (col('Year') - eachyear - 0.5)).
                            otherwise(col('Year') * (-1) + eachyear))
            # 选择比重最小的年份：用于补数的 PHA-Month-Year
            # orderBy 后 取 first 在 zhb的机器上跑回有随机取值发生
            # current_range_for_add = other_years_range.repartition(1).orderBy(other_years_range.weight.asc())
            # current_range_for_add = current_range_for_add.groupBy("PHA", "Month") \
            #     .agg(func.first(current_range_for_add.Year).alias("Year"))
            current_range_for_add = other_years_range.select("PHA", "Month", "Year", "weight") \
                                                            .groupBy("PHA", "Month").apply(pudf_minWeightYear)
    
            # 从 rawdata 根据 current_range_for_add 获取用于补数的数据
            current_raw_data_for_add = raw_data_for_add.where(col('Year') != eachyear) \
                                                .join(current_range_for_add, on=["Month", "PHA", "Year"], how="inner")
            current_raw_data_for_add = current_raw_data_for_add.withColumn("time_diff", (col('Year') - eachyear)) \
                                            .withColumn("weight", func.when((col('Year') > eachyear), (col('Year') - eachyear - 0.5)).
                                                        otherwise(col('Year') * (-1) + eachyear))
    
            # 当前年与(当前年+1)的增长率所在列的index
            base_index = eachyear - min(years) + min(growth_rate_index)
            current_raw_data_for_add = current_raw_data_for_add.withColumn("Sales_bk", col('Sales'))
    
            # 为补数计算增长率
            current_raw_data_for_add = current_raw_data_for_add \
                .withColumn("min_index", func.when((col('Year') < eachyear), (col('time_diff') + base_index)).
                            otherwise(base_index)) \
                .withColumn("max_index", func.when((col('Year') < eachyear), (base_index - 1)).
                            otherwise(col('time_diff') + base_index - 1)) \
                .withColumn("total_gr", func.lit(1))
    
            for i in growth_rate_index:
                col_name = current_raw_data_for_add.columns[i]
                current_raw_data_for_add = current_raw_data_for_add.withColumn(col_name, func.when((col('min_index') > i) | (col('max_index') < i), 1).
                                                             otherwise(col(col_name)))
                current_raw_data_for_add = current_raw_data_for_add.withColumn(col_name, func.when(col('Year') > eachyear, col(col_name) ** (-1)).
                                                             otherwise(col(col_name)))
                current_raw_data_for_add = current_raw_data_for_add.withColumn("total_gr", col('total_gr') * col(col_name))
    
            current_raw_data_for_add = current_raw_data_for_add.withColumn("final_gr", func.when(col('total_gr') < 2, col('total_gr')).
                                                                                             otherwise(2))
    
            # 为当前年的缺失数据补数：根据增长率计算 Sales，匹配 price，计算 Units=Sales/price
            current_adding_data = current_raw_data_for_add.withColumn("Sales", col('Sales') * col('final_gr')) \
                                                        .withColumn("Year", func.lit(eachyear))
            current_adding_data = current_adding_data.withColumn("Date", col('Year') * 100 + col('Month'))
            current_adding_data = current_adding_data.withColumn("Date", col("Date").cast(DoubleType()))
    
            current_adding_data = current_adding_data.withColumnRenamed("CITYGROUP", "City_Tier_2010") \
                .join(price, on=["min2", "Date", "City_Tier_2010"], how="inner") \
                .join(price_city, on=["min2", "Date", "City", "Province"], how="left")
    
            current_adding_data = current_adding_data.withColumn('Price', func.when(col('Price_city').isNull(), 
                                                                                    col('Price_tier')) \
                                                                             .otherwise(col('Price_city')))
            current_adding_data = current_adding_data.withColumn("Units", func.when(col('Sales') == 0, 0).
                                                         otherwise(col('Sales') / col('Price'))) \
                                                    .na.fill({'Units': 0})
                                                                 
            # ==== **** 输出 **** ====                                                     
            outResult(current_adding_data, p_out_adding_data)
    
        return original_range

    # %%
    # raw_data_month = raw_data.where(col('Month') == 1)
    # growth_rate_month = growth_rate.where(col('month_for_monthly_add') == 1)
    # add_data(raw_data_month, growth_rate_month)
    # %%
    # 执行函数 add_data
    if monthly_update == "False" and if_add_data == "True":
        logger.debug('4 补数')
        # 补数：add_data
        original_range = add_data(raw_data, growth_rate)
    
    
    
    # 执行函数 add_data
    if monthly_update == "False" and if_add_data == "True":
        logger.debug('4 补数')
        # 补数：add_data
        add_data_out = add_data(raw_data, growth_rate)
        adding_data = add_data_out[0]
        original_range = add_data_out[1]
    
    elif monthly_update == "True" and if_add_data == "True":
        logger.debug('4 补数')
    
        for index, month in enumerate(range(first_month, current_month + 1)):
            raw_data_month = raw_data.where(col('Month') == month)
    
            growth_rate_month = growth_rate.where(col('month_for_monthly_add') == month)
    
            # 补数：add_data
            adding_data_monthly = add_data(raw_data_month, growth_rate_month)[0]
    
            # 输出adding_data
            if index == 0:
                # adding_data = adding_data_monthly
                adding_data_monthly = adding_data_monthly.repartition(1)
                adding_data_monthly.write.format("parquet") \
                    .mode("overwrite").save(adding_data_path)
            else:
                # adding_data = adding_data.union(adding_data_monthly)
                adding_data_monthly = adding_data_monthly.repartition(1)
                adding_data_monthly.write.format("parquet") \
                    .mode("append").save(adding_data_path)
            logger.debug("输出 adding_data：" + adding_data_path)
    
    
    if monthly_update == "False" and if_add_data == "True":
        adding_data = adding_data.repartition(2)
        adding_data.write.format("parquet") \
            .mode("overwrite").save(adding_data_path)
        logger.debug("输出 adding_data：" + adding_data_path)
    elif monthly_update == "True" and if_add_data == "True":
        adding_data = spark.read.parquet(adding_data_path)
    
    # 1.8 合并补数部分和原始部分:
    # combind_data
    if if_add_data == "True":
        raw_data_adding = (raw_data.withColumn("add_flag", func.lit(0))) \
            .union(adding_data.withColumn("add_flag", func.lit(1)).select(raw_data.columns + ["add_flag"]))
    else:
        raw_data_adding = raw_data.withColumn("add_flag", func.lit(0))
    raw_data_adding.persist()
    
    # 输出
    # raw_data_adding = raw_data_adding.repartition(2)
    # raw_data_adding.write.format("parquet") \
    #     .mode("overwrite").save(raw_data_adding_path)
    # print("输出 raw_data_adding：" + raw_data_adding_path)
    
    if monthly_update == "False":
        if if_add_data == "True":
            # 1.9 进一步为最后一年独有的医院补最后一年的缺失月（可能也要考虑第一年）:add_data_new_hosp
            years = original_range.select("Year").distinct() \
                .orderBy(original_range.Year) \
                .toPandas()["Year"].values.tolist()
    
            # 只在最新一年出现的医院
            new_hospital = (original_range.where(original_range.Year == max(years)).select("PHA").distinct()) \
                .subtract(original_range.where(original_range.Year != max(years)).select("PHA").distinct())
            # print("以下是最新一年出现的医院:" + str(new_hospital.toPandas()["PHA"].tolist()))
            # 输出
            new_hospital = new_hospital.repartition(2)
            new_hospital.write.format("parquet") \
                .mode("overwrite").save(new_hospital_path)
    
            logger.debug("输出 new_hospital：" + new_hospital_path)
    
            # 最新一年没有的月份
            missing_months = (original_range.where(original_range.Year != max(years)).select("Month").distinct()) \
                .subtract(original_range.where(original_range.Year == max(years)).select("Month").distinct())
    
            # 如果最新一年有缺失月份，需要处理
            if missing_months.count() == 0:
                logger.debug("missing_months=0")
                raw_data_adding_final = raw_data_adding
            else:
                number_of_existing_months = 12 - missing_months.count()
                # 用于groupBy的列名：raw_data_adding列名去除list中的列名
                group_columns = set(raw_data_adding.columns) \
                    .difference(set(['Month', 'Sales', 'Units', '季度', "sales_value__rmb_", "total_units", "counting_units", "date"]))
                # 补数重新计算
                adding_data_new = raw_data_adding \
                    .where(raw_data_adding.add_flag == 1) \
                    .where(raw_data_adding.PHA.isin(new_hospital["PHA"].tolist())) \
                    .groupBy(list(group_columns)).agg({"Sales": "sum", "Units": "sum"})
                adding_data_new = adding_data_new \
                    .withColumn("Sales", adding_data_new["sum(Sales)"] / number_of_existing_months) \
                    .withColumn("Units", adding_data_new["sum(Units)"] / number_of_existing_months) \
                    .crossJoin(missing_months)
                # 生成最终补数结果
                same_names = list(set(raw_data_adding.columns).intersection(set(adding_data_new.columns)))
                raw_data_adding_final = raw_data_adding.select(same_names) \
                    .union(adding_data_new.select(same_names))
        else:
            raw_data_adding_final = raw_data_adding
    elif monthly_update == "True":
        raw_data_adding_final = raw_data_adding \
        .where((raw_data_adding.Year == current_year) & (raw_data_adding.Month >= first_month) & (raw_data_adding.Month <= current_month) )
    
    # 输出补数结果 raw_data_adding_final
    raw_data_adding_final = raw_data_adding_final.repartition(2)
    raw_data_adding_final.write.format("parquet") \
        .mode("overwrite").save(raw_data_adding_final_path)
    
    logger.debug("输出 raw_data_adding_final：" + raw_data_adding_final_path)
    
    logger.debug('数据执行-Finish')

