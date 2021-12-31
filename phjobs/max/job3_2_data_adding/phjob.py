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
    current_year = kwargs['current_year']
    first_month = kwargs['first_month']
    current_month = kwargs['current_month']
    monthly_update = kwargs['monthly_update']
    if_add_data = kwargs['if_add_data']
    out_path = kwargs['out_path']
    run_id = kwargs['run_id'].replace(":","_")
    owner = kwargs['owner']
    g_database_temp = kwargs['g_database_temp']
    ### input args ###
    
    ### output args ###
    ### output args ###

    import pandas as pd
    import os
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col        
    import json
    import boto3    
    # %% 
    # =========== 数据执行 =========== 
    # 输入参数设置
    g_out_adding_data = 'adding_data'
    g_out_new_hospital = 'new_hospital'
    g_out_raw_data_adding_final = 'raw_data_adding_final'
    
    logger.debug('job3_data_adding')
    if if_add_data != "False" and if_add_data != "True":
        logger.debug('wrong input: if_add_data, False or True') 
        raise ValueError('wrong input: if_add_data, False or True')
    
    if monthly_update != "False" and monthly_update != "True":
        logger.debug('wrong input: monthly_update, False or True') 
        raise ValueError('wrong input: monthly_update, False or True')
    
    model_month_right = int(model_month_right)
    
    # dict_input_version = json.loads(g_input_version)
    # logger.debug(dict_input_version)
    
    # 月更新相关参数
    if monthly_update == "True":
        current_year = int(current_year)
        first_month = int(first_month)
        current_month = int(current_month)
    else:
        current_year = model_month_right//100
        
    # 输出
    p_out_adding_data = out_path + g_out_adding_data
    p_out_new_hospital = out_path + g_out_new_hospital
    p_out_raw_data_adding_final = out_path + g_out_raw_data_adding_final

    # %% 
    # =========== 输入数据读取 =========== 
    def changeColToInt(df, list_cols):
        for i in list_cols:
            df = df.withColumn(i, col(i).cast('int'))
        return df
        
    def dealToNull(df):
        df = df.replace(["None", ""], None)
        return df
    
    df_raw_data = kwargs['df_raw_data_deal_poi']
    df_raw_data = dealToNull(df_raw_data)
    df_raw_data = changeColToInt(df_raw_data, ['date', 'year', 'month']) 
    
    df_price = kwargs['df_price']
    df_price = dealToNull(df_price)
    df_price = changeColToInt(df_price, ['date']) 
    
    df_price_city = kwargs['df_price_city']
    df_price_city = dealToNull(df_price_city)
    df_price_city = changeColToInt(df_price_city, ['date'])
    
    df_growth_rate = kwargs['df_growth_rate']
    df_growth_rate = dealToNull(df_growth_rate)
    
    df_cpa_pha_mapping = kwargs['df_cpa_pha_mapping']
    df_cpa_pha_mapping = dealToNull(df_cpa_pha_mapping)
    
    df_published =  kwargs['df_published']
    df_published = dealToNull(df_published)
    df_published = changeColToInt(df_published, ['year'])
    
    if monthly_update == "True":       
        df_not_arrived =  kwargs['df_not_arrived']
        df_not_arrived = dealToNull(df_not_arrived)
        df_not_arrived = changeColToInt(df_not_arrived, ['date'])
        
    # 删除已有的s3中间文件
    def deletePath(path_dir):
        file_name = path_dir.replace('//', '/').split('s3:/ph-platform/')[1]
        s3 = boto3.resource('s3', region_name='cn-northwest-1',
                            aws_access_key_id="AKIAWPBDTVEAEU44ZAGT",
                            aws_secret_access_key="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")
        bucket = s3.Bucket('ph-platform')
        bucket.objects.filter(Prefix=file_name).delete()
    deletePath(path_dir=f"{p_out_adding_data}/version={run_id}/provider={project_name}/owner={owner}/")


    # %% 
    # =========== 数据清洗 =============
    def dealScheme(df, dict_scheme):
        # 数据类型处理
        for i in dict_scheme.keys():
            df = df.withColumn(i, col(i).cast(dict_scheme[i]))
        return df
    def deal_ID_length(df):
        # ID不足7位的补足0到6位
        # 国药诚信医院编码长度是7位数字，cpa医院编码是6位数字，其他还有包含字母的ID
        df = df.withColumn("ID", df["ID"].cast(StringType()))
        # 去掉末尾的.0
        df = df.withColumn("ID", func.regexp_replace("ID", "\\.0", ""))
        df = df.withColumn("ID", func.when(func.length(df.ID) < 7, func.lpad(df.ID, 6, "0")).otherwise(df.ID))
        return df
    
    # 1、选择标准列
    # df_poi = df_poi.select('poi').distinct()
    df_raw_data = df_raw_data.drop('version', 'provider', 'owner')
    df_cpa_pha_mapping = df_cpa_pha_mapping.select('ID', 'PHA', '推荐版本').distinct()
    df_price = df_price.select('min2', 'date', 'city_tier_2010', 'price')
    df_price_city = df_price_city.select('min2', 'date', 'city', 'province', 'price')
    growth_rate = df_growth_rate.drop('version', 'provider', 'owner')
    df_published = df_published.select('id', 'source', 'year').distinct()
    if monthly_update == "True":        
        df_not_arrived = df_not_arrived.select('id', 'date').distinct()
    
    # 2、ID列补位
    df_cpa_pha_mapping = deal_ID_length(df_cpa_pha_mapping)
    df_published = deal_ID_length(df_published)
    if monthly_update == "True":        
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
        client = boto3.client('glue', region_name='cn-northwest-1')
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
    
    
    # 一.生成 original_range_raw（样本中已到的Year、Month、PHA的搭配）
    
    # 2017到当前年的全量出版医院 CPA
    d = list(map(lambda x: func.lit(col('Year')*100 + (x + 1)), range(12)))
    published_all = df_published.where(col("Year") <= current_year) \
                            .where(col('Source') == 'CPA') \
                            .withColumn("Dates", func.array(d)) \
                            .withColumn("Date", func.explode(col("Dates")))
    published_all = published_all.select('ID', 'Date')
    
    if monthly_update == 'True':
        # 模型之后的未到名单( > model_year )
        model_year = model_month_right//100    
        not_arrived_all = df_not_arrived.where(((col("Date")/100).cast('int') > model_year) & ((col("Date")/100).cast('int') <= current_year)  )
        not_arrived_all = not_arrived_all.select(["ID", "Date"])
        # 出版医院 减去 未到名单
        original_range_raw = published_all.join(not_arrived_all, on=['ID', 'Date'], how='left_anti')    
    else:
        # 跑模型年的时候，不去除未到名单
        original_range_raw = published_all
    
    # 与 非CPA医院 合并    
    original_range_raw_noncpa = df_raw_data.where(col('Source') != 'CPA').select('ID', 'Date').distinct()    
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
    
    # 1、数据处理
    # products_of_interest = df_poi.toPandas()["poi"].values.tolist()
    # # raw_data 处理
    # raw_data = df_raw_data.withColumn("S_Molecule_for_gr",
    #                                func.when(col("标准商品名").isin(products_of_interest), col("标准商品名")).
    #                                otherwise(col('S_Molecule')))
    
    raw_data = df_raw_data
    price = df_price.withColumnRenamed('Price', 'Price_tier')
    price_city = df_price_city.withColumnRenamed('Price', 'Price_city')
    
    # raw_data 处理
    if monthly_update == "False":
        raw_data = raw_data.where(col('Year') < ((model_month_right // 100) + 1))

    # %%
    # === 补数函数 === 
    schema = StructType([
                StructField("PHA", StringType(), True),
                StructField("Month", IntegerType(), True),
                StructField("Year", IntegerType(), True),
                StructField("current_Year", IntegerType(), True)
                ])
    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def pudf_minWeightYear(pdf):
        # 选择比重最小的年份,用于补数
        minYEAR = int(pdf.sort_values(["weight"],ascending=True).head(1).reset_index(drop=True)['Year'][0])
        PHA = pdf["PHA"][0]
        MONTH = int(pdf["Month"][0])
        current_Year = int(pdf["current_Year"][0])
        return pd.DataFrame([[PHA] + [MONTH] + [minYEAR] + [current_Year]], columns=["PHA", "Month", "Year", "current_Year"])
    
    def getAddMap(original_range, years):
        # ===== 得到 每一年（current_Year） 需要补数的 PHA 以及 该年（current_Year）用哪些 Year 进行补数 ============
        '''
        输出：current_Year，Month，PHA，Year
        current_Year，Month：需要补数的年份和月份
        PHA：需要补数的医院，current_Year 没有数据，但是其他年份有的医院
        Year： 用哪些年的数据对 current_Year 进行补数
        '''
        # 1、当前年 与 其他年的 map
        df_year_map = original_range.select('Year').distinct() \
                        .withColumn('current_Year', func.array([col('Year')])) \
                        .withColumn('all_Years', func.array([func.lit(str(i)) for i in years])) \
                        .withColumn('other_Years', func.array_except('all_Years', 'current_Year')) \
                        .withColumn("other_Year", func.explode(col("other_Years")))
    
        # 2、月份 map
        df_month_map = original_range.groupby('Year').agg(func.collect_set(func.col('Month')).alias('month_array'))
        df_month_map1 = df_month_map.withColumnRenamed('month_array', 'current_Year_month_array')
        df_month_map2 = df_month_map.withColumnRenamed('month_array', 'other_Year_month_array').withColumnRenamed('Year', 'other_Year')
    
        # 3、当前年月 pha 的 map
        df_pha_map = original_range.groupby('Year', 'Month').agg(func.collect_set(func.col('PHA')).alias('PHA_array'))
        df_pha_map1 = df_pha_map.withColumnRenamed('PHA_array', 'current_PHA_array')
        df_pha_map2 = df_pha_map.withColumnRenamed('PHA_array', 'other_PHA_array').withColumnRenamed('Year', 'other_Year')
    
        # 4、联合
        # current_Year 有的月份
        df_year_month_map = df_year_map.join(df_month_map1, on='Year', how='left')
        # other_Year 有的月份   
        df_year_month_map = df_year_month_map.join(df_month_map2, on='other_Year', how='left')
        # current_Year 和 other_Year 相同的月份
        df_year_month_map = df_year_month_map.withColumn('month_array', func.array_intersect('current_Year_month_array', 'other_Year_month_array'))
        df_year_month_map = df_year_month_map.withColumn("Month", func.explode(col("month_array")))
        # current_Year_month 有的pha
        df_year_month_pha_map = df_year_month_map.join(df_pha_map1, on=['Year', 'Month'], how='left') 
        # other_Year_month 有的pha
        df_year_month_pha_map = df_year_month_pha_map.join(df_pha_map2, on=['other_Year', 'Month'], how='left')
        # 需要补数的 PHA：other_Year 有 但是 current_Year 没有的 PHA
        df_year_month_pha_map = df_year_month_pha_map.withColumn('PHA_array', func.array_except('other_PHA_array', 'current_PHA_array'))
        df_year_month_pha_map = df_year_month_pha_map.withColumn("PHA", func.explode(col("PHA_array")))
        
        # 5、结果整理
        df_year_month_pha_map = df_year_month_pha_map.select('Year', 'other_Year', 'Month', 'PHA').distinct() \
                                                    .withColumnRenamed('Year', 'current_Year') \
                                                    .withColumnRenamed('other_Year', 'Year')
        return df_year_month_pha_map
    
    def getFinalGR(df_current_raw_data_for_add, years, growth_rate_index):
        # 计算 index
        df_current_raw_data_for_add_cal = df_current_raw_data_for_add.withColumn("time_diff", (col('Year') - col('current_Year'))) \
                                                .withColumn("weight", func.when((col('Year') > col('current_Year')), (col('Year') - col('current_Year') - 0.5)).
                                                            otherwise(col('Year') * (-1) + col('current_Year'))) \
                                                .withColumn("base_index", (col('current_Year') - min(years) + min(growth_rate_index))) \
                                                .withColumn("min_index", func.when((col('Year') < col('current_Year')), (col('time_diff') + col('base_index'))).
                                                                    otherwise(col('base_index'))) \
                                                .withColumn("max_index", func.when((col('Year') < col('current_Year')), (col('base_index') - 1)).
                                                            otherwise(col('time_diff') + col('base_index') - 1)) \
                                                .withColumn("total_gr", func.lit(1))
        # 计算 补数用的增长率
        for i in growth_rate_index:
            col_name = df_current_raw_data_for_add_cal.columns[i]
            df_current_raw_data_for_add_cal = df_current_raw_data_for_add_cal.withColumn(col_name, func.when((col('min_index') > i) | (col('max_index') < i), 1).
                                                         otherwise(col(col_name)))
            df_current_raw_data_for_add_cal = df_current_raw_data_for_add_cal.withColumn(col_name, func.when(col('Year') > col('current_Year'), col(col_name) ** (-1)).
                                                         otherwise(col(col_name)))
            df_current_raw_data_for_add_cal = df_current_raw_data_for_add_cal.withColumn("total_gr", col('total_gr') * col(col_name))
    
        df_current_raw_data_for_add_cal = df_current_raw_data_for_add_cal.withColumn("final_gr", func.when(col('total_gr') < 2, col('total_gr')).
                                                                                                     otherwise(2))
        return df_current_raw_data_for_add_cal
    
    def add_data(raw_data, growth_rate):
        # 1. 原始数据格式整理， 用于补数
        growth_rate = growth_rate.select(["CITYGROUP", "S_Molecule_for_gr"] + [name for name in growth_rate.columns if name.startswith("gr")]).distinct()
        raw_data_for_add = raw_data.where(~col('PHA').isNull()) \
                                        .orderBy(col('Year').desc()) \
                                        .withColumnRenamed("City_Tier_2010", "CITYGROUP") \
                                        .join(growth_rate, on=["S_Molecule_for_gr", "CITYGROUP"], how="left")
        raw_data_for_add.persist()
            
        years = raw_data_for_add.select("Year").distinct() \
                    .orderBy(col('Year')) \
                    .toPandas()["Year"].values.tolist()
        
        # 2. 所有发表医院信息
        original_range = original_range_raw.where(col('Year').isin(years))
        # print(years)
    
        growth_rate_index = [index for index, name in enumerate(raw_data_for_add.columns) if name.startswith("gr")]
        # print(growth_rate_index)
    
        # 3、对每年的缺失数据进行补数
        # 得到 current_Year 需要补数的 PHA 以及用 可以用哪些 Year 进行补数 
        df_year_month_pha_map = getAddMap(original_range, years)
        
        # 选择比重最小的年份，用于补数的 PHA-Month-Year
        df_other_years_range = df_year_month_pha_map \
                        .withColumn("time_diff", (col('Year') - col('current_Year'))) \
                        .withColumn("weight", func.when((col('Year') > col('current_Year')), (col('Year') - col('current_Year') - 0.5)).
                                    otherwise(col('Year') * (-1) + col('current_Year')))
        df_current_range_for_add = df_other_years_range.select("PHA", "Month", "Year", "weight", "current_Year") \
                                                                    .groupBy("PHA", "Month", "current_Year").apply(pudf_minWeightYear)
        
        # 生成用于补数的数据集：Year 是用于补数的数据，current_Year是给这些年补数
        df_current_raw_data_for_add = raw_data_for_add.join(df_current_range_for_add, on=["Month", "PHA", "Year"], how='left') \
                                                .where(~col('current_Year').isNull())
    
        # 计算最终增长率
        df_current_raw_data_for_add_cal = getFinalGR(df_current_raw_data_for_add, years, growth_rate_index)
    
        # 为当前年的缺失数据补数：根据增长率计算 Sales，匹配 price，计算 Units=Sales/price
        df_current_adding_data = df_current_raw_data_for_add_cal.withColumn("Sales", col('Sales') * col('final_gr')) \
                                                    .withColumn("Year", func.lit(col('current_Year'))) \
                                                    .drop('current_Year')
    
        df_current_adding_data = df_current_adding_data.withColumn("Date", col('Year') * 100 + col('Month'))
        df_current_adding_data = df_current_adding_data.withColumn("Date", col("Date").cast(DoubleType()))
    
        df_current_adding_data = df_current_adding_data.withColumnRenamed("CITYGROUP", "City_Tier_2010") \
                                            .join(price, on=["min2", "Date", "City_Tier_2010"], how="inner") \
                                            .join(price_city, on=["min2", "Date", "City", "Province"], how="left")
    
        df_current_adding_data = df_current_adding_data.withColumn('Price', func.when(col('Price_city').isNull(), 
                                                                                col('Price_tier')) \
                                                                         .otherwise(col('Price_city')))
        df_current_adding_data = df_current_adding_data.withColumn("Units", func.when(col('Sales') == 0, 0).
                                                     otherwise(col('Sales') / col('Price'))) \
                                                        .na.fill({'Units': 0})    
                                                                    
        # ==== **** 输出 **** ====  
        df_current_adding_data = dealScheme(df_current_adding_data, dict_scheme={'min2': 'string', 'Date': 'double', 'city': 'string', 'province': 'string', 'City_Tier_2010': 'string', 'month': 'int', 'pha': 'string', 'Year': 'int', 'S_Molecule_for_gr': 'string', 'min1': 'string', 'id': 'string', 'raw_hosp_name': 'string', 'brand': 'string', 'form': 'string', 'specifications': 'string', 'pack_number': 'string', 'manufacturer': 'string', 'molecule': 'string', 'source': 'string', 'corp': 'string', 'route': 'string', 'org_measure': 'string', 'Sales': 'double', 'Units': 'double', 'units_box': 'double', 'path': 'string', 'sheet': 'string', 's_molecule': 'string', '标准商品名': 'string'})
        outResult(df_current_adding_data, p_out_adding_data)
    
        return original_range

    # %%
    # raw_data_month = raw_data.where(col('Month') == 1)
    # growth_rate_month = growth_rate.where(col('month_for_monthly_add') == 1)
    # add_data(raw_data_month, growth_rate_month)

    # %%
    # 2、执行补数
    if monthly_update == "False" and if_add_data == "True":
        logger.debug('4 补数')
        # 补数：
        original_range = add_data(raw_data, growth_rate)
    
    elif monthly_update == "True" and if_add_data == "True":
        logger.debug('4 补数')
        for index, month in enumerate(range(first_month, current_month + 1)):
            raw_data_month = raw_data.where(col('Month') == month)
            growth_rate_month = growth_rate.where(col('month_for_monthly_add') == month)
            # 补数：
            add_data(raw_data_month, growth_rate_month)
            logger.debug("输出 adding_data, month：" + str(month))
            

    # %%
    # ==== **** 读回 **** ====             
    createPartition(p_out_adding_data)
    adding_data = spark.sql("SELECT * FROM %s.adding_data WHERE version='%s' AND provider='%s' AND  owner='%s'" 
                                 %(g_database_temp, run_id, project_name, owner))

    # %%
    # 3、合并补数部分和原始部分:
    if if_add_data == "True":
        raw_data_adding = raw_data.withColumn("add_flag", func.lit(0)) \
                            .union(adding_data.withColumn("add_flag", func.lit(1)) \
                            .select(raw_data.columns + ["add_flag"]))
    else:
        raw_data_adding = raw_data.withColumn("add_flag", func.lit(0))
        

    # %%
    # 4、其他处理
    if monthly_update == "False":
        if if_add_data == "False":
            raw_data_adding_final = raw_data_adding
        elif if_add_data == "True":
            # 1.9 进一步为最后一年独有的医院补最后一年的缺失月（可能也要考虑第一年）:add_data_new_hosp
            years = original_range.select("Year").distinct() \
                .orderBy(col('Year')) \
                .toPandas()["Year"].values.tolist()
    
            # 只在最新一年出现的医院
            new_hospital = (original_range.where(col('Year') == max(years)).select("PHA").distinct()) \
                        .subtract(original_range.where(col('Year') != max(years)).select("PHA").distinct())
            
            # ==== **** 输出 **** ====        
            outResult(new_hospital, p_out_new_hospital)
            logger.debug("输出 new_hospital：" + p_out_new_hospital)
            createPartition(p_out_new_hospital)
    
    
            # 最新一年没有的月份
            missing_months = (original_range.where(col('Year') != max(years)).select("Month").distinct()) \
                .subtract(original_range.where(col('Year') == max(years)).select("Month").distinct())
    
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
                    .where(col('add_flag') == 1) \
                    .where(col('PHA').isin(new_hospital["PHA"].tolist())) \
                    .groupBy(list(group_columns)).agg({"Sales": "sum", "Units": "sum"})
                adding_data_new = adding_data_new \
                    .withColumn("Sales", col("sum(Sales)") / number_of_existing_months) \
                    .withColumn("Units", col("sum(Units)") / number_of_existing_months) \
                    .crossJoin(missing_months)
                # 生成最终补数结果
                same_names = list(set(raw_data_adding.columns).intersection(set(adding_data_new.columns)))
                raw_data_adding_final = raw_data_adding.select(same_names) \
                                                .union(adding_data_new.select(same_names))        
    elif monthly_update == "True":
        raw_data_adding_final = raw_data_adding.where((col('Year') == current_year) & (col('Month') >= first_month) & (col('Month') <= current_month) )

    # %%
    # =========== 数据输出 =============
    # outResult(raw_data_adding_final, p_out_raw_data_adding_final)
    # print("输出 raw_data_adding_final：" + p_out_raw_data_adding_final)
    # createPartition(p_out_raw_data_adding_final)
    # print('数据执行-Finish')
    
    
    def lowerColumns(df):
        df = df.toDF(*[i.lower() for i in df.columns])
        return df
    
    raw_data_adding_final = lowerColumns(raw_data_adding_final)
    
    logger.debug('数据执行-Finish')
    
    return {'out_df':raw_data_adding_final}
