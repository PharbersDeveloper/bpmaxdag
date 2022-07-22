# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL


def execute(**kwargs):
    logger = phs3logger(kwargs["run_id"], LOG_DEBUG_LEVEL)
    spark = kwargs['spark']
    
    ### input args ###
    project_name = kwargs['project_name']
    model_month_right = kwargs['model_month_right']
    current_year = kwargs['current_year']
    first_month = kwargs['first_month']
    current_month = kwargs['current_month']
    monthly_update = kwargs['monthly_update']
    if_add_data = kwargs['if_add_data']
    run_id = kwargs['run_id'].replace(":","_")
    owner = kwargs['owner']
    g_input_version = kwargs['g_input_version']
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
    # g_out_adding_data = 'adding_data'
    # g_out_new_hospital = 'new_hospital'
    # g_out_raw_data_adding_final = 'raw_data_adding_final'
    
    logger.debug('job3_data_adding')
    if if_add_data != "False" and if_add_data != "True":
        logger.debug('wrong input: if_add_data, False or True') 
        raise ValueError('wrong input: if_add_data, False or True')
    
    if monthly_update != "False" and monthly_update != "True":
        logger.debug('wrong input: monthly_update, False or True') 
        raise ValueError('wrong input: monthly_update, False or True')
      
    model_month_right = int(model_month_right)
    
    # 月更新相关参数
    if monthly_update == "True":
        current_year = int(current_year)
        first_month = int(first_month)
        current_month = int(current_month)
    else:
        current_year = model_month_right//100

    # %% 
    # =========== 输入数据读取 =========== 
    def dealToNull(df):
        df = df.replace(["None", ""], None)
        return df
    
    def dealScheme(df, dict_scheme):
        # 数据类型处理
        if dict_scheme != {}:
            for i in dict_scheme.keys():
                df = df.withColumn(i, col(i).cast(dict_scheme[i]))
        return df
    
    def getInputVersion(df, table_name):
        # 如果 table在g_input_version中指定了version，则读取df后筛选version，否则使用传入的df
        version = g_input_version.get(table_name, '')
        if version != '':
            version_list =  version.replace(' ','').split(',')
            df = df.where(col('version').isin(version_list))
        return df
    
    def readInFile(table_name, dict_scheme={}):
        df = kwargs[table_name]
        df = dealToNull(df)
        df = dealScheme(df, dict_scheme)
        df = getInputVersion(df, table_name.replace('df_', ''))
        df = df.drop('traceId')
        return df
    
    df_raw_data = readInFile('df_raw_data_deal_poi', dict_scheme={'date':'int', 'year':'int', 'month':'int'}) 
    
    df_price = readInFile('df_price', dict_scheme={'date':'int'})
    
    df_price_city = readInFile('df_price_city', dict_scheme={'date':'int'})
    
    df_growth_rate = readInFile('df_growth_rate')
    
    df_original_range_raw = readInFile('df_original_range_raw')
        
    if monthly_update == "False":
        # 只在最新一年出现的医院
        df_new_hospital =  kwargs['df_new_hospital']
        df_new_hospital = dealToNull(df_new_hospital)
     
        
    # 删除已有的s3中间文件
    def deletePath(path_dir):
        file_name = path_dir.replace('//', '/').split('s3:/ph-platform/')[1]
        s3 = boto3.resource('s3', region_name='cn-northwest-1',
                            aws_access_key_id="AKIAWPBDTVEAEU44ZAGT",
                            aws_secret_access_key="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")
        bucket = s3.Bucket('ph-platform')
        bucket.objects.filter(Prefix=file_name).delete()
    # deletePath(path_dir=f"{p_out_adding_data}/version={run_id}/provider={project_name}/owner={owner}/")

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
    df_price = df_price.select('min2', 'date', 'city_tier_2010', 'price')
    df_price_city = df_price_city.select('min2', 'date', 'city', 'province', 'price')
    df_growth_rate = df_growth_rate.drop('version', 'provider', 'owner')


    # %%
    # =========== 补数函数 =========== 
    def addData(df_raw_data, df_growth_rate, df_original_range_raw):
        # 1. 原始数据格式整理， 用于补数， join不同补数月份所用的 growth_rate，形成一个大表
        df_raw_data_for_add = df_raw_data.where(~col('PHA').isNull()) \
                                .withColumnRenamed("City_Tier_2010", "CITYGROUP") \
                                .join(df_growth_rate, on=["S_Molecule_for_gr", "CITYGROUP", "month_for_monthly_add"], how="left")

        # 每年需要补数的月份
        df_years = df_raw_data_for_add.select("Year", "month_for_monthly_add").distinct() \
                                .orderBy("month_for_monthly_add", "Year")

        # 2. 每年发表医院信息
        df_original_range = df_years.join(df_original_range_raw, on='year', how='left')

        growth_rate_index = [index for index, name in enumerate(df_raw_data_for_add.columns) if name.startswith("gr")]

         # 3、对每年的缺失数据进行补数
        # 得到 current_Year 需要补数的 PHA 以及用 可以用哪些 Year 进行补数 
        df_year_month_pha_map = getAddMap(df_original_range, df_years)

        # 选择比重最小的年份，用于补数的 PHA-Month-Year
        df_other_years_range = df_year_month_pha_map \
                        .withColumn("time_diff", (col('Year') - col('current_Year'))) \
                        .withColumn("weight", func.when((col('Year') > col('current_Year')), (col('Year') - col('current_Year') - 0.5)).
                                    otherwise(col('Year') * (-1) + col('current_Year')))
        df_current_range_for_add = df_other_years_range.select("month_for_monthly_add", "PHA", "Month", "Year", "weight", "current_Year") \
                                                                    .groupBy("month_for_monthly_add", "PHA", "Month", "current_Year").apply(pudf_minWeightYear)

        # 生成用于补数的数据集：Year 是用于补数的数据，current_Year是给这些年补数
        df_current_raw_data_for_add = df_raw_data_for_add.join(df_current_range_for_add, on=["month_for_monthly_add", "Month", "PHA", "Year"], how='left') \
                                                .where(~col('current_Year').isNull())

        # 计算最终增长率
        df_min_year = df_years.withColumn('Year', col('Year').cast('int')) \
                        .groupby('month_for_monthly_add').agg(func.min('Year').alias('min_year_for_add'))
        df_current_raw_data_for_add = df_current_raw_data_for_add.join(df_min_year, on='month_for_monthly_add', how='left')
        df_current_raw_data_for_add_cal = getFinalGR(df_current_raw_data_for_add, growth_rate_index)

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
        return {"df_original_range":df_original_range, "df_adding_data":df_current_adding_data}

    def getAddMap(df_original_range, df_years):
        # ===== 得到 每一年（current_Year） 需要补数的 PHA 以及 该年（current_Year）用哪些 Year 进行补数 ============
        '''
        输出：current_Year，Month，PHA，Year
        current_Year，Month：需要补数的年份和月份
        PHA：需要补数的医院，current_Year 没有数据，但是其他年份有的医院
        Year： 用哪些年的数据对 current_Year 进行补数
        '''
        # 1、当前年 与 其他年的 map
        df_years_monthly_add = df_years.groupby('month_for_monthly_add').agg(func.collect_set(col('year')).alias('all_Years'))
        df_year_map = df_original_range.select('month_for_monthly_add', 'Year').distinct() \
                        .withColumn('current_Year', func.array([col('Year')])) \
                        .join(df_years_monthly_add, on='month_for_monthly_add', how='left') \
                        .withColumn('other_Years', func.array_except('all_Years', 'current_Year')) \
                        .withColumn("other_Year", func.explode(col("other_Years")))

        # 2、月份 map
        df_month_map = df_original_range.groupby('month_for_monthly_add', 'Year').agg(func.collect_set(func.col('Month')).alias('month_array'))
        df_month_map1 = df_month_map.withColumnRenamed('month_array', 'current_Year_month_array')
        df_month_map2 = df_month_map.withColumnRenamed('month_array', 'other_Year_month_array').withColumnRenamed('Year', 'other_Year')

        # 3、当前年月 pha 的 map
        df_pha_map = df_original_range.groupby('month_for_monthly_add', 'Year', 'Month').agg(func.collect_set(func.col('PHA')).alias('PHA_array'))
        df_pha_map1 = df_pha_map.withColumnRenamed('PHA_array', 'current_PHA_array')
        df_pha_map2 = df_pha_map.withColumnRenamed('PHA_array', 'other_PHA_array').withColumnRenamed('Year', 'other_Year')

        # 4、联合
        # current_Year 有的月份
        df_year_month_map = df_year_map.join(df_month_map1, on=['month_for_monthly_add' ,'Year'], how='left')
        # other_Year 有的月份   
        df_year_month_map = df_year_month_map.join(df_month_map2, on=['month_for_monthly_add', 'other_Year'], how='left')
        # current_Year 和 other_Year 相同的月份
        df_year_month_map = df_year_month_map.withColumn('month_array', func.array_intersect('current_Year_month_array', 'other_Year_month_array'))
        df_year_month_map = df_year_month_map.withColumn("Month", func.explode(col("month_array")))
        # current_Year_month 有的pha
        df_year_month_pha_map = df_year_month_map.join(df_pha_map1, on=['month_for_monthly_add', 'Year', 'Month'], how='left') 
        # other_Year_month 有的pha
        df_year_month_pha_map = df_year_month_pha_map.join(df_pha_map2, on=['month_for_monthly_add', 'other_Year', 'Month'], how='left')
        # 需要补数的 PHA：other_Year 有 但是 current_Year 没有的 PHA
        df_year_month_pha_map = df_year_month_pha_map.withColumn('PHA_array', func.array_except('other_PHA_array', 'current_PHA_array'))
        df_year_month_pha_map = df_year_month_pha_map.withColumn("PHA", func.explode(col("PHA_array")))

        # 5、结果整理
        df_year_month_pha_map = df_year_month_pha_map.select('month_for_monthly_add', 'Year', 'other_Year', 'Month', 'PHA').distinct() \
                                                    .withColumnRenamed('Year', 'current_Year') \
                                                    .withColumnRenamed('other_Year', 'Year')
        return df_year_month_pha_map

    schema = StructType([
                StructField("month_for_monthly_add", IntegerType(), True),
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
        month_for_monthly_add = int(pdf["month_for_monthly_add"][0])
        return pd.DataFrame([[month_for_monthly_add] + [PHA] + [MONTH] + [minYEAR] + [current_Year]], columns=["month_for_monthly_add", "PHA", "Month", "Year", "current_Year"])

    def getFinalGR(df_current_raw_data_for_add, growth_rate_index):
        # 计算 index
        df_current_raw_data_for_add_cal = df_current_raw_data_for_add.withColumn("time_diff", (col('Year') - col('current_Year'))) \
                                                .withColumn("weight", func.when((col('Year') > col('current_Year')), (col('Year') - col('current_Year') - 0.5)).
                                                            otherwise(col('Year') * (-1) + col('current_Year'))) \
                                                .withColumn("base_index", (col('current_Year') - col('min_year_for_add') + min(growth_rate_index))) \
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

    def getModelFinalAddDate(df_original_range, df_raw_data_adding, df_new_hospital):
        # 进一步为最后一年独有的医院补最后一年的缺失月（可能也要考虑第一年）
        years = df_original_range.select("Year").distinct() \
            .orderBy(col('Year')) \
            .toPandas()["Year"].values.tolist()
        # 最新一年没有的月份
        missing_months = (df_original_range.where(col('Year') != max(years)).select("Month").distinct()) \
            .subtract(df_original_range.where(col('Year') == max(years)).select("Month").distinct())
        # 如果最新一年有缺失月份，需要处理
        if missing_months.count() == 0:
            print("missing_months=0")
            df_raw_data_adding_final = df_raw_data_adding
        else:
            number_of_existing_months = 12 - missing_months.count()
            # 用于groupBy的列名：raw_data_adding列名去除list中的列名
            group_columns = set(df_raw_data_adding.columns) \
                .difference(set(['Month', 'Sales', 'Units', '季度', "sales_value__rmb_", "total_units", "counting_units", "date"]))
            # 补数重新计算
            df_adding_data_new = df_raw_data_adding \
                .where(col('add_flag') == 1) \
                .where(col('PHA').isin(df_new_hospital["PHA"].tolist())) \
                .groupBy(list(group_columns)).agg({"Sales": "sum", "Units": "sum"})
            df_adding_data_new = df_adding_data_new \
                .withColumn("Sales", col("sum(Sales)") / number_of_existing_months) \
                .withColumn("Units", col("sum(Units)") / number_of_existing_months) \
                .crossJoin(missing_months)
            # 生成最终补数结果
            same_names = list(set(df_raw_data_adding.columns).intersection(set(df_adding_data_new.columns)))
            df_raw_data_adding_final = df_raw_data_adding.select(same_names) \
                                            .union(df_adding_data_new.select(same_names)) 
        return df_raw_data_adding_final

    # %%
    # =========== 数据执行 =========== 
    logger.debug('数据执行-start')

    price = df_price.withColumnRenamed('Price', 'Price_tier')
    price_city = df_price_city.withColumnRenamed('Price', 'Price_city')
    
    # raw_data 处理
    if monthly_update == "False":
        df_raw_data = df_raw_data.where(col('Year') < ((model_month_right // 100) + 1))

        
    if if_add_data == "True":
        #logger.debug('4 补数')
        # 2、执行补数
        if monthly_update == "True":
            df_raw_data = df_raw_data.where( (col('Month') >=first_month) & (col('Month') <= current_month) ) \
                                .withColumn('month_for_monthly_add', col('Month'))
        elif monthly_update == "False":
            df_raw_data = df_raw_data.withColumn('month_for_monthly_add', func.lit('0'))
            df_growth_rate = df_growth_rate.withColumn('month_for_monthly_add', func.lit('0'))

        df_adding_data = addData(df_raw_data, df_growth_rate, df_original_range_raw)['df_adding_data']
        df_original_range = addData(df_raw_data, df_growth_rate, df_original_range_raw)['df_original_range']

        # 3、合并补数部分和原始部分:
        df_raw_data_adding = df_raw_data.withColumn("add_flag", func.lit(0)) \
                                    .union(df_adding_data.withColumn("add_flag", func.lit(1)) \
                                    .select(df_raw_data.columns + ["add_flag"]))
    else:
        df_raw_data_adding = df_raw_data.withColumn("add_flag", func.lit(0))

    # 4、其他处理
    if monthly_update == "False":
        df_raw_data_adding_final = (df_raw_data_adding if if_add_data == "False" else getModelFinalAddDate(df_original_range, df_raw_data_adding, df_new_hospital) )
    elif monthly_update == "True":
        df_raw_data_adding_final = df_raw_data_adding.where((col('Year') == current_year) & (col('Month') >= first_month) & (col('Month') <= current_month) )
    df_raw_data_adding_final = df_raw_data_adding_final.drop('month_for_monthly_add')
        

    # %%
    # =========== 数据输出 =============  
    def lowerColumns(df):
        df = df.toDF(*[i.lower() for i in df.columns])
        return df
    
    df_raw_data_adding_final = lowerColumns(df_raw_data_adding_final)
    
    logger.debug('数据执行-Finish')
    
    return {'out_df':df_raw_data_adding_final}

