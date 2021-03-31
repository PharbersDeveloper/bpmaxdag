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
    max_path = kwargs['max_path']
    project_name = kwargs['project_name']
    model_month_right = kwargs['model_month_right']
    max_month = kwargs['max_month']
    year_missing = kwargs['year_missing']
    current_year = kwargs['current_year']
    first_month = kwargs['first_month']
    current_month = kwargs['current_month']
    if_others = kwargs['if_others']
    monthly_update = kwargs['monthly_update']
    not_arrived_path = kwargs['not_arrived_path']
    published_path = kwargs['published_path']
    out_path = kwargs['out_path']
    out_dir = kwargs['out_dir']
    need_test = kwargs['need_test']
    if_add_data = kwargs['if_add_data']
    ### input args ###
    
    ### output args ###
    c = kwargs['c']
    d = kwargs['d']
    ### output args ###

    import pandas as pd
    import os
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StringType, IntegerType, DoubleType
    from pyspark.sql import functions as func
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col

    '''
    project_name = "贝达"
    model_month_right = "201912"
    first_month = "1"
    current_month = "12"
    monthly_update = "True"
    out_dir = "202012"
    '''

    logger.debug('job3_data_adding')
    
    if if_add_data != "False" and if_add_data != "True":
        logger.error('wrong input: if_add_data, False or True') 
        raise ValueError('wrong input: if_add_data, False or True')
    
    if if_others == "True":
        out_dir = out_dir + "/others_box/"
    
    out_path_dir = out_path + "/" + project_name + '/' + out_dir
    
    # 输入
    product_mapping_out_path = out_path_dir + "/product_mapping_out"
    products_of_interest_path = max_path + "/" + project_name + "/poi.csv"
    cpa_pha_mapping_path = max_path + "/" + project_name + "/cpa_pha_mapping"
    
    if year_missing:
        year_missing = year_missing.replace(" ","").split(",")
    else:
        year_missing = []
    year_missing = [int(i) for i in year_missing]
    model_month_right = int(model_month_right)
    max_month = int(max_month)
    
    # 月更新相关参数
    if monthly_update != "False" and monthly_update != "True":
        logger.debug('wrong input: monthly_update, False or True') 
        raise ValueError('wrong input: monthly_update, False or True')
    
    if monthly_update == "True":
        current_year = int(current_year)
        first_month = int(first_month)
        current_month = int(current_month)
        if not_arrived_path == "Empty":    
            not_arrived_path = max_path + "/Common_files/Not_arrived" + str(current_year*100 + current_month) + ".csv"
        if published_path == "Empty":
            published_right_path = max_path + "/Common_files/Published" + str(current_year) + ".csv"
            published_left_path = max_path + "/Common_files/Published" + str(current_year - 1) + ".csv"
        else:
            published_path  = published_path.replace(" ","").split(",")
            published_left_path = published_path[0]
            published_right_path = published_path[1]
    else:
        current_year = model_month_right//100
    
    # published_left_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/Common_files/Published2019.csv"
    # published_right_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/Common_files/Published2020.csv"
    
    # 输出
    price_path = out_path_dir + "/price"
    price_city_path = out_path_dir + "/price_city"
    growth_rate_path = out_path_dir + "/growth_rate"
    adding_data_path =  out_path_dir + "/adding_data"
    raw_data_adding_path =  out_path_dir + "/raw_data_adding"
    new_hospital_path = out_path_dir + "/new_hospital"
    raw_data_adding_final_path =  out_path_dir + "/raw_data_adding_final"

    # =========== 数据检查 =============
    
    logger.debug('数据检查-start')
    
    # 存储文件的缺失列
    misscols_dict = {}
    
    # product_mapping_out file
    raw_data = spark.read.parquet(product_mapping_out_path)
    colnames_raw_data = raw_data.columns
    misscols_dict.setdefault("product_mapping_out", [])
    
    colnamelist = ['min1', 'PHA', 'City', 'year_month', 'ID',  'Brand', 'Form',
    'Specifications', 'Pack_Number', 'Manufacturer', 'Molecule', 'Source',
    'Sales', 'Units', 'Path', 'Sheet', 'BI_hospital_code', 'Province', 
    'Month', 'Year', 'City_Tier_2010', 'min2', 'S_Molecule', 'std_route', "标准商品名"]
    #'Raw_Hosp_Name','ORG_Measure','Units_Box', 'Corp', 'Route',
    
    for each in colnamelist:
        if each not in colnames_raw_data:
            misscols_dict["product_mapping_out"].append(each)
    
    # 判断输入文件是否有缺失列
    misscols_dict_final = {}
    for eachfile in misscols_dict.keys():
        if len(misscols_dict[eachfile]) != 0:
            misscols_dict_final[eachfile] = misscols_dict[eachfile]
    # 如果有缺失列，则报错，停止运行
    if misscols_dict_final:
        logger.error('miss columns: %s' % (misscols_dict_final))
        raise ValueError('miss columns: %s' % (misscols_dict_final))
    
    logger.debug('数据检查-Pass')

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
    
    def deal_ID_length(df):
        # ID不足7位的补足0到6位
        # 国药诚信医院编码长度是7位数字，cpa医院编码是6位数字，其他还有包含字母的ID
        df = df.withColumn("ID", df["ID"].cast(StringType()))
        # 去掉末尾的.0
        df = df.withColumn("ID", func.regexp_replace("ID", "\\.0", ""))
        df = df.withColumn("ID", func.when(func.length(df.ID) < 7, func.lpad(df.ID, 6, "0")).otherwise(df.ID))
        return df
    
    # 一. 生成 original_range_raw（样本中已到的Year、Month、PHA的搭配）
    # 2017到当前年的全量出版医院
    Published_years = list(range(2017, current_year+1, 1))
    for index, eachyear in enumerate(Published_years):
        allmonth = [str(eachyear*100 + i) for i in list(range(1,13,1))]
        published_path = max_path + "/Common_files/Published"+str(eachyear)+".csv"
        published = spark.read.csv(published_path, header=True)
        published = deal_ID_length(published)
        for i in allmonth:
            published = published.withColumn(i, func.lit(1))
        if index == 0:
            published_full = published
        else:
            published_full = published_full.join(published, on='ID', how='full')
    
    published_all = unpivot(published_full, ['ID'])
    published_all = published_all.where(col('value')==1).withColumnRenamed('feature', 'Date') \
                                .drop('value')
    
    # 模型前之前的未到名单（跑模型年的时候，不去除未到名单） 
    if monthly_update == 'True':        
        # 1.当前年的未到名单
        not_arrived_current = spark.read.csv(not_arrived_path, header=True)
        not_arrived_current = not_arrived_current.select('ID', 'Date').distinct()
        not_arrived_current = deal_ID_length(not_arrived_current)
        # 2.其他模型年之后的未到名单
        model_year = model_month_right//100
        not_arrived_others_years = set((range(model_year+1, current_year+1, 1)))-set([current_year])
        if not_arrived_others_years:
            for index, eachyear in enumerate(not_arrived_others_years):
                not_arrived_others_path = max_path + "/Common_files/Not_arrived"+str(eachyear)+"12.csv"
                logger.debug(not_arrived_others_path)
                not_arrived = spark.read.csv(not_arrived_others_path, header=True)
                not_arrived = not_arrived.select('ID', 'Date').distinct()
                not_arrived = deal_ID_length(not_arrived)
                if index == 0:
                    not_arrived_others = not_arrived
                else:
                    not_arrived_others = not_arrived_others.union(not_arrived)
            not_arrived_all = not_arrived_current.union(not_arrived_others)
        else:
            not_arrived_all = not_arrived_current
            
    # 出版医院 减去 未到名单
    if monthly_update == 'True':
        original_range_raw = published_all.join(not_arrived_all, on=['ID', 'Date'], how='left_anti')
    else:
        original_range_raw = published_all
        
    # 匹配 PHA
    cpa_pha_mapping = spark.read.parquet(cpa_pha_mapping_path)
    cpa_pha_mapping = cpa_pha_mapping.filter(cpa_pha_mapping["推荐版本"] == 1) \
                                    .select("ID", "PHA").distinct()
    cpa_pha_mapping = deal_ID_length(cpa_pha_mapping)
    
    original_range_raw = original_range_raw.join(cpa_pha_mapping, on='ID', how='left')
    original_range_raw = original_range_raw.where(~col('PHA').isNull()) \
                                            .withColumn('Year', func.substring(col('Date'), 0, 4)) \
                                            .withColumn('Month', func.substring(col('Date'), 5, 2).cast(IntegerType())) \
                                            .select('PHA', 'Year', 'Month').distinct()

    # =========== 数据执行 =============
    logger.debug('数据执行-start')
    
    # 数据读取
    raw_data = spark.read.parquet(product_mapping_out_path)
    raw_data.persist()
    products_of_interest = spark.read.csv(products_of_interest_path, header=True)
    products_of_interest = products_of_interest.toPandas()["poi"].values.tolist()
    
    # raw_data 处理
    raw_data = raw_data.withColumn("S_Molecule_for_gr",
                                   func.when(raw_data["标准商品名"].isin(products_of_interest), raw_data["标准商品名"]).
                                   otherwise(raw_data.S_Molecule))
    
    price = spark.read.parquet(price_path)
    price = price.withColumnRenamed('Price', 'Price_tier')
    
    growth_rate = spark.read.parquet(growth_rate_path)
    growth_rate.persist()
    
    price_city = spark.read.parquet(price_city_path)
    price_city = price_city.withColumnRenamed('Price', 'Price_city')
    
    
    # raw_data 处理
    if monthly_update == "False":
        raw_data = raw_data.where(raw_data.Year < ((model_month_right // 100) + 1))
        if project_name == "Sanofi" or project_name == "AZ":
            raw_data = raw_data.where(raw_data.Year > 2016)
    elif monthly_update == "True":
        if project_name == "Sanofi" or project_name == "AZ":
            raw_data = raw_data.where(raw_data.Year > 2016)
    
    # 4 补数
    def add_data(raw_data, growth_rate):
        # 4.1 原始数据格式整理， 用于补数: trans_raw_data_for_adding
        growth_rate = growth_rate.select(["CITYGROUP", "S_Molecule_for_gr"] + [name for name in growth_rate.columns if name.startswith("GR1")]) \
            .distinct()
        raw_data_for_add = raw_data.where(raw_data.PHA.isNotNull()) \
            .orderBy(raw_data.Year.desc()) \
            .withColumnRenamed("City_Tier_2010", "CITYGROUP") \
            .join(growth_rate, on=["S_Molecule_for_gr", "CITYGROUP"], how="left")
        raw_data_for_add.persist()
    
        # 4.2 补充各个医院缺失的月份数据:
        # add_data
        # 原始数据的 PHA-Month-Year
        # original_range = raw_data_for_add.select("Year", "Month", "PHA").distinct()
    
        years = raw_data_for_add.select("Year").distinct() \
            .orderBy(raw_data_for_add.Year) \
            .toPandas()["Year"].values.tolist()
        
        original_range = original_range_raw.where(original_range_raw.Year.isin(years))
        # print(years)
    
        growth_rate_index = [index for index, name in enumerate(raw_data_for_add.columns) if name.startswith("GR")]
        # print(growth_rate_index)
    
        # 对每年的缺失数据分别进行补数
        empty = 0
        for eachyear in years:
            # cal_time_range
            # 当前年：月份-PHA
            current_range_pha_month = original_range.where(original_range.Year == eachyear) \
                .select("Month", "PHA").distinct()
            # 当前年：月份
            current_range_month = current_range_pha_month.select("Month").distinct()
            # 其他年：月份-当前年有的月份，PHA-当前年没有的医院
            other_years_range = original_range.where(original_range.Year != eachyear) \
                .join(current_range_month, on="Month", how="inner") \
                .join(current_range_pha_month, on=["Month", "PHA"], how="left_anti")
            # 其他年：与当前年的年份差值，比重计算
            other_years_range = other_years_range \
                .withColumn("time_diff", (other_years_range.Year - eachyear)) \
                .withColumn("weight", func.when((other_years_range.Year > eachyear), (other_years_range.Year - eachyear - 0.5)).
                            otherwise(other_years_range.Year * (-1) + eachyear))
            # 选择比重最小的年份：用于补数的 PHA-Month-Year
            current_range_for_add = other_years_range.repartition(1).orderBy(other_years_range.weight.asc())
            current_range_for_add = current_range_for_add.groupBy("PHA", "Month") \
                .agg(func.first(current_range_for_add.Year).alias("Year"))
    
            # get_seed_data
            # 从 rawdata 根据 current_range_for_add 获取用于补数的数据
            current_raw_data_for_add = raw_data_for_add.where(raw_data_for_add.Year != eachyear) \
                .join(current_range_for_add, on=["Month", "PHA", "Year"], how="inner")
            current_raw_data_for_add = current_raw_data_for_add \
                .withColumn("time_diff", (current_raw_data_for_add.Year - eachyear)) \
                .withColumn("weight", func.when((current_raw_data_for_add.Year > eachyear), (current_raw_data_for_add.Year - eachyear - 0.5)).
                            otherwise(current_raw_data_for_add.Year * (-1) + eachyear))
    
            # cal_seed_with_gr
            # 当前年与(当前年+1)的增长率所在列的index
            base_index = eachyear - min(years) + min(growth_rate_index)
            current_raw_data_for_add = current_raw_data_for_add.withColumn("Sales_bk", current_raw_data_for_add.Sales)
    
            # 为补数计算增长率
            current_raw_data_for_add = current_raw_data_for_add \
                .withColumn("min_index", func.when((current_raw_data_for_add.Year < eachyear), (current_raw_data_for_add.time_diff + base_index)).
                            otherwise(base_index)) \
                .withColumn("max_index", func.when((current_raw_data_for_add.Year < eachyear), (base_index - 1)).
                            otherwise(current_raw_data_for_add.time_diff + base_index - 1)) \
                .withColumn("total_gr", func.lit(1))
    
            for i in growth_rate_index:
                col_name = current_raw_data_for_add.columns[i]
                current_raw_data_for_add = current_raw_data_for_add.withColumn(col_name, func.when((current_raw_data_for_add.min_index > i) | (current_raw_data_for_add.max_index < i), 1).
                                                             otherwise(current_raw_data_for_add[col_name]))
                current_raw_data_for_add = current_raw_data_for_add.withColumn(col_name, func.when(current_raw_data_for_add.Year > eachyear, current_raw_data_for_add[col_name] ** (-1)).
                                                             otherwise(current_raw_data_for_add[col_name]))
                current_raw_data_for_add = current_raw_data_for_add.withColumn("total_gr", current_raw_data_for_add.total_gr * current_raw_data_for_add[col_name])
    
            current_raw_data_for_add = current_raw_data_for_add.withColumn("final_gr", func.when(current_raw_data_for_add.total_gr < 2, current_raw_data_for_add.total_gr).
                                                         otherwise(2))
    
            # 为当前年的缺失数据补数：根据增长率计算 Sales，匹配 price，计算 Units=Sales/price
            current_adding_data = current_raw_data_for_add \
                .withColumn("Sales", current_raw_data_for_add.Sales * current_raw_data_for_add.final_gr) \
                .withColumn("Year", func.lit(eachyear))
            current_adding_data = current_adding_data.withColumn("year_month", current_adding_data.Year * 100 + current_adding_data.Month)
            current_adding_data = current_adding_data.withColumn("year_month", current_adding_data["year_month"].cast(DoubleType()))
    
            current_adding_data = current_adding_data.withColumnRenamed("CITYGROUP", "City_Tier_2010") \
                .join(price, on=["min2", "year_month", "City_Tier_2010"], how="inner") \
                .join(price_city, on=["min2", "year_month", "City", "Province"], how="left")
    
            current_adding_data = current_adding_data.withColumn('Price', func.when(current_adding_data.Price_city.isNull(), 
                                                                                    current_adding_data.Price_tier) \
                                                                             .otherwise(current_adding_data.Price_city))
    
            current_adding_data = current_adding_data.withColumn("Units", func.when(current_adding_data.Sales == 0, 0).
                                                         otherwise(current_adding_data.Sales / current_adding_data.Price)) \
                .na.fill({'Units': 0})
    
            if empty == 0:
                adding_data = current_adding_data
            else:
                adding_data = adding_data.union(current_adding_data)
            empty = empty + 1
    
        return adding_data, original_range
    
    # 执行函数 add_data
    if monthly_update == "False":
        logger.debug('4 补数')
        # 补数：add_data
        add_data_out = add_data(raw_data, growth_rate)
        adding_data = add_data_out[0]
        original_range = add_data_out[1]
    
    elif monthly_update == "True" and if_add_data == "True":
        published_left = spark.read.csv(published_left_path, header=True)
        published_right = spark.read.csv(published_right_path, header=True)
        not_arrived =  spark.read.csv(not_arrived_path, header=True)
    
        logger.debug('4 补数')
    
        for index, month in enumerate(range(first_month, current_month + 1)):
            # publish交集，去除当月未到
            # month_hospital = published_left.intersect(published_right) \
            #     .exceptAll(not_arrived.where(not_arrived.Date == current_year*100 + month).select("ID")) \
            #     .toPandas()["ID"].tolist()
    
            raw_data_month = raw_data.where(raw_data.Month == month)
    
            growth_rate_month = growth_rate.where(growth_rate.month_for_monthly_add == month)
    
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
    
    
    if monthly_update == "False":
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
        # 1.9 进一步为最后一年独有的医院补最后一年的缺失月（可能也要考虑第一年）:add_data_new_hosp
        years = original_range.select("Year").distinct() \
            .orderBy(original_range.Year) \
            .toPandas()["Year"].values.tolist()
    
        # 只在最新一年出现的医院
        new_hospital = (original_range.where(original_range.Year == max(years)).select("PHA").distinct()) \
            .subtract(original_range.where(original_range.Year != max(years)).select("PHA").distinct())
        logger.debug("以下是最新一年出现的医院:" + str(new_hospital.toPandas()["PHA"].tolist()))
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
                .difference(set(['Month', 'Sales', 'Units', '季度', "sales_value__rmb_", "total_units", "counting_units", "year_month"]))
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
    elif monthly_update == "True":
        raw_data_adding_final = raw_data_adding \
        .where((raw_data_adding.Year == current_year) & (raw_data_adding.Month >= first_month) & (raw_data_adding.Month <= current_month) )
    
    # 输出补数结果 raw_data_adding_final
    raw_data_adding_final = raw_data_adding_final.repartition(2)
    raw_data_adding_final.write.format("parquet") \
        .mode("overwrite").save(raw_data_adding_final_path)
    
    logger.debug("输出 raw_data_adding_final：" + raw_data_adding_final_path)
    
    logger.debug('数据执行-Finish')



