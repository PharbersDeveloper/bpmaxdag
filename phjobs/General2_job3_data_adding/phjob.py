# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
import pandas as pd
from phlogs.phlogs import phlogger
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql import functions as func


def execute(max_path, project_name, model_month_right, max_month, year_missing, current_year, first_month, current_month, 
if_others, monthly_update, not_arrived_path, published_path, out_path, out_dir, need_test):
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("data from s3") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.instance", "1") \
        .config("spark.executor.memory", "3g") \
        .config('spark.sql.codegen.wholeStage', False) \
        .getOrCreate()

    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    if access_key is not None:
        spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
        spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
        spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
        spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
        # spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
        spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.cn-northwest-1.amazonaws.com.cn")

    phlogger.info('job3_data_adding')
    
    if if_others == "True":
        out_dir = out_dir + "/others_box/"
    
    out_path_dir = out_path + "/" + project_name + '/' + out_dir

    # 输入
    product_mapping_out_path = out_path_dir + "/product_mapping_out"
    if project_name == "Sanofi" or project_name == "AZ":
        products_of_interest_path = max_path + "/AZ_Sanofi/poi.csv"
    else:
        products_of_interest_path = max_path + "/" + project_name + "/poi.csv"
    if year_missing:
        year_missing = year_missing.replace(" ","").split(",")
    else:
        year_missing = []
    year_missing = [int(i) for i in year_missing]
    model_month_right = int(model_month_right)
    max_month = int(max_month)
    
    # 月更新相关参数
    if monthly_update != "False" and monthly_update != "True":
        phlogger.error('wrong input: monthly_update, False or True') 
        raise ValueError('wrong input: monthly_update, False or True')
    if monthly_update == "True":
        current_year = int(current_year)
        first_month = int(first_month)
        current_month = int(current_month)
        if not_arrived_path == "Empty":    
            not_arrived_path = max_path + "/Common_files/Not_arrived" + str(current_year*100 + current_month) + ".csv"
        published_path  = published_path.replace(", ",",").split(",")
        published_left_path = published_path[0]
        published_right_path = published_path[1]
    # not_arrived_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/Common_files/Not_arrived202004.csv"
    # published_left_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/Common_files/Published2019.csv"
    # published_right_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/Common_files/Published2020.csv"

    # 输出
    price_path = out_path_dir + "/price"
    growth_rate_path = out_path_dir + "/growth_rate"
    adding_data_path =  out_path_dir + "/adding_data"
    raw_data_adding_path =  out_path_dir + "/raw_data_adding"
    new_hospital_path = out_path_dir + "/new_hospital"
    raw_data_adding_final_path =  out_path_dir + "/raw_data_adding_final"


    # =========== 数据检查 =============

    phlogger.info('数据检查-start')

    # 存储文件的缺失列
    misscols_dict = {}

    # product_mapping_out file
    raw_data = spark.read.parquet(product_mapping_out_path)
    colnames_raw_data = raw_data.columns
    misscols_dict.setdefault("product_mapping_out", [])

    colnamelist = ['min1', 'PHA', 'City', 'year_month', 'ID',  'Brand', 'Form',
    'Specifications', 'Pack_Number', 'Manufacturer', 'Molecule', 'Source', 'Corp', 'Route',
    'Sales', 'Units', 'Path', 'Sheet', 'BI_hospital_code', 'Province',
    'Month', 'Year', 'City_Tier_2010', 'min2', 'S_Molecule', 'std_route', "标准商品名"]
    #'Raw_Hosp_Name','ORG_Measure','Units_Box'

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
        phlogger.error('miss columns: %s' % (misscols_dict_final))
        raise ValueError('miss columns: %s' % (misscols_dict_final))

    phlogger.info('数据检查-Pass')

    # =========== 数据执行 =============
    phlogger.info('数据执行-start')

    # 数据读取
    raw_data = spark.read.parquet(product_mapping_out_path)
    raw_data.persist()
    products_of_interest = spark.read.csv(products_of_interest_path, header=True)
    products_of_interest = products_of_interest.toPandas()["poi"].values.tolist()

    # raw_data 处理
    raw_data = raw_data.withColumn("S_Molecule_for_gr",
                                   func.when(raw_data["标准商品名"].isin(products_of_interest), raw_data["标准商品名"]).
                                   otherwise(raw_data.S_Molecule))

    phlogger.info('1 价格计算')

    # 1 价格计算：cal_price 补数部分的数量需要用价格得出
    price = raw_data.groupBy("min2", "year_month", "City_Tier_2010") \
        .agg((func.sum("Sales") / func.sum("Units")).alias("Price"))
    price2 = raw_data.groupBy("min2", "year_month") \
        .agg((func.sum("Sales") / func.sum("Units")).alias("Price2"))
    price = price.join(price2, on=["min2", "year_month"], how="left")
    price = price.withColumn("Price", func.when(func.isnull(price.Price), price.Price2).
                             otherwise(price.Price))
    price = price.withColumn("Price", func.when(func.isnull(price.Price), func.lit(0)).
                             otherwise(price.Price)) \
        .drop("Price2")

    # 输出price
    price = price.repartition(2)
    price.write.format("parquet") \
        .mode("overwrite").save(price_path)
    # 读取price
    price = spark.read.parquet(price_path)

    phlogger.info("输出 price：".decode("utf-8") + price_path)

    # raw_data 处理
    if monthly_update == "False":
        raw_data = raw_data.where(raw_data.Year < ((model_month_right // 100) + 1))
        if project_name == "Sanofi" or project_name == "AZ":
            raw_data = raw_data.where((raw_data.Year > 2016) & (raw_data.Year < 2020))
    elif monthly_update == "True":
        if project_name == "Sanofi" or project_name == "AZ":
            raw_data = raw_data.where(raw_data.Year > 2016)

    phlogger.info('2 连续性计算')

    # 2 计算样本医院连续性: cal_continuity
    # 每个医院每年的月份数
    continuity = raw_data.select("Year", "Month", "PHA").distinct() \
        .groupBy("PHA", "Year").count()
    # 每个医院最大月份数，最小月份数
    continuity_whole_year = continuity.groupBy("PHA") \
        .agg(func.max("count").alias("MAX"), func.min("count").alias("MIN"))
    continuity = continuity.repartition(2, "PHA")

    years = continuity.select("Year").distinct().toPandas()["Year"].sort_values().values.tolist()
    # 数据长变宽
    continuity = continuity.groupBy("PHA").pivot("Year").agg(func.sum('count')).fillna(0)
    # 列名修改
    for eachyear in years:
        eachyear = str(eachyear)
        continuity = continuity.withColumn(eachyear, continuity[eachyear].cast(DoubleType())) \
            .withColumnRenamed(eachyear, "Year_" + eachyear)
    # year列求和
    # month_sum = con.Year_2018 + con.Year_2019
    month_sum = ""
    for i in continuity.columns[1:]:
        month_sum += ("continuity." + i + "+")
    month_sum = month_sum.strip('+')
    continuity = continuity.withColumn("total", eval(month_sum))
    # ['PHA', 'Year_2018', 'Year_2019', 'total', 'MAX', 'MIN']
    continuity = continuity.join(continuity_whole_year, on="PHA", how="left")

    # 3 计算样本分子增长率: cal_growth
    def calculate_growth(raw_data, max_month=12):
        # TODO: 完整年用完整年增长，不完整年用不完整年增长
        if max_month < 12:
            raw_data = raw_data.where(raw_data.Month <= max_month)

        # raw_data 处理
        growth_raw_data = raw_data.na.fill({"City_Tier_2010": 5.0})
        growth_raw_data = growth_raw_data.withColumn("CITYGROUP", growth_raw_data.City_Tier_2010)

        # 增长率计算过程
        growth_calculating = growth_raw_data.groupBy("S_Molecule_for_gr", "CITYGROUP", "Year") \
            .agg(func.sum(growth_raw_data.Sales).alias("value"))

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
        original_range = raw_data_for_add.select("Year", "Month", "PHA").distinct()
        original_range.persist()
    
        years = original_range.select("Year").distinct() \
            .orderBy(original_range.Year) \
            .toPandas()["Year"].values.tolist()
        phlogger.info(years)
    
        growth_rate_index = [index for index, name in enumerate(raw_data_for_add.columns) if name.startswith("GR")]
        phlogger.info(growth_rate_index)
    
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
                .join(price, on=["min2", "year_month", "City_Tier_2010"], how="inner")
            current_adding_data = current_adding_data.withColumn("Units", func.when(current_adding_data.Sales == 0, 0).
                                                         otherwise(current_adding_data.Sales / current_adding_data.Price)) \
                .na.fill({'Units': 0})
    
            if empty == 0:
                adding_data = current_adding_data
            else:
                adding_data = adding_data.union(current_adding_data)
            empty = empty + 1
            
        return adding_data, original_range
            
    # 执行函数 calculate_growth, add_data
    if monthly_update == "False":
        phlogger.info('3 增长率计算')
        # AZ-Sanofi 要特殊处理
        if if_others == "True":
            growth_rate = spark.read.parquet("/".join(out_path_dir.split("/")[0:-1]) + "/growth_rate")
        else:
            if project_name != "Sanofi" and project_name != "AZ":
                growth_rate = calculate_growth(raw_data)
            else:
                year_missing_df = pd.DataFrame(year_missing, columns=["Year"])
                year_missing_df = spark.createDataFrame(year_missing_df)
                year_missing_df = year_missing_df.withColumn("Year", year_missing_df["Year"].cast(IntegerType()))
                # 完整年
                growth_rate_p1 = calculate_growth(raw_data.join(year_missing_df, on=["Year"], how="left_anti"))
                # 不完整年
                growth_rate_p2 = calculate_growth(raw_data.where(raw_data.Year.isin(year_missing + [y - 1 for y in year_missing] + [y + 1 for y in year_missing])), max_month)
        
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
                
        phlogger.info('4 补数')
        # 补数：add_data
        add_data_out = add_data(raw_data, growth_rate)
        adding_data = add_data_out[0]
        original_range = add_data_out[1]
            
    elif monthly_update == "True":
        published_left = spark.read.csv(published_left_path, header=True)
        published_right = spark.read.csv(published_right_path, header=True)
        not_arrived =  spark.read.csv(not_arrived_path, header=True)
        
        phlogger.info('3 增长率计算 + 4 补数')
        
        for index, month in enumerate(range(first_month, current_month + 1)):
            # publish交集，去除当月未到
            month_hospital = published_left.intersect(published_right) \
                .exceptAll(not_arrived.where(not_arrived.Date == current_year*100 + month).select("ID")) \
                .toPandas()["ID"].tolist()
            
            raw_data_month = raw_data.where(raw_data.Month == month)
            
            growth_rate = calculate_growth(raw_data_month.where(raw_data_month.ID.isin(month_hospital)))
            
            # 补数：add_data
            adding_data_monthly = add_data(raw_data_month, growth_rate)[0]
            
            if index == 0:
                adding_data = adding_data_monthly
            else:
                adding_data = adding_data.union(adding_data_monthly)
        
    # 输出growth_rate结果
    growth_rate = growth_rate.repartition(2)
    growth_rate.write.format("parquet") \
        .mode("overwrite").save(growth_rate_path)
        
    phlogger.info("输出 growth_rate：".decode("utf-8") + growth_rate_path)
        
    # 输出adding_data
    adding_data = adding_data.repartition(2)
    adding_data.write.format("parquet") \
        .mode("overwrite").save(adding_data_path)
    # 读取adding_data
    adding_data = spark.read.parquet(adding_data_path)

    phlogger.info("输出 adding_data：".decode("utf-8") + adding_data_path)

    # 1.8 合并补数部分和原始部分:
    # combind_data
    raw_data_adding = (raw_data.withColumn("add_flag", func.lit(0))) \
        .union(adding_data.withColumn("add_flag", func.lit(1)).select(raw_data.columns + ["add_flag"]))
    raw_data_adding.persist()

    # 输出
    # raw_data_adding = raw_data_adding.repartition(2)
    # raw_data_adding.write.format("parquet") \
    #     .mode("overwrite").save(raw_data_adding_path)
    # phlogger.info("输出 raw_data_adding：".decode("utf-8") + raw_data_adding_path)
    
    if monthly_update == "False":
        # 1.9 进一步为最后一年独有的医院补最后一年的缺失月（可能也要考虑第一年）:add_data_new_hosp
        years = original_range.select("Year").distinct() \
            .orderBy(original_range.Year) \
            .toPandas()["Year"].values.tolist()
    
        # 只在最新一年出现的医院
        new_hospital = (original_range.where(original_range.Year == max(years)).select("PHA").distinct()) \
            .subtract(original_range.where(original_range.Year != max(years)).select("PHA").distinct())
        phlogger.info("以下是最新一年出现的医院:" + str(new_hospital.toPandas()["PHA"].tolist()))
        # 输出
        new_hospital = new_hospital.repartition(2)
        new_hospital.write.format("parquet") \
            .mode("overwrite").save(new_hospital_path)
    
        phlogger.info("输出 new_hospital：".decode("utf-8") + new_hospital_path)
    
        # 最新一年没有的月份
        missing_months = (original_range.where(original_range.Year != max(years)).select("Month").distinct()) \
            .subtract(original_range.where(original_range.Year == max(years)).select("Month").distinct())
    
        # 如果最新一年有缺失月份，需要处理
        if missing_months.count() == 0:
            phlogger.info("missing_months=0")
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

    phlogger.info("输出 raw_data_adding_final：".decode("utf-8") + raw_data_adding_final_path)

    phlogger.info('数据执行-Finish')

    # =========== 数据验证 =============
    # 与原R流程运行的结果比较正确性: Sanofi与Sankyo测试通过
    if int(need_test) > 0:
        phlogger.info('数据验证-start')

        my_out = spark.read.parquet(raw_data_adding_final_path)
        
        if project_name == "Sanofi":
            R_out_path = "/common/projects/max/AZ_Sanofi/adding_data_new"
        elif project_name == "AZ":
            R_out_path = "/user/ywyuan/max/AZ/Rout/adding_data_new"
        elif project_name == "Sankyo":
            R_out_path = "/common/projects/max/Sankyo/adding_data_new"
        elif project_name == "Astellas":
            R_out_path = "/common/projects/max/Astellas/adding_data_new"
        elif project_name == "京新".decode("utf-8"):
            R_out_path = u"/common/projects/max/京新/adding_data_new"
        R_out = spark.read.parquet(R_out_path)

        # 检查内容：列缺失，列的类型，列的值
        for colname, coltype in R_out.dtypes:
            # 列是否缺失
            if colname not in my_out.columns:
                phlogger.warning ("miss columns:", colname)
            else:
                # 数据类型检查
                if my_out.select(colname).dtypes[0][1] != coltype:
                    phlogger.warning("different type columns: " + colname + ", " + my_out.select(colname).dtypes[0][1] + ", " + "right type: " + coltype)

                # 数值列的值检查
                if coltype == "double" or coltype == "int":
                    sum_my_out = my_out.groupBy().sum(colname).toPandas().iloc[0, 0]
                    sum_R = R_out.groupBy().sum(colname).toPandas().iloc[0, 0]
                    # phlogger.info(colname, sum_raw_data, sum_R)
                    if (sum_my_out - sum_R) != 0:
                        phlogger.warning("different value(sum) columns: " + colname + ", " + str(sum_my_out) + ", " + "right value: " + str(sum_R))

        phlogger.info('数据验证-Finish')

    # =========== return =============
    return raw_data_adding_final
