# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
import numpy as np
import pandas as pd

from pyspark.sql import SparkSession
import time
from pyspark.sql.types import *
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql import functions as func


def execute(a, b):
	spark = SparkSession.builder \
		.master("yarn") \
		.appName("sparkOutlier") \
		.config("spark.driver.memory", "1g") \
		.config("spark.executor.cores", "1") \
		.config("spark.executor.instance", "2") \
		.config("spark.executor.memory", "2g") \
		.getOrCreate()
		
	# =========== 数据检查 =============
		
	
	# 输入
	product_mapping_out_path = "/user/ywyuan/max/Sankyo/raw_data_job2_out"
	products_of_interest_path = "/workspace/BP_Max_AutoJob/Sankyo/poi.xlsx"
	model_month_right = 201912
	project_name = "Sankyo"
	max_month = 12
	year_missing = []
	
	# 输出
	price_path = "/user/ywyuan/max/Sankyo/price"
	raw_data_adding_path = "/user/ywyuan/max/Sankyo/raw_data_adding"
	new_hospital_path = '/workspace/BP_Max_AutoJob/Sankyo/2019new_hospital.xlsx'
	
	# =========== 数据执行 =============
    logger.info('数据执行-start')

	raw_data = spark.read.parquet(product_mapping_out_path)
	
	products_of_interest = pd.read_excel(products_of_interest_path)
	products_of_interest = products_of_interest["poi"].values.tolist()
	
	# 新增一列S_Molecule_for_gr：products_of_interest为商品名，其他产品为分子名
	raw_data = raw_data.withColumn("S_Molecule_for_gr",
	                               func.when(raw_data["标准商品名"].isin(products_of_interest), raw_data["标准商品名"]).
	                               otherwise(raw_data.S_Molecule))
	
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
	
	raw_data = raw_data.where(raw_data.Year < ((model_month_right // 100) + 1))
	if project_name == "Sanofi" or project_name == "AZ":
	    raw_data = raw_data.where(raw_data.Year > 2016 & raw_data.Year < 2020)

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
	# 最大最小值
	# ['PHA', 'Year_2018', 'Year_2019', 'total', 'MAX', 'MIN']
	continuity = continuity.join(continuity_whole_year, on="PHA", how="left")
	
	# 3 计算样本分子增长率: cal_growth
	def calculate_growth(raw_data, max_month=12):
	    # TODO: 完整年用完整年增长，不完整年用不完整年增长
	    if max_month < 12:
	        raw_data = raw_data.where(raw_data.Month <= max_month)
	
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
	        growth_rate = growth_calculating.withColumn("GR" + years[i][2:4] + years[i + 1][2:4],
	                                                    growth_calculating[years_name[i + 1]] / growth_calculating[years_name[i]])
	    # 增长率的调整：modify_gr
	    for y in [name for name in growth_rate.columns if name.startswith("GR")]:
	        growth_rate = growth_rate.withColumn(y, func.when(func.isnull(growth_rate[y]) | (growth_rate[y] > 10) | (growth_rate[y] < 0.1), 1).
	                                             otherwise(growth_rate[y]))
	    return growth_rate
	
	
	# AZ-Sanofi 要特殊处理
	if project_name != "Sanofi" and project_name != "AZ":
	    growth_rate = calculate_growth(raw_data)
	else:
	    # 完整年
	    growth_rate_p1 = calculate_growth(raw_data.where(raw_data.Year.isin(year_missing)))
	    # 不完整年
	    growth_rate_p2 = calculate_growth(raw_data.where(raw_data.Year.isin(year_missing + [y - 1 for y in year_missing] + [y + 1 for y in year_missing])), max_month)
	
	    growth_rate = growth_rate_p1.select("S_Molecule_for_gr", "CITYGROUP") \
	        .union(growth_rate_p2.select("S_Molecule_for_gr", "CITYGROUP")) \
	        .distinct()
	    growth_rate = growth_rate.join(
	        growth_rate_p1.select("S_Molecule_for_gr", "CITYGROUP",[name for name in growth_rate_p1.columns if name.startswith("GR")]),
	        on=["S_Molecule_for_gr", "CITYGROUP"],
	        how="left")
	    growth_rate = growth_rate.join(
	        growth_rate_p2.select("S_Molecule_for_gr", "CITYGROUP",[name for name in growth_rate_p2.columns if name.startswith("GR")]),
	        on=["S_Molecule_for_gr", "CITYGROUP"],
	        how="left")
	
	# 输出growth_rate结果
	growth_rate = growth_rate.repartition(2)
	growth_path_online = "/user/ywyuan/max/Sankyo/gr"
	growth_rate.write.format("parquet") \
	    .mode("overwrite").save(growth_path_online)
	    
	# 4 补数: 
	# 4.1 原始数据格式整理: trans_raw_data_for_adding
	growth_rate = growth_rate.select(["CITYGROUP", "S_Molecule_for_gr"] + [name for name in growth_rate.columns if name.startswith("GR1")]) \
	    .distinct()
	raw_data_for_add = raw_data.where(raw_data.PHA.isNotNull()) \
	    .orderBy(raw_data.Year.desc()) \
	    .withColumnRenamed("City_Tier_2010", "CITYGROUP") \
	    .join(growth_rate, on=["S_Molecule_for_gr", "CITYGROUP"], how="left")

	# 1.7 补充各个医院缺失的月份:
	# add_data
	original_range = seed.select("Year", "Month", "PHA").distinct()
	
	years = original_range.select("Year").distinct() \
	    .orderBy(original_range.Year) \
	    .toPandas()["Year"].values.tolist()
	print(years)
	
	all_gr_index = [index for index, name in enumerate(seed.columns) if name.startswith("GR")]
	print(all_gr_index)
	
	# 每年的补数
	# price_path = "/common/projects/max/Sankyo/price"
	#price_path = "/user/ywyuan/max/Sankyo/price"
	#price = spark.read.parquet(price_path)
	
	# 往下测试
	#years = [2018, 2019]
	#all_gr_index = [31]
	#original_range = spark.read.parquet("/user/ywyuan/max/Sankyo/original_range")
	#seed = spark.read.parquet("/user/ywyuan/max/Sankyo/seed")
	
	empty = 0
	for eachyear in years:
	    # cal_time_range
	    # 当前年的 月份-PHA 集合
	    current_range_pha_month = original_range.where(original_range.Year == eachyear) \
	        .select("Month", "PHA").distinct()
	    # 当前年的 月份 集合
	    current_range_month = current_range_pha_month.select("Month").distinct()
	    # 其他年 在当前年有的 月份-PHA
	    other_years_range = original_range.where(original_range.Year != eachyear) \
	        .join(current_range_month, on="Month", how="inner") \
	        .join(current_range_pha_month, on=["Month", "PHA"], how="left_anti")
	    # 其他年 与 当前年的 差值，比重计算
	    other_years_range = other_years_range \
	        .withColumn("time_diff", (other_years_range.Year - eachyear)) \
	        .withColumn("weight", func.when((other_years_range.Year > eachyear), (other_years_range.Year - eachyear - 0.5)).
	                    otherwise(other_years_range.Year * (-1) + eachyear))
	    # 选择比重最小的其他年份
	    seed_range = other_years_range.orderBy(other_years_range.weight) \
	        .groupBy("PHA", "Month") \
	        .agg(func.first(other_years_range.Year).alias("Year"))
	
	    # get_seed_data
	    # 从rawdata根据seed_range获取要补数的数据
	    seed_for_adding = seed.where(seed.Year != eachyear) \
	        .join(seed_range, on=["Month", "PHA", "Year"], how="inner")
	    seed_for_adding = seed_for_adding \
	        .withColumn("time_diff", (seed_for_adding.Year - eachyear)) \
	        .withColumn("weight", func.when((seed_for_adding.Year > eachyear), (seed_for_adding.Year - eachyear - 0.5)).
	                    otherwise(seed_for_adding.Year * (-1) + eachyear))
	
	    # cal_seed_with_gr
	    base_index = eachyear - min(years) + min(all_gr_index)
	    seed_for_adding = seed_for_adding.withColumn("Sales_bk", seed_for_adding.Sales)
	
	    # min_index：seed_for_adding年份小于当前年， time_diff+base_index
	    # max_index：seed_for_adding年份小于当前年，base_index-1
	    seed_for_adding = seed_for_adding \
	        .withColumn("min_index", func.when((seed_for_adding.Year < eachyear), (seed_for_adding.time_diff + base_index)).
	                    otherwise(base_index)) \
	        .withColumn("max_index", func.when((seed_for_adding.Year < eachyear), (base_index - 1)).
	                    otherwise(seed_for_adding.time_diff + base_index - 1)) \
	        .withColumn("total_gr", func.lit(1))
	
	    for i in all_gr_index:
	        col_name = seed_for_adding.columns[i]
	        seed_for_adding = seed_for_adding.withColumn(col_name, func.when(
	            (seed_for_adding.min_index > i) | (seed_for_adding.max_index < i), 1).
	                                                     otherwise(seed_for_adding[col_name]))
	        seed_for_adding = seed_for_adding.withColumn(col_name, func.when(seed_for_adding.Year > eachyear,
	                                                                         seed_for_adding[col_name] ** (-1)).
	                                                     otherwise(seed_for_adding[col_name]))
	        seed_for_adding = seed_for_adding.withColumn("total_gr", seed_for_adding.total_gr * seed_for_adding[col_name])
	
	    seed_for_adding = seed_for_adding.withColumn("final_gr",
	                                                 func.when(seed_for_adding.total_gr < 2, seed_for_adding.total_gr).
	                                                 otherwise(2))
	    seed_for_adding = seed_for_adding \
	        .withColumn("Sales", seed_for_adding.Sales * seed_for_adding.final_gr) \
	        .withColumn("Year", func.lit(eachyear))
	    seed_for_adding = seed_for_adding.withColumn("year_month", seed_for_adding.Year * 100 + seed_for_adding.Month)
	    seed_for_adding = seed_for_adding.withColumn("year_month", seed_for_adding["year_month"].cast(DoubleType()))
	
	    seed_for_adding = seed_for_adding.withColumnRenamed("CITYGROUP", "City_Tier_2010") \
	        .join(price, on=["min2", "year_month", "City_Tier_2010"], how="inner")
	    seed_for_adding = seed_for_adding.withColumn("Units", func.when(seed_for_adding.Sales == 0, 0).
	                                                 otherwise(seed_for_adding.Sales / seed_for_adding.Price)) \
	        .na.fill({'Units': 0})
	
	    if empty == 0:
	        adding_data = seed_for_adding
	    else:
	        adding_data = adding_data.union(seed_for_adding)
	    empty = empty + 1
	

	# 1.8 合并补数部分和原始部分:
	# combind_data
	raw_data_adding = (raw_data.withColumn("add_flag", func.lit(0))) \
	    .union(adding_data.withColumn("add_flag", func.lit(1)).select(raw_data.columns + ["add_flag"]))
	# 输出第一阶段补数结果
	raw_data_adding = raw_data_adding.repartition(2)
	raw_data_adding.write.format("parquet") \
		.mode("overwrite").save(raw_data_adding_path)
	
	# 1.9 进一步为最后一年独有的医院补最后一年的缺失月（可能也要考虑第一年）:
	
	#      （可能也要考虑第一年）:
	print(years)
	
	# 只在最新年份出现的PHA-医院
	new_hospital = (original_range.where(original_range.Year == max(years)).select("PHA").distinct()) \
	    .subtract(original_range.where(original_range.Year != max(years)).select("PHA").distinct()) \
	    .toPandas()
	print("以下是最新一年出现的医院:" + str(new_hospital["PHA"].tolist()))
	new_hospital.to_excel(new_hospital_path)
	
	# 最新年份没有的月份
	missing_months = (original_range.where(original_range.Year != max(years)).select("Month").distinct()) \
	    .subtract(original_range.where(original_range.Year == max(years)).select("Month").distinct())
	
	# 如果最新年份有缺失月份，特殊处理
	if missing_months.count() == 0:
	    adding_data_new = raw_data_adding
	else:
	    number_of_existing_months = 12 - missing_months.count()
	    group_cols = set(raw_data_adding.columns).difference(
	        set(['Month', 'Sales', 'Units', '季度', "sales_value__rmb_", "total_units", "counting_units", "year_month"]))
	    adding_data_new = raw_data_adding \
	        .where(raw_data_adding.add_flag == 1) \
	        .where(raw_data_adding.PHA.isin(new_hospital["PHA"].tolist())) \
	        .groupBy(list(group_cols)).agg({"Sales": "sum", "Units": "sum"})
	    adding_data_new = adding_data_new \
	        .withColumn("Sales", adding_data_new["sum(Sales)"] / number_of_existing_months) \
	        .withColumn("Units", adding_data_new["sum(Units)"] / number_of_existing_months) \
	        .crossJoin(missing_months)
	    same_names = list(set(raw_data_adding.columns).intersection(set(adding_data_new.columns)))
	    adding_data_new = raw_data_adding.select(same_names) \
	        .union(adding_data_new.select(same_names))
	
	# 输出补数结果 adding_data_new
	adding_data_new.repartition(2).write.format("parquet") \
	    .mode("overwrite").save("/user/ywyuan/max/Sankyo/adding_data_new")
	adding_data_new.show(2)

		
