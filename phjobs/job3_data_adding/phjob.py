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

	raw_data_job2_out_path = "/user/ywyuan/max/Sankyo/raw_data_job2_out"
	raw_data = spark.read.parquet(raw_data_job2_out_path)

	poi_path = "/workspace/BP_Max_AutoJob/poi.xlsx"
	poi = pd.read_excel(poi_path)
	poi = poi["poi"].values.tolist()

	raw_data = raw_data.withColumn("S_Molecule_for_gr", func.when(raw_data["标准商品名"].isin(poi), raw_data["标准商品名"]).
	                               otherwise(raw_data.S_Molecule))

	# 补数部分的数量需要用价格得出
	# cal_price
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
	price_path = "/user/ywyuan/max/Sankyo/price"
	price = price.repartition(2)
	price.write.format("parquet") \
	    .mode("overwrite").save(price_path)

	model_month_r = 201912
	raw_data = raw_data.where(raw_data.Year < ((model_month_r // 100) + 1))

	# 1.4 计算样本医院连续性:
	# cal_continuity
	con = raw_data.select("Year", "Month", "PHA").distinct() \
	    .groupBy("PHA", "Year").count()

	con_whole_year = con.groupBy("PHA") \
	    .agg(func.max("count").alias("MAX"), func.min("count").alias("MIN"))
	con_dis = con.join(con_whole_year, on=["PHA"], how="left") \
	    .na.fill({'MAX': 0, 'MIN': 0})

	distribution = con_dis.select('MAX', 'MIN', 'PHA').distinct() \
	    .groupBy('MAX', 'MIN').count()
	
	con = con.repartition(2, "PHA")

	years = con.select("Year").distinct().toPandas()["Year"].sort_values().values.tolist()
	# 数据长变宽
	con = con.groupBy("PHA").pivot("Year").agg(func.sum('count')).fillna(0)
	
	for eachyear in years:
	    eachyear = str(eachyear)
	    con = con.withColumn(eachyear, con[eachyear].cast(DoubleType())) \
	        .withColumnRenamed(eachyear, "Year_" + eachyear)
	        
	# year列求和
	a = ""
	for i in con.columns[1:]:
	    a += ("con." + i + "+")
	a = a.strip('+')
	# a = con.Year_2018 + con.Year_2019
	con = con.withColumn("total", eval(a))
	# 最大最小值
	con = con.join(con_whole_year, on="PHA", how="left")
	
	# 1.5 计算样本分子增长率:
	# cal_growth
	def cal_growth(raw_data, max_month=12):
	    # TODO: 完整年用完整年增长，不完整年用不完整年增长
	    if max_month < 12:
	        raw_data = raw_data.where(raw_data.Month <= max_month)
	
	    gr_raw_data = raw_data.na.fill({"City_Tier_2010": 5.0})
	    gr_raw_data = gr_raw_data.withColumn("CITYGROUP", gr_raw_data.City_Tier_2010)
	
	    gr = gr_raw_data.groupBy("S_Molecule_for_gr", "CITYGROUP", "Year") \
	        .agg(func.sum(gr_raw_data.Sales).alias("value"))
	    gr = gr.repartition(2, ["S_Molecule_for_gr", "CITYGROUP"])
	
	    years = gr.select("Year").distinct().toPandas()["Year"].sort_values().values.tolist()
	    years = [str(i) for i in years]
	    newyears = ["Year_" + str(i) for i in years]
	    # 数据长变宽, 展开year
	    gr = gr.groupBy("S_Molecule_for_gr", "CITYGROUP").pivot("Year").agg(func.sum('value')).fillna(0)
	    gr = gr.select(["S_Molecule_for_gr", "CITYGROUP"] + years)
	    # 改名
	    for i in range(0, len(years)):
	        gr = gr.withColumnRenamed(years[i], newyears[i])
	    # 年增长计算 add_gr_cols
	    for i in range(0, len(years) - 1):
	        gr = gr.withColumn(("GR" + years[i][2:4] + years[i + 1][2:4]), gr[newyears[i + 1]] / gr[newyears[i]])
	    # modify_gr
	    for y in [name for name in gr.columns if name.startswith("GR")]:
	        gr = gr.withColumn(y, func.when(func.isnull(gr[y]) | (gr[y] > 10) | (gr[y] < 0.1), 1).
	                           otherwise(gr[y]))
	
	    gr_with_id = gr_raw_data.select('PHA', 'ID', 'City', 'CITYGROUP', 'Molecule', 'S_Molecule_for_gr') \
	        .distinct() \
	        .join(gr, on=["CITYGROUP", "S_Molecule_for_gr"], how="left")
	
	    return gr
	
	
	project_name = "Sankyo"
	
	# AZ-Sanofi 要特殊处理
	if project_name != "Sanofi" and project_name != "AZ":
	    gr = cal_growth(raw_data)
	else:
	    # year_missing = [2019]
	    # 完整年
	    gr_p1 = cal_growth(raw_data.where(raw_data.Year.isin(year_missing)))
	    # 不完整年
	    gr_p2 = cal_growth(raw_data.where(
	        raw_data.Year.isin(year_missing + [y - 1 for y in year_missing] + [y + 1 for y in year_missing])),
	        max_month)
	
	    gr = (gr_p1.select("S_Molecule_for_gr", "CITYGROUP")).union(gr_p2.select("S_Molecule_for_gr", "CITYGROUP")) \
	        .distinct()
	    gr = gr.join(
	        gr_p1.select("S_Molecule_for_gr", "CITYGROUP", [name for name in gr_p1.columns if name.startswith("GR")]),
	        on=["S_Molecule_for_gr", "CITYGROUP"], how="left")
	    gr = gr.join(
	        gr_p2.select("S_Molecule_for_gr", "CITYGROUP", [name for name in gr_p2.columns if name.startswith("GR")]),
	        on=["S_Molecule_for_gr", "CITYGROUP"], how="left")
	
	gr = gr.repartition(2)
	gr_path_online = "/user/ywyuan/max/Sankyo/gr"
	gr.write.format("parquet") \
	    .mode("overwrite").save(gr_path_online)
	    
	# 1.6 原始数据格式整理:
	# trans_raw_data_for_adding
	gr = gr.select(["CITYGROUP", "S_Molecule_for_gr"] +
	               [name for name in gr.columns if name.startswith("GR1")]) \
	    .distinct()
	seed = raw_data.where(raw_data.PHA.isNotNull()) \
	    .orderBy(raw_data.Year.desc()) \
	    .withColumnRenamed("City_Tier_2010", "CITYGROUP") \
	    .join(gr, on=["S_Molecule_for_gr", "CITYGROUP"], how="left")
	
	seed.repartition(2).write.format("parquet") \
	    .mode("overwrite").save("/user/ywyuan/max/Sankyo/seed")
	    
	seed.persist()    
	# 1.7 补充各个医院缺失的月份:
	# add_data
	# 1. 得到年
	original_range = seed.select("Year", "Month", "PHA").distinct()
	
	years = original_range.select("Year").distinct() \
	    .orderBy(original_range.Year) \
	    .toPandas()["Year"].values.tolist()
	print(years)
	
	all_gr_index = [index for index, name in enumerate(seed.columns) if name.startswith("GR")]
	print(all_gr_index)
	
	# 3. 每年的补数
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
	
	# 测试：输出adding_data
	adding_data.repartition(2).write.format("parquet") \
	    .mode("overwrite").save("/user/ywyuan/max/Sankyo/adding_data")


	# 1.8 合并补数部分和原始部分:
	# combind_data
	raw_data_adding = (raw_data.withColumn("add_flag", func.lit(0))) \
	    .union(adding_data.withColumn("add_flag", func.lit(1)).select(raw_data.columns + ["add_flag"]))
	raw_data_adding = raw_data_adding.repartition(2)
	
	raw_data_adding.show(2)
