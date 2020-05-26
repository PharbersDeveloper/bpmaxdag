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
	    
	seed.show()
