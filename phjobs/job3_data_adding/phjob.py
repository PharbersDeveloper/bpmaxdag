# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
import click
import numpy as np
import pandas as pd

from pyspark.sql import SparkSession
import time
from pyspark.sql.types import *
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql import functions as func


@click.command()
@click.option('--a')
@click.option('--b')
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
	
	con.show(2)