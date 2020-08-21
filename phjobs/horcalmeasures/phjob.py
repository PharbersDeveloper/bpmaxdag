# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as func
from phlogs.phlogs import phlogger
from pyspark.sql.functions import col
from pyspark.sql.functions import sum
from pyspark.sql.functions import lit
from pyspark.sql.functions import udf
from pyspark.sql.functions import monotonically_increasing_id
import pandas as pd  
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.functions import array
from pyspark.sql.functions import array_position


def execute(a, b):
	spark = SparkSession.builder \
        .master("yarn") \
        .appName("data cube cal hor measures") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.cores", "2") \
        .config("spark.executor.instance", "4") \
        .config("spark.executor.memory", "2g") \
        .config('spark.sql.codegen.wholeStage', False) \
        .getOrCreate()
        
    # access_key = os.getenv("AWS_ACCESS_KEY_ID")
    # secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
	access_key = "AKIAWPBDTVEAJ6CCFVCP"
	secret_key = "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"
	if access_key is not None:
		spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
		spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
		spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
		spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
		# spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
		spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.cn-northwest-1.amazonaws.com.cn")

	phlogger.info("create calculate measures base on time dimension")

	def lattice2joincondi_acc(cur, dim):
		par = []
		for tmp in cur:
			par.append(str(tmp))

		time = ["YEAR", "QUARTER", "MONTH"]
		geo = ["COUNTRY_NAME", "PROVINCE_NAME", "CITY_NAME"]
		prod = ["COMPANY", "MKT", "MOLE_NAME", "PRODUCT_NAME"]
		
		tmp = list(set(cur).intersection(set(prod))) if dim is "prod" else list(set(cur).intersection(set(geo))) if dim is "geo" else list(set(cur).intersection(set(time)))
		if len(tmp) is 0:
			return []
		else:
			if dim is "prod":
				return prod[0:prod.index(tmp[0]) + 1]
			elif dim is "geo":
				return geo[0:geo.index(tmp[0]) + 1]
			elif dim is "time":
				return time[0:time.index(tmp[0]) + 1]
			else:
				return []
	
	def lattice2joincondi(cur):
		res = []
		dims = ["prod", "geo", "time"]
		for dim in dims:
			res.extend(lattice2joincondi_acc(cur, dim))
		return res
	
	df = spark.read.parquet("s3a://ph-max-auto/2020-08-11/cube/dest/8cd67399-3eeb-4f47-aaf9-9d2cc4258d90/result2/final-result-ver-measures-3")
	df.persist()
	df.show()
	
	df_lattices = df.select("LATTLES").distinct().repartition(1).withColumn("LATTLES_ID", monotonically_increasing_id()).toPandas()
	print df_lattices
	
	"""
		lattices 的横向，只有时间维度，也就是x轴
		换句话说，x轴一定是连续的 continuous 
	"""
	@pandas_udf(ArrayType(DoubleType()), PandasUDFType.GROUPED_AGG) 
	def step_mapping_udf(v):
		res = []
		for tmp in v:
			res.append(tmp[0])
			res.append(tmp[1])
		return res
		
	@udf(returnType=DoubleType())
	def hor_time_step_udf(cat, v):
		if cat is "MONTH":
			if v % 100 == 1:
				return ((v / 10000 - 1) * 100 + 4) * 100 + 12
			else:
				return v - 1
		elif cat == "QUARTER":
			if v % 100 == 1:
				return (v / 100 - 1) * 100 + 4
			else:
				return v - 1
		elif cat == "YEAR":
			return v - 1
		else:
			return -1
	
	@udf(returnType=DoubleType())
	def hor_time_value_udf(pv, mp):
		if pv in mp:
			return mp[mp.index(pv) + 1]
		else:
			return 0.0
			
	for idx, row in df_lattices.iterrows():
		print idx
		print row
		df_c = df.where(col("LATTLES") == row["LATTLES"])
		# df_c.show()
		cur_l = row["LATTLES"].replace("%5B", "").replace("%5D", "").replace(" ", "").split(",")
		condi = lattice2joincondi(cur_l)
		
		if "MONTH" in cur_l:
			"""
				月度lattices平移
			"""
			df_c = df_c.withColumn("SALES_MAPPING", array("MONTH", "SALES_VALUE")).withColumn("TIME_PROVIOUS_LEVEL", hor_time_step_udf(col("TIME_CUR_LEVEL"), col("MONTH")))
			df_c = df_c.withColumn("SHARE_MAPPING", array("MONTH", "MARKET_SHARE")).withColumn("TIME_PROVIOUS_LEVEL", hor_time_step_udf(col("TIME_CUR_LEVEL"), col("MONTH")))
			condi.remove("MONTH")
			condi.remove("QUARTER")
			condi.remove("YEAR")
		elif "QUARTER" in cur_l:
			"""
				季度lattices平移
			"""
			df_c = df_c.withColumn("SALES_MAPPING", array("QUARTER", "SALES_VALUE")).withColumn("TIME_PROVIOUS_LEVEL", hor_time_step_udf(col("TIME_CUR_LEVEL"), col("QUARTER")))
			df_c = df_c.withColumn("SHARE_MAPPING", array("QUARTER", "MARKET_SHARE")).withColumn("TIME_PROVIOUS_LEVEL", hor_time_step_udf(col("TIME_CUR_LEVEL"), col("QUARTER")))
			condi.remove("QUARTER")
			condi.remove("YEAR")
		elif "YEAR" in cur_l:
			"""
				年度lattices平移
			"""
			df_c = df_c.withColumn("SALES_MAPPING", array("YEAR", "SALES_VALUE")).withColumn("TIME_PROVIOUS_LEVEL", hor_time_step_udf(col("TIME_CUR_LEVEL"), col("YEAR")))
			df_c = df_c.withColumn("SHARE_MAPPING", array("YEAR", "MARKET_SHARE")).withColumn("TIME_PROVIOUS_LEVEL", hor_time_step_udf(col("TIME_CUR_LEVEL"), col("YEAR")))
			condi.remove("YEAR")
		else:
			pass
		

		columns = ["YEAR", "QUARTER", "MONTH", \
			"COMPANY", "MKT", "MOLE_NAME", "PRODUCT_NAME", \
			"COUNTRY_NAME", "PROVINCE_NAME", "CITY_NAME", \
			"SALES_VALUE", "SALES_QTY", \
			"PROD_CUR_LEVEL", "GEO_CUR_LEVEL", "TIME_CUR_LEVEL", \
			"PROD_PARPENT_VALUE", "GEO_PARPENT_VALUE", "TIME_PARPENT_VALUE", "PROD_PP_VALUE", \
			"LATTLES", \
			"MARKET_SHARE", "MOLE_SHARE", "PROD_MOLE_SHARE", \
			"TIME_PROVIOUS_VALUE", "TIME_PROVIOUS_SHARE_VALUE", \
			"MARKET_SHARE_GROWTH", "SALES_GROWTH", "EI"]

		df_map = df_c.groupBy(condi).agg(step_mapping_udf(df_c.SALES_MAPPING).alias("SALES_MAPPING"), step_mapping_udf(df_c.SHARE_MAPPING).alias("SHARE_MAPPING"))
		df_c = df_c.drop("SALES_MAPPING", "SHARE_MAPPING").join(df_map, on=condi, how="left") \
				.withColumn("TIME_PROVIOUS_VALUE", hor_time_value_udf(col("TIME_PROVIOUS_LEVEL"), col("SALES_MAPPING"))) \
				.withColumn("TIME_PROVIOUS_SHARE_VALUE", hor_time_value_udf(col("TIME_PROVIOUS_LEVEL"), col("SHARE_MAPPING")))
		df_c = df_c.withColumn("EI", df_c.MARKET_SHARE / df_c.TIME_PROVIOUS_SHARE_VALUE) \
					.withColumn("SALES_GROWTH", (df_c.TIME_PROVIOUS_VALUE - df_c.SALES_VALUE) / df_c.TIME_PROVIOUS_VALUE) \
					.withColumn("MARKET_SHARE_GROWTH", (df_c.MARKET_SHARE - df_c.TIME_PROVIOUS_SHARE_VALUE) / df_c.TIME_PROVIOUS_SHARE_VALUE) 
		
		df_c.select(columns).repartition(1).write.mode("append").parquet("s3a://ph-max-auto/2020-08-11/cube/dest/8cd67399-3eeb-4f47-aaf9-9d2cc4258d90/result2/final-result-ver-hor-measures")
	