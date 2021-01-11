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
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.window import Window
from pyspark.sql.functions import rank
from pyspark.sql.functions import desc
from pyspark.sql.functions import monotonically_increasing_id


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

	phlogger.info("create calculate ice cube with cal measures")
	
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
		
	def icelattice2joincondi_acc(cur, dim, ice):
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
				return prod[0:prod.index(tmp[0]) + (0 if ice == "prod" else 1)]
			elif dim is "geo":
				return geo[0:geo.index(tmp[0]) + (0 if ice == "geo" else 1)]
			elif dim is "time":
				return time[0:time.index(tmp[0]) + (0 if ice == "time" else 1)]
			else:
				return []
	
	def icelattice2joincondi(cur, ice):
		res = []
		dims = ["prod", "geo", "time"]
		for dim in dims:
			res.extend(icelattice2joincondi_acc(cur, dim, ice))
		return res
	
	df = spark.read.parquet("s3a://ph-max-auto/2020-08-11/cube/dest/8cd67399-3eeb-4f47-aaf9-9d2cc4258d90/result2/final-result-ver-hor-measures")
	df.printSchema()
	df.show()
	
	df_lattices = df.select("LATTLES").distinct().repartition(1).withColumn("LATTLES_ID", monotonically_increasing_id()).toPandas()
	print df_lattices

	for idx, row in df_lattices.iterrows():
		print idx
		print row
		df_c = df.where(col("LATTLES") == row["LATTLES"])
		print df_c.count()
		cur_l = row["LATTLES"].replace("%5B", "").replace("%5D", "").replace(" ", "").split(",")
		condi = lattice2joincondi(cur_l)
		print condi
		
		"""
			现在的ice cube 没有多大意义
		"""
		ice_dims = ["prod", "geo"]
		for ice_dim in ice_dims:
			ice_condi = icelattice2joincondi(cur_l, ice_dim)
			print ice_condi
			window = Window.partitionBy(ice_condi).orderBy(desc("SALES_VALUE")).rowsBetween(Window.unboundedPreceding, Window.currentRow)
			df_t = df_c.withColumn("ICE", lit(ice_dim)).withColumn("rank", rank().over(window)).filter(col("rank") <= 10)
			print df_t.count()
			df_c.repartition(1).write.mode("append").parquet("s3a://ph-max-auto/2020-08-11/cube/dest/8cd67399-3eeb-4f47-aaf9-9d2cc4258d90/result2/ice-cube")