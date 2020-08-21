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
from pyspark.sql.functions import max
from pyspark.sql.functions import monotonically_increasing_id


def execute(a, b):
	spark = SparkSession.builder \
        .master("yarn") \
        .appName("data cube cal measures") \
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
	geo_level_udf = udf(lambda x: "CITY_NAME" if "CITY_NAME" in x else "PROVINCE_NAME" if "PROVINCE_NAME" in x else "COUNTRY_NAME", StringType())
	time_level_udf = udf(lambda x: "YEAR" if "YEAR" in x else "QUARTER" if "QUARTER" in x else "MONTH", StringType())
	time_parent_level_udf = udf(lambda x: "*" if "YEAR" in x else "YEAR" if "QUARTER" in x else "QUARTER", StringType())
	prod_level_udf = udf(lambda x: "PRODUCT_NAME" if "PRODUCT_NAME" in x else "MKT" if "MKT" in x else "MOLE_NAME" if "MOLE_NAME" in x else "COMPANY", StringType())
	prod_parent_level_udf = udf(lambda x: "MOLE" if "PRODUCT_NAME" in x else "MKT" if "MOLE_NAME" in x else "COMPANY" if "MKT" in x else "*", StringType())
	
	def ver_parent_condi(cur, dim):
		par = []
		for tmp in cur:
			par.append(str(tmp))

		time = ["YEAR", "QUARTER", "MONTH"]
		geo = ["COUNTRY_NAME", "PROVINCE_NAME", "CITY_NAME"]
		prod = ["COMPANY", "MKT", "MOLE_NAME", "PRODUCT_NAME"]
		tmp = list(set(cur).intersection(set(prod[1:]))) if dim is "prod" else list(set(cur).intersection(set(geo[1:]))) if dim is "geo" else list(set(cur).intersection(set(time[1:])))
		if len(tmp) is 0:
			return []
		else:
			if dim is "prod":
				rp = prod[prod.index(tmp[0]) - 1]
				par = [rp if x == tmp[0] else x for x in par]
			elif dim is "geo":
				rp = geo[geo.index(tmp[0]) - 1]
				par = [rp if x == tmp[0] else x for x in par]
			elif dim is "time":
				rp = time[time.index(tmp[0]) - 1]
				par = [rp if x == tmp[0] else x for x in par]
			else:
				pass
				
		return par
		
		
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
		
	"""
		# assumption 1: every lattice always have all three dimensions
		# we use CUBOIDS_ID for the first time
		
		计算度量中很多是lattices之间的平移，也就是一个join的操作
		
		寻找层次间的平移, GEO 层级好像没有计算度量需要纵向的平移
		优先从产品层面平移吧
	"""
	
	df = spark.read.parquet("s3a://ph-max-auto/2020-08-11/cube/dest/8cd67399-3eeb-4f47-aaf9-9d2cc4258d90/result2/final-result-2") \
			.where((col("CUBOIDS_ID") == 3))
	df = df.withColumn("PROD_CUR_LEVEL", prod_level_udf(col("LATTLES"))) \
			.withColumn("GEO_CUR_LEVEL", geo_level_udf(col("LATTLES"))) \
			.withColumn("TIME_CUR_LEVEL", time_level_udf(col("LATTLES")))
	df.persist()
	df.show()
		

	df_lattices = df.select("LATTLES", "CUBOIDS_NAME").distinct().repartition(1).withColumn("LATTLES_ID", monotonically_increasing_id()).toPandas()
	print df_lattices
	columns = ["YEAR", "MONTH", "QUARTER", "COUNTRY_NAME", "PROVINCE_NAME", "CITY_NAME", "MKT", "COMPANY", "MOLE_NAME", "PRODUCT_NAME", "CUBOIDS_ID", "CUBOIDS_NAME", "LATTLES", "apex", "dimension_name", "dimension_value"]

	for idx, row in df_lattices.iterrows():
		print idx
		print row
		df_c = df.where(col("LATTLES") == row["LATTLES"])
		# df_c.show()
		cur_l = row["LATTLES"].replace("%5B", "").replace("%5D", "").replace(" ", "").split(",")
		# 产品维度层面
		cur_p = ver_parent_condi(cur_l, "prod")
		if len(cur_p) is not 0:
			str_cur_p = str(cur_p).replace("[", "%5B").replace("]", "%5D").replace("'", "")
			condi_p = lattice2joincondi(cur_p)
			df_p = df.where(col("LATTLES") == str_cur_p).select(condi_p + ["SALES_VALUE"]).withColumnRenamed("SALES_VALUE", "PROD_PARPENT_VALUE")
			df_c = df_c.join(df_p, on=condi_p, how="left")
		else:
			df_c = df_c.withColumn("PROD_PARPENT_VALUE", lit(0.0))
		
		# 地理维度层面
		cur_g = ver_parent_condi(cur_l, "geo")
		if len(cur_g) is not 0:
			str_cur_g = str(cur_g).replace("[", "%5B").replace("]", "%5D").replace("'", "")
			condi_g = lattice2joincondi(cur_g)
			df_g = df.where(col("LATTLES") == str_cur_g).select(condi_g + ["SALES_VALUE"]).withColumnRenamed("SALES_VALUE", "GEO_PARPENT_VALUE")
			df_c = df_c.join(df_g, on=condi_g, how="left")
		else:
			df_c = df_c.withColumn("GEO_PARPENT_VALUE", lit(0.0))
			
		# 时间维度层面
		cur_t = ver_parent_condi(cur_l, "time")
		if len(cur_t) is not 0:
			str_cur_t = str(cur_t).replace("[", "%5B").replace("]", "%5D").replace("'", "")
			condi_t = lattice2joincondi(cur_t)
			df_t = df.where(col("LATTLES") == str_cur_t).select(condi_t + ["SALES_VALUE"]).withColumnRenamed("SALES_VALUE", "TIME_PARPENT_VALUE")
			df_c = df_c.join(df_t, on=condi_t, how="left")
		else:
			df_c = df_c.withColumn("TIME_PARPENT_VALUE", lit(0.0))
			
		# lattice 横移两次，市场份额的算法
		if "PRODUCT_NAME" in cur_l:
			cur_pp = []
			for tmp in cur_l:
				cur_pp.append(str(tmp))
			cur_pp = ["MKT" if x == "PRODUCT_NAME" else x for x in cur_pp]
			if len(cur_pp) is not 0:
				str_cur_pp = str(cur_pp).replace("[", "%5B").replace("]", "%5D").replace("'", "")
				condi_pp = lattice2joincondi(cur_pp)
				df_t = df.where(col("LATTLES") == str_cur_pp).select(condi_pp + ["SALES_VALUE"]).withColumnRenamed("SALES_VALUE", "PROD_PP_VALUE")
				df_c = df_c.join(df_t, on=condi_pp, how="left")
			else:
				df_c = df_c.withColumn("PROD_PP_VALUE", lit(0.0))
		else:
			df_c = df_c.withColumn("PROD_PP_VALUE", lit(0.0))
		
		"""
			TODO: 由于主数据的确实，在这里地方会由于畸形的数据清洗问题
			导致数据未能正确的groupby，从而导致未能正确的join
			这里去除重复，以待改正
		"""
		condi = lattice2joincondi(cur_l)
		df_c = df_c.groupBy(condi).agg(
				max(df_c.SALES_VALUE).alias("SALES_VALUE"),
				max(df_c.SALES_QTY).alias("SALES_QTY"),
				max(df_c.PROD_PARPENT_VALUE).alias("PROD_PARPENT_VALUE"),
				max(df_c.GEO_PARPENT_VALUE).alias("GEO_PARPENT_VALUE"),
				max(df_c.TIME_PARPENT_VALUE).alias("TIME_PARPENT_VALUE"),
				max(df_c.PROD_PP_VALUE).alias("PROD_PP_VALUE")
			)
		ad_col = list(set(columns).difference(set(condi)))
		for tc in ad_col:
			if tc is "LATTLES":
				df_c = df_c.withColumn(tc, lit(row["LATTLES"]))
			elif tc is "dimension_name":
				df_c = df_c.withColumn(tc, lit(row["CUBOIDS_NAME"]))
			elif tc is "CUBOIDS_NAME":
				df_c = df_c.withColumn(tc, lit(row["CUBOIDS_NAME"]))
			elif tc is "MONTH":
				df_c = df_c.withColumn(tc, lit(-1))
			elif tc is "QUARTER":
				df_c = df_c.withColumn(tc, lit(-1))
			else:
				df_c = df_c.withColumn(tc, lit("*"))

		df_c.repartition(1).write.mode("append").parquet("s3a://ph-max-auto/2020-08-11/cube/dest/8cd67399-3eeb-4f47-aaf9-9d2cc4258d90/result2/final-result-ver-measures-2")

	"""
		应该分为两个job，或者在上面一次性计算
	"""
	# 计算各种增长率	
	df = spark.read.parquet("s3a://ph-max-auto/2020-08-11/cube/dest/8cd67399-3eeb-4f47-aaf9-9d2cc4258d90/result2/final-result-ver-measures-2")
	df = df.withColumn("PROD_CUR_LEVEL", prod_level_udf(col("LATTLES"))) \
			.withColumn("GEO_CUR_LEVEL", geo_level_udf(col("LATTLES"))) \
			.withColumn("TIME_CUR_LEVEL", time_level_udf(col("LATTLES")))
	
	@udf(returnType=DoubleType())
	def sales_share_udf(cat, s, ps, pps):
		if (cat == "PRODUCT_NAME") & (pps != 0):
			return s / pps
		else:
			return 0.0


	@udf(returnType=DoubleType())
	def mole_share_udf(cat, s, ps, pps):
		if (cat == "PRODUCT_NAME") & (pps != 0):
			return ps / pps
		elif (cat == "MOLE_NAME") & (ps != 0):
			return s / ps
		else:
			return 0.0

	@udf(returnType=DoubleType())
	def prod_mole_share_udf(cat, s, ps, pps):
		if (cat == "PRODUCT_NAME") & (ps != 0):
			return s / ps
		else:
			return 0.0
	
	# 计算各种份额	
	df = df.withColumn("MARKET_SHARE", sales_share_udf(col("PROD_CUR_LEVEL"), col("SALES_VALUE"), col("PROD_PARPENT_VALUE"), col("PROD_PP_VALUE"))) \
			.withColumn("MOLE_SHARE", mole_share_udf(col("PROD_CUR_LEVEL"), col("SALES_VALUE"), col("PROD_PARPENT_VALUE"), col("PROD_PP_VALUE"))) \
			.withColumn("PROD_MOLE_SHARE", prod_mole_share_udf(col("PROD_CUR_LEVEL"), col("SALES_VALUE"), col("PROD_PARPENT_VALUE"), col("PROD_PP_VALUE")))

	sch_columns = ["YEAR", "QUARTER", "MONTH", \
				"COMPANY", "MKT", "MOLE_NAME", "PRODUCT_NAME", \
				"COUNTRY_NAME", "PROVINCE_NAME", "CITY_NAME", \
				"SALES_VALUE", "SALES_QTY", \
				"PROD_CUR_LEVEL", "GEO_CUR_LEVEL", "TIME_CUR_LEVEL", \
				"PROD_PARPENT_VALUE", "GEO_PARPENT_VALUE", "TIME_PARPENT_VALUE", "PROD_PP_VALUE", \
				"LATTLES", \
				"MARKET_SHARE", "MOLE_SHARE", "PROD_MOLE_SHARE"]
	df.select(sch_columns).write.mode("overwrite").parquet("s3a://ph-max-auto/2020-08-11/cube/dest/8cd67399-3eeb-4f47-aaf9-9d2cc4258d90/result2/final-result-ver-measures-3")