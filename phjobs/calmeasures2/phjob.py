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
	"""
	
	schema = \
        StructType([ \
            StructField("YEAR", IntegerType()), \
            StructField("MONTH", IntegerType()), \
            StructField("QUARTER", LongType()), \
            StructField("COUNTRY_NAME", StringType()), \
            StructField("PROVINCE_NAME", StringType()), \
            StructField("CITY_NAME", StringType()), \
            StructField("MKT", StringType()), \
            StructField("COMPANY", StringType()), \
            StructField("MOLE_NAME", StringType()), \
            StructField("PRODUCT_NAME", StringType()), \
            StructField("CUBOIDS_ID", LongType()), \
            StructField("CUBOIDS_NAME", StringType()), \
            StructField("LATTLES", StringType()), \
            StructField("apex", StringType()), \
            StructField("dimension_name", StringType()), \
            StructField("dimension_value", StringType()), \
            StructField("SALES_QTY", DoubleType()), \
            StructField("SALES_VALUE", DoubleType())
        ])
	
	df = spark.read.parquet("s3a://ph-max-auto/2020-08-11/cube/dest/8cd67399-3eeb-4f47-aaf9-9d2cc4258d90/result2/final-result-2") \
			.where((col("CUBOIDS_ID") == 3))
	df = df.withColumn("PROD_CUR_LEVEL", prod_level_udf(col("LATTLES"))) \
			.withColumn("GEO_CUR_LEVEL", geo_level_udf(col("LATTLES"))) \
			.withColumn("TIME_CUR_LEVEL", time_level_udf(col("LATTLES")))
	df.persist()
	df.show()
		
	"""
		计算度量中很多是lattices之间的平移，也就是一个join的操作
	"""

	"""
		寻找层次间的平移, GEO 层级好像没有计算度量需要纵向的平移
		优先从产品层面平移吧
	"""
	df_lattices = df.select("LATTLES").distinct().repartition(1).withColumn("LATTLES_ID", monotonically_increasing_id()).toPandas()
	print df_lattices

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
				
		df_c.repartition(1).write.mode("append").parquet("s3a://ph-max-auto/2020-08-11/cube/dest/8cd67399-3eeb-4f47-aaf9-9d2cc4258d90/result2/final-result-ver-measures-2")
	