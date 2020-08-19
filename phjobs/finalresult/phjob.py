# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as func
from phlogs.phlogs import phlogger
from pyspark.sql.functions import udf
from pyspark.sql.functions import col
from pyspark.sql.functions import floor
from pyspark.sql.functions import lit
from pyspark.sql.functions import first
from pyspark.sql.functions import sum
from pyspark.sql.functions import udf
from pyspark.sql.functions import monotonically_increasing_id
import pandas as pd



def execute(a, b):
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("data cube result job") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.cores", "2") \
        .config("spark.executor.instance", "4") \
        .config("spark.executor.memory", "2g") \
        .config('spark.sql.codegen.wholeStage', False) \
        .config("spark.sql.files.maxRecordsPerFile", 33554432) \
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

	phlogger.info("final cube data")

	"""
		前面的计算会有一个bug, 因为分片计算, 在quarter上的聚合与年上的聚合是错的
		所以对结果在来一次
		因为小文件过多，很容易造成数据队列的oom
		
		同时还有类型不匹配的bug，以后修改
	"""
	
	"""
		root
		 |-- QUARTER: string (nullable = true)
		 |-- COUNTRY_NAME: string (nullable = true)
		 |-- PROVINCE_NAME: string (nullable = true)
		 |-- CITY_NAME: string (nullable = true)
		 |-- MKT: string (nullable = true)
		 |-- COMPANY: string (nullable = true)
		 |-- MOLE_NAME: string (nullable = true)
		 |-- PRODUCT_NAME: string (nullable = true)
		 |-- CUBOIDS_NAME: string (nullable = true)
		 |-- apex: string (nullable = true)
		 |-- dimension_name: string (nullable = true)
		 |-- dimension_value: string (nullable = true)
		 |-- SALES_QTY: double (nullable = true)
		 |-- SALES_VALUE: double (nullable = true)
		 |-- YEAR: Integer
		 |-- MONTH: Integer
		 |-- CUBOIDS_ID: integer (nullable = true)
		 |-- LATTLES: string (nullable = true)
	"""
	
	schema = \
        StructType([ \
            StructField("QUARTER", StringType()), \
            StructField("COUNTRY_NAME", StringType()), \
            StructField("PROVINCE_NAME", StringType()), \
            StructField("CITY_NAME", StringType()), \
            StructField("MKT", StringType()), \
            StructField("MOLE_NAME", StringType()), \
            StructField("PRODUCT_NAME", StringType()), \
            StructField("SALES_QTY", DoubleType()), \
            StructField("SALES_VALUE", DoubleType()), \
            StructField("apex", StringType()), \
            StructField("dimension_name", StringType()), \
            StructField("dimension_value", StringType()), \
            StructField("YEAR", IntegerType()), \
            StructField("MONTH", IntegerType()), \
            StructField("COMPANY", StringType()), \
            StructField("CUBOIDS_ID", LongType()), \
            StructField("CUBOIDS_NAME", StringType()), \
            StructField("LATTLES", StringType())
        ])
	
	sch_columns = ["YEAR", "MONTH", "QUARTER", "COUNTRY_NAME", "PROVINCE_NAME", "CITY_NAME", "MKT", "COMPANY", "MOLE_NAME", "PRODUCT_NAME", "CUBOIDS_ID", "CUBOIDS_NAME", "LATTLES", "apex", "dimension_name", "dimension_value", "SALES_QTY", "SALES_VALUE"]
	
	df = spark.read.schema(schema).parquet("s3a://ph-max-auto/2020-08-11/cube/dest/8cd67399-3eeb-4f47-aaf9-9d2cc4258d90/result2/lattices-result") \
			.where((col("YEAR") == 2019) & (col("CUBOIDS_ID") == 3)) \
			.drop("QUARTER")
	df = df.withColumn("QUARTER", floor((col("MONTH") - 1) / 3 + 1)).withColumn("QUARTER", col("QUARTER").cast(IntegerType()))
	df = df.withColumn("CUBOIDS_NAME", col("dimension_name"))
	df.persist()

	dim = spark.read.parquet("s3a://ph-max-auto/2020-08-11/cube/dest/8cd67399-3eeb-4f47-aaf9-9d2cc4258d90/meta/dimensions") \
			.repartition(1).withColumn("LEVEL", monotonically_increasing_id())
	dim_group_level = dim.groupBy("DIMENSION").agg({"LEVEL":"min"}).withColumnRenamed("min(LEVEL)", "EDGE")
	dim = dim.join(dim_group_level, how="left", on="DIMENSION").toPandas()
	print dim
	
	def leaf2Condi(df, lts):
	    res = []
	    for tmp in lts:
	        level = df[df["HIERARCHY"] == tmp].iloc[0]["LEVEL"]
	        edge = df[df["HIERARCHY"] == tmp].iloc[0]["EDGE"]
	        res.extend(list(df[(df["LEVEL"] >= edge) & (df["LEVEL"] <= level)]["HIERARCHY"]))
	    return res

	df_lattices = df.select("LATTLES", "CUBOIDS_NAME").distinct().repartition(1).withColumn("LATTLES_ID", monotonically_increasing_id()).toPandas()
	print df_lattices
	columns = ["YEAR", "MONTH", "QUARTER", "COUNTRY_NAME", "PROVINCE_NAME", "CITY_NAME", "MKT", "COMPANY", "MOLE_NAME", "PRODUCT_NAME", "CUBOIDS_ID", "CUBOIDS_NAME", "LATTLES", "apex", "dimension_name", "dimension_value"]
	sch_columns = ["YEAR", "MONTH", "QUARTER", "COUNTRY_NAME", "PROVINCE_NAME", "CITY_NAME", "MKT", "COMPANY", "MOLE_NAME", "PRODUCT_NAME", "CUBOIDS_ID", "CUBOIDS_NAME", "LATTLES", "apex", "dimension_name", "dimension_value", "SALES_QTY", "SALES_VALUE"]

	for idx, row in df_lattices.iterrows():
		df_c = df.where(col("LATTLES") == row["LATTLES"])
		lts = []
		cur_l = row["LATTLES"].replace("%5B", "").replace("%5D", "").replace(" ", "").split(",")
		for tmp in cur_l:
			lts.append(str(tmp))
		condi = ["CUBOIDS_ID"]
		condi.extend(leaf2Condi(dim, lts))
		print "===> alfred test"
		print row["LATTLES"]
		print condi
		print "===> alfred test"
    	
		ad_col = list(set(columns).difference(set(condi)))
		print ad_col
		
		if "MONTH" in row["LATTLES"]:
			# 0. MONTH 的做法
			print "MONTH"
			df_c.select(sch_columns).repartition(1).write.mode("append").parquet("s3a://ph-max-auto/2020-08-11/cube/dest/8cd67399-3eeb-4f47-aaf9-9d2cc4258d90/result2/final-result")
		elif "QUARTER" in row["LATTLES"]:
			# 1. QUARTER 的做法
			print "QUARTER"
			print cur_l
			df_c = df_c.groupBy(condi).agg(
					sum(df_c.SALES_VALUE).alias("SALES_VALUE"),
					sum(df_c.SALES_QTY).alias("SALES_QTY")
				)
			print ad_col
			
			for tc in ad_col:
				if tc is "LATTLES":
					df_c = df_c.withColumn(tc, lit(row["LATTLES"]))
				elif tc is "dimension_name":
					df_c = df_c.withColumn(tc, lit(row["CUBOIDS_NAME"]))
				elif tc is "CUBOIDS_NAME":
					df_c = df_c.withColumn(tc, lit(row["CUBOIDS_NAME"]))
				elif tc is "MONTH":
					df_c = df_c.withColumn(tc, lit(-1))
				else:
					df_c = df_c.withColumn(tc, lit("*"))
			df_c.select(sch_columns).repartition(1).write.mode("append").parquet("s3a://ph-max-auto/2020-08-11/cube/dest/8cd67399-3eeb-4f47-aaf9-9d2cc4258d90/result2/final-result")
		elif "YEAR" in row["LATTLES"]:
			# 2. YEAR 的做
			print "YEAR"
			print cur_l
			df_c = df_c.groupBy(condi).agg(
					sum(df_c.SALES_VALUE).alias("SALES_VALUE"),
					sum(df_c.SALES_QTY).alias("SALES_QTY")
				)
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
			df_c.select(sch_columns).repartition(1).write.mode("append").parquet("s3a://ph-max-auto/2020-08-11/cube/dest/8cd67399-3eeb-4f47-aaf9-9d2cc4258d90/result2/final-result")
		else:
			pass
	