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
import pandas as pd

def execute(a, b):
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("data cube result job") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.instance", "1") \
        .config("spark.executor.memory", "1g") \
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
	
	df = spark.read.schema(schema).parquet("s3a://ph-max-auto/2020-08-11/cube/dest/8cd67399-3eeb-4f47-aaf9-9d2cc4258d90/result/lattices-result")
	df = df.withColumn("QUARTER", floor((col("MONTH")- 1) / 3 + 1))
	df.persist()
	
	# 1. QUARTER 的做法
	# df = df.where(("time" not in col("CUBOIDS_NAME")) | ("QUARTER" not in col("LATTLES")))
	df_quarter = df.where(("time" in col("CUBOIDS_NAME")) & ("QUARTER" in col("LATTLES")))
	df_quarter = df_quarter.groupBy("QUARTER").agg({
				"QUARTER": "first",
				"COUNTRY_NAME": "first",
				"PROVINCE_NAME": "first",
				"CITY_NAME": "first",
				"MKT": "first",
				"COMPANY": "first",
				"MOLE_NAME": "first",
				"PRODUCT_NAME": "first",
				"CUBOIDS_NAME": "first",
				"apex": "first",
				"dimension_name": "first",
				"dimension_value": "first",
				"SALES_QTY": "sum",
				"SALES_VALUE": "sum"
			}).withColumnRenamed("first(QUARTER)", "QUARTER") \
			.withColumnRenamed("first(COUNTRY_NAME)", "COUNTRY_NAME") \
			.withColumnRenamed("first(PROVINCE_NAME)", "PROVINCE_NAME") \
			.withColumnRenamed("first(CITY_NAME)", "CITY_NAME") \
			.withColumnRenamed("first(MKT)", "MKT") \
			.withColumnRenamed("first(COMPANY)", "COMPANY") \
			.withColumnRenamed("first(MOLE_NAME)", "MOLE_NAME") \
			.withColumnRenamed("first(PRODUCT_NAME)", "PRODUCT_NAME") \
			.withColumnRenamed("first(CUBOIDS_NAME)", "CUBOIDS_NAME") \
			.withColumnRenamed("first(apex)", "apex") \
			.withColumnRenamed("first(dimension_name)", "dimension_name") \
			.withColumnRenamed("first(dimension_value)", "dimension_value") \
			.withColumnRenamed("sum(SALES_QTY)", "SALES_QTY") \
			.withColumnRenamed("sum(SALES_VALUE)", "SALES_VALUE") \
			.select(sch_columns)
	# 2. year 的做法
	df_year = df.where(("time" in col("CUBOIDS_NAME")) & ("YEAR" in col("LATTLES")))
	df_year = df_year.groupBy("YEAR").agg({
				"QUARTER": "first",
				"COUNTRY_NAME": "first",
				"PROVINCE_NAME": "first",
				"CITY_NAME": "first",
				"MKT": "first",
				"COMPANY": "first",
				"MOLE_NAME": "first",
				"PRODUCT_NAME": "first",
				"CUBOIDS_NAME": "first",
				"apex": "first",
				"dimension_name": "first",
				"dimension_value": "first",
				"SALES_QTY": "sum",
				"SALES_VALUE": "sum"
			}).withColumnRenamed("first(QUARTER)", "QUARTER") \
			.withColumnRenamed("first(COUNTRY_NAME)", "COUNTRY_NAME") \
			.withColumnRenamed("first(PROVINCE_NAME)", "PROVINCE_NAME") \
			.withColumnRenamed("first(CITY_NAME)", "CITY_NAME") \
			.withColumnRenamed("first(MKT)", "MKT") \
			.withColumnRenamed("first(COMPANY)", "COMPANY") \
			.withColumnRenamed("first(MOLE_NAME)", "MOLE_NAME") \
			.withColumnRenamed("first(PRODUCT_NAME)", "PRODUCT_NAME") \
			.withColumnRenamed("first(CUBOIDS_NAME)", "CUBOIDS_NAME") \
			.withColumnRenamed("first(apex)", "apex") \
			.withColumnRenamed("first(dimension_name)", "dimension_name") \
			.withColumnRenamed("first(dimension_value)", "dimension_value") \
			.withColumnRenamed("sum(SALES_QTY)", "SALES_QTY") \
			.withColumnRenamed("sum(SALES_VALUE)", "SALES_VALUE") \
			.select(sch_columns)
			
	df = df.where(("time" not in col("CUBOIDS_NAME")) | ("MONTH" in col("LATTLES"))).union(df_quarter).union(df_year)
	df.write.parquet("s3a://ph-max-auto/2020-08-11/cube/dest/8cd67399-3eeb-4f47-aaf9-9d2cc4258d90/result/final-result")