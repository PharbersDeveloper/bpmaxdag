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
	
	df = spark.read.schema(schema).parquet("s3a://ph-max-auto/2020-08-11/cube/dest/8cd67399-3eeb-4f47-aaf9-9d2cc4258d90/result2/lattices-result").where(col("YEAR") == 2018).drop("QUARTER")
	df = df.withColumn("QUARTER", floor((col("MONTH") - 1) / 3 + 1))
	df.persist()

	
	# 1. QUARTER 的做法
	df_quarter = df.where((col("CUBOIDS_NAME").contains("time")) & (col("LATTLES").contains("QUARTER")))
	df_quarter = df_quarter.groupBy("YEAR", "QUARTER", "CUBOIDS_ID", "LATTLES").agg(
				first(df_quarter.COUNTRY_NAME).alias("COUNTRY_NAME"),
				first(df_quarter.PROVINCE_NAME).alias("PROVINCE_NAME"),
				first(df_quarter.CITY_NAME).alias("CITY_NAME"),
				first(df_quarter.MKT).alias("MKT"),
				first(df_quarter.COMPANY).alias("COMPANY"),
				first(df_quarter.MOLE_NAME).alias("MOLE_NAME"),
				first(df_quarter.PRODUCT_NAME).alias("PRODUCT_NAME"),
				first(df_quarter.CUBOIDS_NAME).alias("CUBOIDS_NAME"),
				first(df_quarter.apex).alias("apex"),
				first(df_quarter.dimension_name).alias("dimension_name"),
				first(df_quarter.dimension_value).alias("dimension_value"),
				sum(df_quarter.SALES_VALUE).alias("SALES_VALUE"),
				sum(df_quarter.SALES_QTY).alias("SALES_QTY")) \
			.withColumn("MONTH", lit(-1)) \
			.select(sch_columns)
	# 2. year 的做法
	df_year = df.where((col("CUBOIDS_NAME").contains("time")) & (col("LATTLES").contains("YEAR")))
	df_year = df_year.groupBy("YEAR", "CUBOIDS_ID", "LATTLES").agg(
				first(df_year.COUNTRY_NAME).alias("COUNTRY_NAME"),
				first(df_year.PROVINCE_NAME).alias("PROVINCE_NAME"),
				first(df_year.CITY_NAME).alias("CITY_NAME"),
				first(df_year.MKT).alias("MKT"),
				first(df_year.COMPANY).alias("COMPANY"),
				first(df_year.MOLE_NAME).alias("MOLE_NAME"),
				first(df_year.PRODUCT_NAME).alias("PRODUCT_NAME"),
				first(df_year.CUBOIDS_NAME).alias("CUBOIDS_NAME"),
				first(df_year.apex).alias("apex"),
				first(df_year.dimension_name).alias("dimension_name"),
				first(df_year.dimension_value).alias("dimension_value"),
				sum(df_year.SALES_VALUE).alias("SALES_VALUE"),
				sum(df_year.SALES_QTY).alias("SALES_QTY")) \
			.withColumn("QUARTER", lit(-1)) \
			.withColumn("MONTH", lit(-1)) \
			.select(sch_columns)
			
	filter_udf = udf(lambda cn,l: ("time" not in cn) | ("MONTH" in l), BooleanType())
	df = df.where(filter_udf(df.CUBOIDS_NAME, df.LATTLES)).union(df_quarter).union(df_year)
	df.write.parquet("s3a://ph-max-auto/2020-08-11/cube/dest/8cd67399-3eeb-4f47-aaf9-9d2cc4258d90/result2/final-result")