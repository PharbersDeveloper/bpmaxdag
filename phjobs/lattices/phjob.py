# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as func
from phlogs.phlogs import phlogger
from pyspark.sql.functions import explode
from pyspark.sql.functions import col
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import array
from pyspark.sql.functions import array_union
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import lit
import string
import pandas as pd


def execute(**kwargs):
	
	startDate = kwargs['start_date']
	jobId = kwargs['job_id']
	destPath = "s3a://ph-max-auto/" + startDate +"/cube/dest/" + jobId
	
	year = 2019
	month = 1

	spark = SparkSession.builder \
        .master("yarn") \
        .appName("data cube lattices job") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.cores", "2") \
        .config("spark.executor.instance", "4") \
        .config("spark.executor.memory", "2g") \
        .config('spark.sql.codegen.wholeStage', False) \
        .config("spark.sql.crossJoin.enabled", "true") \
        .config("spark.sql.autoBroadcastJoinThreshold", 1048576000) \
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

	phlogger.info("create data lattices with year " + str(year) + " and month " + str(month))
 
	"""   
	 |-- QUARTER: long (nullable = true)
	 |-- COUNTRY_NAME: string (nullable = false)
	 |-- PROVINCE_NAME: string (nullable = true)
	 |-- CITY_NAME: string (nullable = true)
	 |-- MKT: string (nullable = true)
	 |-- MOLE_NAME: string (nullable = true)
	 |-- PRODUCT_NAME: string (nullable = true)
	 |-- SALES_QTY: double (nullable = false)
	 |-- SALES_VALUE: double (nullable = false)
	 |-- apex: string (nullable = false)
	 |-- dimension.name: string (nullable = false)
	 |-- dimension.value: string (nullable = false)
	 |-- YEAR: integer (nullable = true)
	 |-- MONTH: integer (nullable = true)
	 |-- COMPANY: string (nullable = true)
	"""
	
	schema = \
        StructType([ \
            StructField("QUARTER", LongType()), \
            StructField("COUNTRY_NAME", StringType()), \
            StructField("PROVINCE_NAME", StringType()), \
            StructField("CITY_NAME", StringType()), \
            StructField("MKT", StringType()), \
            StructField("MOLE_NAME", StringType()), \
            StructField("PRODUCT_NAME", StringType()), \
            StructField("SALES_QTY", DoubleType()), \
            StructField("SALES_VALUE", DoubleType()), \
            StructField("apex", StringType()), \
            StructField("dimension.name", StringType()), \
            StructField("dimension.value", StringType()), \
            StructField("YEAR", IntegerType()), \
            StructField("MONTH", IntegerType()), \
            StructField("COMPANY", StringType())
        ])
   
	df = spark.read.schema(schema).parquet(destPath + "/content")

	cuboids_df = spark.read.parquet(destPath + "/meta/lattices")

	df.crossJoin(broadcast(cuboids_df)) \
			.write \
        	.format("parquet") \
        	.mode("overwrite") \
	        .save(destPath + "/lattices/content")
