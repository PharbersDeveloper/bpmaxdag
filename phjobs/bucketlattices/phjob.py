# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import col
from pyspark.sql.functions import udf
from phlogs.phlogs import phlogger
import string
import pandas as pd


def execute(**kwargs):
	
	startDate = kwargs['start_date']
	jobId = kwargs['job_id']
	destPath = "s3a://ph-max-auto/" + startDate +"/cube/dest/" + jobId
	
	spark = SparkSession.builder \
        .master("yarn") \
        .appName("data cube bucket lattices job") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.instance", "2") \
        .config("spark.executor.memory", "2g") \
        .config('spark.sql.codegen.wholeStage', False) \
        .config("spark.sql.files.maxPartitionBytes", 10485760) \
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

	phlogger.info("bucket the lattices data from batch")
	phlogger.info("the better way is to use streamming group directly, but we don't have the hbase or mpp database services")
	
	# schema = \
 #       StructType([ \
 #           StructField("QUARTER", LongType()), \
 #           StructField("COUNTRY_NAME", StringType()), \
 #           StructField("PROVINCE_NAME", StringType()), \
 #           StructField("CITY_NAME", StringType()), \
 #           StructField("MKT", StringType()), \
 #           StructField("MOLE_NAME", StringType()), \
 #           StructField("PRODUCT_NAME", StringType()), \
 #           StructField("SALES_QTY", DoubleType()), \
 #           StructField("SALES_VALUE", DoubleType()), \
 #           StructField("apex", StringType()), \
 #           StructField("dimension.name", StringType()), \
 #           StructField("dimension.value", StringType()), \
 #           StructField("YEAR", IntegerType()), \
 #           StructField("MONTH", IntegerType()), \
 #           StructField("COMPANY", StringType()), \
 #           StructField("CUBOIDS_ID", LongType()), \
 #           StructField("CUBOIDS_NAME", StringType()), \
 #           StructField("LATTLES", ArrayType(StringType()))
 #       ])
	
	# df = spark.read.schema(schema).parquet(destPath + "/lattices/content").where(col("CUBOIDS_ID") == 3)
	
	# array2str_udf = udf(lambda x: str(x).replace("u'", "").replace("'", "").replace("[", "%5B").replace("]", "%5D"), StringType())
	array2str_udf = udf(lambda x: "-".join(x), StringType())
	
	# tmp = spark.read.parquet(destPath + "/lattices/content").where(col("CUBOIDS_ID") == 3).withColumn("LATTLES_STR", array2str_udf(col("LATTLES")))
	# tmp.printSchema()
	# tmp.select("LATTLES_STR").show(20)
	
	spark.read.parquet(destPath + "/lattices/content").where(col("CUBOIDS_ID") == 3) \
		.withColumn("LATTLES_STR", array2str_udf(col("LATTLES"))) \
		.write \
		.partitionBy("YEAR", "MONTH", "CUBOIDS_ID", "LATTLES_STR") \
        .format("parquet") \
        .mode("overwrite") \
        .save(destPath + "/lattices-buckets-3/content")
