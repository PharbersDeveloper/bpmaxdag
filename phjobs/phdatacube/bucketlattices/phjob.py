# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import col
from pyspark.sql.functions import udf
import logging
import os
import string
import pandas as pd
from time import sleep
from uuid import uuid4


def execute(**kwargs):

	logging.basicConfig(format='%(asctime)s %(filename)s %(funcName)s %(lineno)d %(message)s')
	logger = logging.getLogger('driver_logger')
	logger.setLevel(logging.INFO)
	logger.info("Origin kwargs = {}.".format(str(kwargs)))

	# input required
	lattices_content_path = kwargs['lattices_content_path']
	if lattices_content_path == u'default':
		raise Exception("Invalid lattices_content_path!", lattices_content_path)

	# output
	lattices_bucket_content_path = kwargs['lattices_bucket_content_path']
	if lattices_bucket_content_path == u'default':
		jobName = "lattices"
		version = kwargs['version']
		if not version:
			raise Exception("Invalid version!", version)
		runId = kwargs['run_id']
		if runId == u'default':
			runId = str(uuid4())
			logger.info("runId is " + runId)
		jobId = kwargs['job_id']
		if jobId == u'default':
			jobId = str(uuid4())
			logger.info("jobId is " + jobId)
		destPath = "s3a://ph-max-auto/" + version +"/jobs/runId_" + runId + "/" + jobName +"/jobId_" + jobId
		logger.info("DestPath is {}.".format(destPath))
		lattices_bucket_content_path = destPath + "/lattices-buckets/content"
	logger.info("lattices_bucket_content_path is {}.".format(lattices_bucket_content_path))
	lattices_bucket_checkpoint_path = "checkpoint".join(lattices_bucket_content_path.rsplit('content', 1))
	logger.info("lattices_bucket_checkpoint_path is {}.".format(lattices_bucket_checkpoint_path))

	spark = SparkSession.builder \
        .master("yarn") \
        .appName("data cube bucket lattices job") \
        .config("spark.driver.memory", "5g") \
        .config("spark.executor.cores", "2") \
        .config("spark.executor.instance", "2") \
        .config("spark.executor.memory", "5g") \
        .config('spark.sql.codegen.wholeStage', False) \
        .config("spark.sql.files.maxPartitionBytes", 10485760) \
        .config("spark.shuffle.memoryFraction", "0.4") \
        .getOrCreate()


    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

	if access_key is not None:
		spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
		spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
		spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
		spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
		# spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
		spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.cn-northwest-1.amazonaws.com.cn")

	logger.info("bucket the lattices data from batch")
	logger.info("the better way is to use streamming group directly, but we don't have the hbase or mpp database services")

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
            StructField("COMPANY", StringType()), \
            StructField("CUBOIDS_ID", LongType()), \
            StructField("CUBOIDS_NAME", StringType()), \
            StructField("LATTLES", ArrayType(StringType()))
    	])


	df = spark.readStream.schema(schema).parquet(lattices_content_path)
	query = df.writeStream \
				.partitionBy("YEAR", "MONTH", "CUBOIDS_ID", "LATTLES") \
        		.format("parquet") \
        		.outputMode("append") \
        		.option("checkpointLocation", lattices_bucket_checkpoint_path) \
        		.option("path", lattices_bucket_content_path) \
        		.start()

	# query.awaitTermination()

	stop = False
	# intervalMills = 10 * 1000 # 每隔10秒扫描一次消息是否存在
	intervalSeconds = 60	# 每隔60秒扫描一次消息是否存在

	while not stop :
		logger.info("Please wait for data processing.")
		sleep(intervalSeconds)
		# stop = query.awaitTerminationOrTimeout(intervalMills)
		# logger.info("Now query stop is " + status)

		isDataAvailable = query.status['isDataAvailable']
		logger.info("Now query isDataAvailable is " + str(isDataAvailable))
		# if not stop and not isDataAvailable :
		if not isDataAvailable :
			logger.info("Now stop query after a few seconds")
			sleep(intervalSeconds)
			query.stop()
			stop = True
		sleep(intervalSeconds)
