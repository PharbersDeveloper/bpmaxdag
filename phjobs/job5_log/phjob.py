# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

功能描述：job5: join 回cpa 数据+生成日志 
  * @author yzy
  * @version 0.0
  * @since 2020/08/25
  * @note 输入数据：cpa_match
		落盘数据：
  
"""

import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as func
import re
import numpy as np
from pyspark.sql.window import Window


def execute():
	"""
		please input your code below
	"""
	
	print("--"*80)
	print("程序start: job5 log")
	
	# 读取s3桶中的数据
	spark = SparkSession.builder \
		.master("yarn") \
		.appName("BPBatchDAG") \
		.config("spark.driver.memory", "1g") \
		.config("spark.executor.cores", "1") \
		.config("spark.executor.instance", "1") \
		.config("spark.executor.memory", "1g") \
		.config('spark.sql.codegen.wholeStage', False) \
		.enableHiveSupport() \
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
  
	cpa_match = spark.read.parquet("s3a://ph-max-auto/2020-08-11/BPBatchDAG/cpa_match")
	cpa_input_data = spark.read.parquet("s3a://ph-stream/common/public/pfizer_test/0.0.1")
	cpa_input_data2 = spark.read.parquet("s3a://ph-stream/common/public/pfizer_check")
	
	# cpa_match.show(5)
	# print(cpa_match.count())  # 57099
	cpa_input_data2.show(5)
	print(cpa_input_data2.count())  # 36166
	
	cpa_check = spark.read.parquet("s3a://ph-stream/test/inDfWithDistance").distinct()
	cpa_check.show(5)
	print(cpa_check.count())  # 25185


	print("程序end: job5 log")
	print("--"*80)
	
	
execute()