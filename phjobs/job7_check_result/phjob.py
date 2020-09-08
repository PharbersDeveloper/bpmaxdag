# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

功能描述：job7_check_result
  * @author yzy
  * @version 0.0
  * @since 2020/09/08
  * @note  落盘数据：cpa_prod_join
  
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as func

def execute():
	"""
		please input your code below
	"""
	
	print("--"*80)
	print("程序start: job7_check_result")
	
	os.environ["PYSPARK_PYTHON"] = "python3"
	# 读取s3桶中的数据
	spark = SparkSession.builder \
		.master("yarn") \
		.appName("BPBatchDAG") \
		.config("spark.driver.memory", "1g") \
		.config("spark.executor.cores", "1") \
		.config("spark.executor.instance", "1") \
		.config("spark.executor.memory", "1g") \
		.config('spark.sql.codegen.wholeStage', False) \
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
  
	# 需要的所有表格命名
	out_path = "s3a://ph-max-auto/2020-08-11/BPBatchDAG/pfizer_check/0.0.1/cpa_match"
	cpa_prod_join_null = spark.read.parquet(out_path)
	# cpa_prod_join_null.show(5)
	# print(cpa_prod_join_null.count())
	xixi1=cpa_prod_join_null.toPandas()
	xixi1.to_excel('pfizer_match.xlsx', index = False)

	
	
execute()