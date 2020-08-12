# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

功能描述：job1: 替换不规范的mole_name，对cpa_input数据去重并生成唯一id
  * @author yzy
  * @version 0.0
  * @since 2020/08/11
  * @note
  
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as func

def execute():
	"""
		please input your code below
	"""
	
	print("--"*80)
	print("程序start: job2_join")
	
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
	cpa_distinct_data = spark.read.parquet("s3a://ph-max-auto/2020-08-11/BPBatchDAG/cpa_distinct")
	prod_data_path = "s3a://ph-stream/common/public/prod/0.0."
	product_data = spark.read.parquet("s3a://ph-stream/common/public/prod/0.0.14")

	
	prod_min_key_lst = ["MOLE_NAME_EN", "MOLE_NAME_CH", "PROD_NAME_CH", "SPEC", "DOSAGE", "PACK", "MNF_NAME_CH", "MNF_NAME_EN", "PACK_ID"]
	product_check_data = product_data.select(prod_min_key_lst)
	# cpa_distinct_data.show(10)  # 共31475条数据
	# product_check_data.show(10)
	
	# 给input cpa 和 prod 列明前分别加in_ 和 check_
	cpa_renamed = cpa_distinct_data
	for col in cpa_renamed.columns:
		cpa_renamed = cpa_renamed.withColumnRenamed(col, "in_" + col)
	cpa_renamed = cpa_renamed.withColumnRenamed("in_id", "id")
	cpa_renamed.show(3)
	
	prod_renamed = product_check_data
	for col in prod_renamed.columns:
		prod_renamed = prod_renamed.withColumnRenamed(col, "check_" + col)
	prod_renamed = prod_renamed.withColumnRenamed("check_PACK_ID", "PACK_ID")	
	
	prod_renamed.show(3)
	
	cpa_prod_join_data = cpa_renamed.join(prod_renamed,
								   cpa_renamed.in_MOLE_NAME == prod_renamed.check_MOLE_NAME_CH,
								   how="left")
	cpa_prod_join_data.show(100)
	print(cpa_prod_join_data.count())  # 3347778
	print(cpa_renamed.count())  # 31475
	print(cpa_distinct_data.count())  # 31475
	print(product_check_data.count())  # 41030
	
	
	# cpa_prod_join_data.createOrReplaceTempView("cpa_prod_join_data") 
	# cpa_prod_join_data = spark.sql("select check_MOLE_NAME_CH where check_MOLE_NAME_EN is not null")
	# cpa_prod_join_data.show(30)

	print("程序end job2_join")
	print("--"*80)
	

execute()	