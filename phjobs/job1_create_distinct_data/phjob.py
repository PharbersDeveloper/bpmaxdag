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
	
	print("--"*60)
	print("程序start: job1_create_distinct_data")
	
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
  
	# 需要的所有四个表格命名
	cpa_input_data = spark.read.parquet("s3a://ph-stream/common/public/cpa/0.0.1")
	# product_data = spark.read.parquet("s3a://ph-stream/common/public/prod/0.0.14")
	# human_replace_data = spark.read.parquet("s3a://ph-stream/common/public/human_replace/0.0.14")
	mole_human_replace_data = spark.read.parquet("s3a://ph-stream/common/public/mole_human_replace/0.0.1")

	# cpa_input_data.show(4)
	# product_data.show(4)
	# human_replace_data.show(4)
	# mole_human_replace_data.show(4)
	
	# 选取 mole_human_replace_data 的有用列、有用行
	mole_human_replace_data.createOrReplaceTempView("mole_human_replace_data") 
	mole_human_replace_data = spark.sql("select MOLE_NAME as INPUT_MOLE,  \
										 PROD_MOLE_NAME as PROD_MOLE \
										 from mole_human_replace_data where PROD_MOLE_NAME is not null \
										 and PROD_MOLE_NAME != '#N/A' and PROD_MOLE_NAME != ''")
	mole_human_replace_data.show(4)  # 共156条数据
	
	#  规范mole_name
	# （如果mole_human_replace_data 里有该mole_name的标准化写法（PROD_MOLE），则用PROD_MOLE替换，若没有则保留原来的mole_name）
	all_cpa_data = cpa_input_data.join(mole_human_replace_data,
									  cpa_input_data.MOLE_NAME == mole_human_replace_data.INPUT_MOLE,
									  how="left") \
								 .withColumn("MOLE_NAME", func.when((mole_human_replace_data.INPUT_MOLE == "") | (mole_human_replace_data.INPUT_MOLE.isNull()), cpa_input_data.MOLE_NAME). \
															  otherwise(mole_human_replace_data.PROD_MOLE)) \
								 .drop("INPUT_MOLE", "PROD_MOLE")
	
	# all_cpa_data.select("MOLE_NAME", "PRODUCT_NAME", "SPEC", "DOSAGE", "PACK_QTY", "MANUFACTURER_NAME").show(10)  # 共23405221条数据
	
	# select 6个产品字段并去重 + id
	min_keys_lst = ["MOLE_NAME", "PRODUCT_NAME", "SPEC", "DOSAGE", "PACK_QTY", "MANUFACTURER_NAME"]  
	cpa_distinct_data = all_cpa_data.select(min_keys_lst).distinct() \
									.withColumn("id", func.monotonically_increasing_id())
	cpa_distinct_data.show(100)  # 共31475条数据
	
	# out_path = "s3a://ph-max-auto/2020-08-11/BPBatchDAG/cpa_distinct"
	# cpa_distinct_data.write.format("parquet").mode("overwrite").save(out_path)
	
	print("程序end job1_create_distinct_data")
	print("--"*80)
	
execute()	