# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

功能描述：job1: 替换不规范的mole_name，对cpa_input数据去重并生成唯一id
  * @author yzy
  * @version 0.0
  * @since 2020/08/11
  * @note  落盘数据：cpa_distinct_data
  
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as func

def execute(in_cpa_path, in_prod_path, in_hr_path, in_mhr_path, out_path, min_keys_lst):
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
	cpa_input_data = spark.read.parquet(in_cpa_path).drop("id")
	# print(cpa_input_data.count())
	cpa_input_data = cpa_input_data.filter(cpa_input_data.PRODUCT_NAME.isNotNull()).filter(cpa_input_data.DOSAGE.isNotNull()) \
							.filter(cpa_input_data.PACK_ID_CHECK.isNotNull()).filter(cpa_input_data.PACK_ID_CHECK !="NULL").filter(cpa_input_data.PACK_ID_CHECK !="")
	# print(cpa_input_data.count())
	mole_human_replace_data = spark.read.parquet(in_mhr_path)

	# cpa_input_data.show(4)
	# print(cpa_input_data.count())
	# mole_human_replace_data.show(4)
	
	# 选取 mole_human_replace_data 的有用列、有用行
	mole_human_replace_data.createOrReplaceTempView("mole_human_replace_data") 
	mole_human_replace_data = spark.sql("select MOLE_NAME as INPUT_MOLE,  \
										 PROD_MOLE_NAME as PROD_MOLE \
										 from mole_human_replace_data where PROD_MOLE_NAME is not null \
										 and PROD_MOLE_NAME != '#N/A' and PROD_MOLE_NAME != ''")
	# mole_human_replace_data.show(4)  # 共156条数据
	# print(mole_human_replace_data.count())
	
	#  规范mole_name
	# （如果mole_human_replace_data 里有该mole_name的标准化写法（PROD_MOLE），则用PROD_MOLE替换，若没有则保留原来的mole_name）
	all_cpa_data = cpa_input_data.join(mole_human_replace_data, \
								cpa_input_data.MOLE_NAME == mole_human_replace_data.INPUT_MOLE, \
								how="left") \
								.withColumn("MOLE_NAME", func.when((mole_human_replace_data.INPUT_MOLE == "") | (mole_human_replace_data.INPUT_MOLE.isNull()), cpa_input_data.MOLE_NAME). \
															  otherwise(mole_human_replace_data.PROD_MOLE)) \
								 .drop("INPUT_MOLE", "PROD_MOLE")
	
	# all_cpa_data.select("MOLE_NAME", "PRODUCT_NAME", "SPEC", "DOSAGE", "PACK_QTY", "MANUFACTURER_NAME").show(10)  # 原来有共23405221条数据（不是测试数据
	
	# select 6个产品字段并去重 + id
	min_keys_lst = ["MOLE_NAME", "PRODUCT_NAME", "SPEC", "DOSAGE", "PACK_QTY", "MANUFACTURER_NAME"]
	cpa_distinct_data = all_cpa_data.select(min_keys_lst).distinct() \
									.withColumn("id", func.monotonically_increasing_id())
	# cpa_distinct_data.show(10)  
	# print(cpa_distinct_data.count())
	
	# # 写入
	out_path = out_path + "/" + "cpa_distinct"
	cpa_distinct_data.write.format("parquet").mode("overwrite").save(out_path)
	print("写入 " + out_path + " 完成")
	
	print("程序end job1_create_distinct_data")
	print("--"*80)
	

