# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

功能描述：job4: 匹配（人工匹配表/prod算出的最小编辑距离行）
  * @author yzy
  * @version 0.0
  * @since 2020/08/19
  * @note 输入数据：cpa_ed （ed_total列是总编辑距离）
					human_replace
		落盘数据：
  
"""

# from phlogs.phlogs import phlogger
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
	print("程序start: job4_match")
	
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
	# cpa_ed = spark.read.parquet("s3a://ph-max-auto/2020-08-11/BPBatchDAG/cpa_ed")
	human_replace_data = spark.read.parquet("s3a://ph-stream/common/public/human_replace/0.0.14")
	# human_replace_data = spark.read.parquet("s3a://ph-stream/common/public/human_replace_new/0.0.1")
	product_data = spark.read.parquet("s3a://ph-stream/common/public/prod/0.0.14")
	
	# cpa_ed.show(5)
	# print(cpa_ed.count()) # 5851567
	# human_replace_data.show(5)
	# print(human_replace_data.count()) # 111594
	
	# 选出编辑距离最小的一行
	# ed_df = Window.partitionBy("id").orderBy(func.col("ed_total"))
	# min_df = cpa_ed.withColumn("rank", func.rank().over(ed_df)).filter("rank=1")
	# min_df.select("in_MOLE_NAME", "in_SPEC", "check_SPEC", "ed_SPEC", "id", "PACK_ID", "ed_total").show(100)
	# print(min_df.count())  # 54063
	# only_id = cpa_ed.select("id").distinct()
	# print(only_id.count())  # 36122
	
	# # 将最小编辑距离行写入 其余行舍弃
	# out_path = "s3a://ph-max-auto/2020-08-11/BPBatchDAG/cpa_min_ed"
	# min_df.write.format("parquet").mode("overwrite").save(out_path)
	# print("写入 " + out_path + " 完成")
	
	min_df = spark.read.parquet("s3a://ph-max-auto/2020-08-11/BPBatchDAG/cpa_min_ed").drop("rank")
	# min_df.show(4)
	# product_data.show(4)
	# print(min_df.count())  # 54063
	
	# 给human replace 加上pack_id
	human_replace = human_replace_data.join(product_data.select("PACK_ID", "MOLE_NAME_CH", "PROD_NAME_CH", "SPEC", "DOSAGE", "PACK", "MNF_NAME_CH"), 
								   [human_replace_data.MOLE_NAME == product_data.MOLE_NAME_CH,
								   human_replace_data.PRODUCT_NAME == product_data.PROD_NAME_CH,
								   #human_replace_data.SPEC == product_data.SPEC, ],
								   human_replace_data.DOSAGE == product_data.DOSAGE, 
								   human_replace_data.PACK_QTY == product_data.PACK, ]
								   #human_replace_data.MANUFACTURER_NAME == product_data.MNF_NAME_CH],
								   how="left")
	human_replace.show(5)
	# # print(human_replace.count()) # 111594 
	
	not_null = human_replace.filter(human_replace["PACK_ID"].isNotNull())
	not_null.show(5)
	print(not_null.count())
	
	


	print("程序end job4_match") 
	print("--"*80)
	

execute()	