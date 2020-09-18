# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

功能描述：job5: 匹配（人工匹配表+prod算出的最小编辑距离行）
  * @author yzy
  * @version 0.0
  * @since 2020/08/19
  * @note 落盘数据：cpa_match
  
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

def execute(out_path, in_prod_path):
	"""
		please input your code below
	"""
	
	print("--"*80)
	print("程序start: job5_match")
	
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
  
	# 需要的所有四个表格命名
	cpa_ed = spark.read.parquet(out_path + "/" + "cpa_ed")
	product_data = spark.read.parquet(in_prod_path)
	cpa_join_hr = spark.read.parquet(out_path + "/" + "cpa_hr_done") \
						.withColumnRenamed("MOLE_NAME", "in_MOLE_NAME") \
						.withColumnRenamed("PRODUCT_NAME", "in_PRODUCT_NAME") \
						.withColumnRenamed("SPEC", "in_SPEC") \
						.withColumnRenamed("DOSAGE", "in_DOSAGE") \
						.withColumnRenamed("PACK_QTY", "in_PACK_QTY") \
						.withColumnRenamed("MANUFACTURER_NAME", "in_MANUFACTURER_NAME") 
	
	# cpa_ed.show(5)
	# print(cpa_ed.count()) # 1181917
	
	# 选出编辑距离最小的一行
	ed_df = Window.partitionBy("id").orderBy(func.col("ed_total"), func.col("ed_PACK"), func.col("ed_SPEC"), func.col("ed_PROD_NAME_CH"), func.col("ed_MNF_NAME_CH"), \
											func.col("ed_DOSAGE"), func.col("ed_MNF_NAME_EN"), func.col("PACK_ID"))
	min_df = cpa_ed.withColumn("rank", func.rank().over(ed_df)).filter("rank=1")
	# min_df.select("in_MOLE_NAME", "in_SPEC", "check_SPEC", "ed_SPEC", "id", "PACK_ID", "ed_total").show(100)
	# min_df.show(10)
	# print(min_df.count())  # 8168

	cpa_join_ed = 	min_df.withColumn("mark", func.lit("ed")) \
					   .withColumnRenamed("check_MOLE_NAME_CH", "match_MOLE_NAME_CH") \
					   .withColumnRenamed("check_MOLE_NAME_EN", "match_MOLE_NAME_EN") \
					   .withColumnRenamed("check_PROD_NAME_CH", "match_PRODUCT_NAME") \
					   .withColumnRenamed("check_SPEC", "match_SPEC") \
					   .withColumnRenamed("check_DOSAGE", "match_DOSAGE") \
					   .withColumnRenamed("check_PACK", "match_PACK_QTY") \
				   	   .withColumnRenamed("check_MNF_NAME_CH", "match_MANUFACTURER_NAME_CH") \
				   	   .withColumnRenamed("check_MNF_NAME_EN", "match_MANUFACTURER_NAME_EN") \
				   	   .drop("rank")

	# cpa_join_ed.show(2)
	# print(cpa_join_ed.count())  # 8168
	# 写入 cpa_join_ed
	cpa_join_ed.write.format("parquet").mode("overwrite").save(out_path + "/" + "cpa_join_ed")
	print("写入 " + out_path + "/" + "cpa_join_ed" + " 完成")
	
	# cpa_join_hr.show(3)

	# 把人工匹配和ed匹配两张表union起来
	cpa_match = cpa_join_ed.unionByName(cpa_join_hr)
	# cpa_match.show(5)
	# print(cpa_match.count())  # 17230
	
	
	# 写入cpa_match
	out_path = out_path + "/" + "cpa_match"
	cpa_match.write.format("parquet").mode("overwrite").save(out_path)
	print("写入 " + out_path + "/" + "cpa_match" + " 完成")

	print("程序end job5_match")
	print("--"*80)
	
