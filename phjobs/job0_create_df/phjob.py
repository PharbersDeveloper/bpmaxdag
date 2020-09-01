# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

功能描述：job0: 各种表的生成
  * @author yzy
  * @version 0.0
  * @since 2020/08/19
  * @note 输入数据：cpa_ed （ed_total列是总编辑距离）
					human_replace
					product_data
		落盘数据：cpa_match
  
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
	print("程序start: job0_create_df")
	
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
	
		
	def create_hrpackid_df():
		# create human_replace_packid 
		prod = spark.read.parquet("s3a://ph-stream/common/public/prod/0.0.15") \
						 .select("PACK_ID", "MOLE_NAME_CH", "PROD_NAME_CH", "SPEC", "DOSAGE", "PACK", "MNF_NAME_CH") \
						 .withColumnRenamed("SPEC", "SPEC_prod") \
						 .withColumnRenamed("DOSAGE", "DOSAGE_prod")
		prod.show(5)
		human_replace = spark.read.parquet("s3a://ph-stream/common/public/human_replace/0.0.14")
								  
		human_replace.show(10)
		# print(human_replace.count())
		
		human_replace_packid = human_replace.join(prod,
											 [human_replace.MOLE_NAME == prod.MOLE_NAME_CH, \
											  human_replace.PRODUCT_NAME == prod.PROD_NAME_CH, \
											  human_replace.SPEC == prod.SPEC_prod, \
											  human_replace.PACK_QTY == prod.PACK, \
											  human_replace.MANUFACTURER_NAME == prod.MNF_NAME_CH, \
											  human_replace.DOSAGE == prod.DOSAGE_prod], \
											  how="left") \
											 .drop("version", "MOLE_NAME_CH", "PROD_NAME_CH", "SPEC_prod", "DOSAGE_prod", "PACK", "MNF_NAME_CH")
		# human_replace_packid.show(10)
		# print(human_replace_packid.count())
		
		human_replace_packid = human_replace_packid.filter(human_replace_packid["PACK_ID"].isNotNull()) \
													.withColumnRenamed("MOLE_NAME", "match_MOLE_NAME_CH") \
													.withColumnRenamed("PRODUCT_NAME", "match_PRODUCT_NAME") \
													.withColumnRenamed("SPEC", "match_SPEC") \
													.withColumnRenamed("DOSAGE", "match_DOSAGE") \
													.withColumnRenamed("PACK_QTY", "match_PACK_QTY") \
													.withColumnRenamed("MANUFACTURER_NAME", "match_MANUFACTURER_NAME_CH")
			
		human_replace_packid.show()
		# 写入
		# out_path = "s3a://ph-max-auto/2020-08-11/BPBatchDAG/human_replace_packid"
		# human_replace_packid.write.format("parquet").mode("overwrite").save(out_path)
		# print("写入 " + out_path + " 完成")
		


	def create_prod_check():
		# 选择prod表中的有用列并重命名列名
		prod_min_key_lst = ["MOLE_NAME_EN", "MOLE_NAME_CH", "PROD_NAME_CH", "SPEC", "DOSAGE", "PACK", "MNF_NAME_CH", "MNF_NAME_EN", "PACK_ID"]
		product_data = spark.read.parquet("s3a://ph-stream/common/public/prod/0.0.15").select(prod_min_key_lst)
		product_data.show(3)
		
		prod_renamed = product_data
		for col in prod_renamed.columns:
			prod_renamed = prod_renamed.withColumnRenamed(col, "check_" + col)
		prod_renamed = prod_renamed.withColumnRenamed("check_PACK_ID", "PACK_ID")	
		prod_renamed.show(3)
		
		# 写入
		# out_path = "s3a://ph-max-auto/2020-08-11/BPBatchDAG/check_prod_renamed"
		# prod_renamed.write.format("parquet").mode("overwrite").save(out_path)
		# print("写入 " + out_path + " 完成")
		


execute()