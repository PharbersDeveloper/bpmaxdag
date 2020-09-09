# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

功能描述：job6: 匹配回原cpa数据df并删除无用列,生成最终df：原cpa数据+pack_id
  * @author yzy
  * @version 0.0
  * @since 2020/09/07
  * @note 落盘数据：cpa_match_result
  
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

def execute(out_path, cpa_raw_data_path):
	"""
		please input your code below
	"""
	
	print("--"*80)
	print("程序start: job6_joinback")
	
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
	# out_path = "s3a://ph-max-auto/2020-08-11/BPBatchDAG/pfi_check/0.0.4"
	cpa_match = spark.read.parquet(out_path + "/" + "cpa_match") \
							.select("in_MOLE_NAME", "in_PRODUCT_NAME", "in_SPEC", "in_DOSAGE", "in_PACK_QTY", "in_MANUFACTURER_NAME", "PACK_ID")
	cpa_raw_data = spark.read.parquet(cpa_raw_data_path)
	
	cpa_match_result = cpa_raw_data.join(cpa_match, \
								 [cpa_match.in_MOLE_NAME == cpa_raw_data.MOLE_NAME, \
								 cpa_match.in_PRODUCT_NAME == cpa_raw_data.PRODUCT_NAME, \
								 cpa_match.in_SPEC == cpa_raw_data.SPEC, \
								 cpa_match.in_DOSAGE == cpa_raw_data.DOSAGE, \
								 cpa_match.in_PACK_QTY == cpa_raw_data.PACK_QTY, \
								 cpa_match.in_MANUFACTURER_NAME == cpa_raw_data.MANUFACTURER_NAME], \
							      how="left") \
							      .drop("in_MOLE_NAME", "in_PRODUCT_NAME", "in_SPEC", "in_DOSAGE", "in_PACK_QTY", "in_MANUFACTURER_NAME")
	cpa_match_result.show(3)
	print(cpa_match_result.count())
	
	# 写入
	out_path = out_path + "/" + "cpa_match_result"
	cpa_match_result.write.format("parquet").mode("overwrite").save(out_path)
	print("写入 " + out_path + " 完成")
	
	
	print("程序end job6_joinback")
	print("--"*80)
	