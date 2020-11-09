# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as func
import re
import numpy as np
import jieba
import jieba.posseg as pseg
import jieba.analyse

def execute(a):
	"""
		please input your code below
	"""
	print("--"*80)
	print("程序start: job_mnf")
	
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
	
	
	# prod_in = spark.read.parquet("s3a://ph-stream/common/public/prod/0.0.15")
	# mnf_name_replace = spark.read.parquet("s3a://ph-max-auto/2020-08-11/BPBatchDAG/mnf_name_mapping/mnf_name_mapping")
	# # prod_in.show()
	# print(prod_in.count())
	
	
	def prod_mnf_replace():
		prod_out = prod_in.join(mnf_name_replace, \
								prod_in.MNF_NAME_CH == mnf_name_replace.FORMER_MNF_NAME, \
								how="left") \
								.withColumn("MNF_NAME_CH", func.when((mnf_name_replace.FORMER_MNF_NAME == "") | (mnf_name_replace.FORMER_MNF_NAME.isNull()), prod_in.MNF_NAME_CH). \
															  otherwise(mnf_name_replace.MNF_NAME)) \
								 .drop("MNF_NAME", "FORMER_MNF_NAME")
		# mnf_replace = prod_out.filter(prod_out.FORMER_MNF_NAME.isNotNull())
		# print(mnf_replace.count())
		out_path = "s3a://ph-stream/common/public/prod/0.0.16"
		prod_out.write.format("parquet").mode("overwrite").save(out_path)
		print("写入 " + out_path + "完成")
	
	def edit_distance(in_value, check_value):
		# 输入两个str 计算编辑距离 输出int
		
		m, n = len(in_value), len(check_value)
		dp = [[0 for _ in range(n + 1)] for _ in range(m + 1)]
		for i in range(m + 1):
			dp[i][0] = i
		for j in range(n + 1):
			dp[0][j] = j
		for i in range(1, m + 1):
			for j in range(1, n + 1):
				dp[i][j] = min(dp[i - 1][j - 1] + (0 if in_value[i - 1] == check_value[j - 1] else 1),
							   dp[i - 1][j] + 1,
							   dp[i][j - 1] + 1,
							   )
		return dp[m][n]
		
		
	@func.udf(returnType=StringType())		
	def fenci(in_value, check_value):
		in_words = pseg.cut(in_value)
		in_str_geo = ""
		in_str_core = ""
		in_str_name = ""
		for word, flag in in_words:
		    if flag == "ns":
		        in_str_geo += word
		    elif flag == "nr":
		        in_str_core += word
		    else:
		        in_str_name += word
		        
		check_words = pseg.cut(check_value)
		check_str_geo = ""
		check_str_core = ""
		check_str_name = ""
		for word, flag in check_words:
		    if flag == "ns":
		        check_str_geo += word
		    elif flag == "nr":
		        check_str_core += word
		    else:
		        check_str_name += word        
		print(in_str_geo)
		print(check_str_geo)   
		print(in_str_core)   
		print(check_str_core)   
		print(in_str_name)   
		print(check_str_name)   
		print(edit_distance(in_str_geo, check_str_geo))
		print(edit_distance(in_str_core, check_str_core))
		print(edit_distance(in_str_name, check_str_name))
		ed = 5*edit_distance(in_str_geo, check_str_geo) + 10*edit_distance(in_str_core, check_str_core) + edit_distance(in_str_name, check_str_name)
		print(ed)
		
		return ed
	# prod_mnf_replace()
	
	cpa_check = spark.read.parquet("s3a://ph-stream/common/public/pfizer_check")
	cpa_check = cpa_check.withColumn("check", fenci(cpa_check.MANUFACTURER_NAME, cpa_check.MOLE_NAME))
	cpa_check.show()
	
	# fenci()
	
	print("程序end job_nmf")
	print("--"*80)

execute(1)