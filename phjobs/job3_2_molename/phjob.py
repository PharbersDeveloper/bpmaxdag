# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

功能描述：job3_2：模糊匹配分子名
  * @author yzy
  * @version 0.0
  * @since 2020/10/14
  * @note  落盘数据：
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import *

def execute():
	"""
		please input your code below
	"""
	
	print("--"*80)
	print("程序start: job3_2_molename")
	
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
	out_path = "s3a://ph-max-auto/2020-08-11/BPBatchDAG/azsanofi_check/0.0.4"
	cpa_prod_join_null = spark.read.parquet(out_path + "/" + "cpa_prod_join_null") \
					.drop("check_MOLE_NAME_EN", "check_MOLE_NAME_CH", "check_PROD_NAME_CH", \
					      "check_SPEC", "check_DOSAGE", "check_PACK", "check_MNF_NAME_CH", "check_MNF_NAME_EN", "PACK_ID")
	# cpa_prod_join_null.show()
	# print(cpa_prod_join_null.count())
	cpa_mole_null = cpa_prod_join_null.select("in_MOLE_NAME").distinct()
	# cpa_mole_null.show()
	# print(cpa_mole_null.count())  # 94
	prod = spark.read.parquet("s3a://ph-stream/common/public/prod/0.0.16")
	# prod.show(3)
	# print(prod.count())
	prod_mole_name = prod.select("MOLE_NAME_CH").distinct().collect()
	mole_name_lst = [row.MOLE_NAME_CH for row in prod_mole_name]
	mole_name_lst = list(mole_name_lst)
	# print(len(mole_name_lst))
	# prod_mole_name.show()
	# print(prod_mole_name.count())
	
	# # 将不规范的匹配不上的mole name计算编辑距离模糊匹配
	# def edit_distance(in_value, check_value):
	# 	# 输入两个str 计算编辑距离 输出int
		
	# 	m, n = len(in_value), len(check_value)
	# 	dp = [[0 for _ in range(n + 1)] for _ in range(m + 1)]
	# 	for i in range(m + 1):
	# 		dp[i][0] = i
	# 	for j in range(n + 1):
	# 		dp[0][j] = j
	# 	for i in range(1, m + 1):
	# 		for j in range(1, n + 1):
	# 			dp[i][j] = min(dp[i - 1][j - 1] + (0 if in_value[i - 1] == check_value[j - 1] else 1),
	# 						   dp[i - 1][j] + 1,
	# 						   dp[i][j - 1] + 1,
	# 						   )
	# 	return dp[m][n]
	
	# @func.udf(returnType=StringType())
	# def mole_rename(cpa_mole_name):
	# 	ed_lst = [100]
	# 	for prod_mole_name in mole_name_lst:
	# 		ed = edit_distance(cpa_mole_name, prod_mole_name)
	# 		if ed < min(ed_lst):
	# 			ed_lst.append(ed)
	# 			return_name = prod_mole_name
	# 	print(return_name)
	# 	return return_name
	
	# cpa_mole_null_replace = cpa_mole_null.withColumn("new_MOLE_NAME", mole_rename("in_MOLE_NAME"))
	# # cpa_mole_null_replace.show(100)
	# print(cpa_mole_null_replace.count())

	# # 写入：
	# cpa_mole_null_replace.write.format("parquet").mode("overwrite").save(out_path + "/" + "cpa_mole_null_replace")
	# print("写入 " + out_path + "/" + "cpa_mole_null_replace" + " 完成")
	
	# 将模糊匹配完成的数据再join，然后union得到cpa_prod_join
	cpa_mole_null_replace = spark.read.parquet("s3a://ph-max-auto/2020-08-11/BPBatchDAG/azsanofi_check/0.0.4/cpa_mole_null_replace")
	cpa_mole_null_replace = cpa_prod_join_null.join(cpa_mole_null_replace, "in_MOLE_NAME", how="left")
	# cpa_mole_null_replace.show()
	# print(cpa_mole_null_replace.count())
	
	azsanofi_456 = cpa_mole_null_replace.withColumn("in_MOLE_NAME", cpa_mole_null_replace.new_mole_name).drop("new_mole_name", "id") \
								.withColumnRenamed("in_MOLE_NAME", "MOLE_NAME") \
								.withColumnRenamed("in_PRODUCT_NAME", "PRODUCT_NAME") \
								.withColumnRenamed("in_SPEC", "SPEC") \
								.withColumnRenamed("in_DOSAGE", "DOSAGE") \
								.withColumnRenamed("in_PACK_QTY", "PACK_QTY") \
								.withColumnRenamed("in_MANUFACTURER_NAME", "MANUFACTURER_NAME")
	azsanofi_456.show()
	print(azsanofi_456.count())
	azsanofi_456.write.format("parquet").mode("overwrite").save("s3a://ph-max-auto/2020-08-11/BPBatchDAG/azsanofi_check/0.0.5/raw_data")
	
	


	print("程序end job3_2_molename")
	print("--"*80)
	
	# azsanofi = spark.read.parquet("s3a://ph-max-auto/2020-08-11/BPBatchDAG/azsanofi_check/0.0.4/cpa_distinct")
	# w = azsanofi.filter(azsanofi.PACK_ID_CHECK.isNull())
	# w.show(10)
	# print(w.count())
	
execute()
