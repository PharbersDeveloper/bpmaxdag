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
import pandas as pd
from pyspark.sql.types import *

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
		
	@func.udf(returnType=StringType())
	def pack_id(in_value):
		return in_value.lstrip("0")
		
	
	# 参数配置
	out_path = "s3a://ph-max-auto/2020-08-11/BPBatchDAG/sanofi_check/0.0.1"

	# 数据匹配率检查
	print()
	print("-----开始进行 " + out_path + " 数据匹配率检查-----")
	
	# 这个是有人工匹配pack_id的测试数据, 路径不一样的比较多
	# cpa_check = spark.read.parquet("s3a://ph-stream/common/public/pfizer_check") \
	# cpa_check = spark.read.parquet("s3a://ph-max-auto/2020-08-11/BPBatchDAG/azsanofi_check") \
	cpa_check = spark.read.parquet("s3a://ph-max-auto/2020-08-11/BPBatchDAG/sanofi_check/raw_data") \
						.na.fill("") \
						.withColumn("PACK_ID_CHECK", pack_id("PACK_ID_CHECK")) \
						.drop("id")
	print("测试数据共有 " + str(cpa_check.count()) + " 条")
	
	cpa_check_valid = cpa_check.filter(cpa_check.DOSAGE != "") \
						.filter(cpa_check.PACK_ID_CHECK != "").filter(cpa_check.PACK_ID_CHECK != "NULL")
	print("有效测试数据有 " + str(cpa_check_valid.count()) + " 条")

	
	cpa_distinct = spark.read.parquet(out_path + "/cpa_distinct")
	print()
	print("job1生成的去重数据 " + str(cpa_distinct.count()) + " 条")
	
	# 这个是我生成的数据
	cpa_match = spark.read.parquet(out_path + "/cpa_match") \
								.na.fill("") \
								.withColumn("PACK_ID", pack_id("PACK_ID"))
	# cpa_match.show(5)
	print("其中最终匹配成功数据 " + str(cpa_match.count()) + " 条")
	
	cpa_prod_join_null = spark.read.parquet(out_path + "/cpa_prod_join_null").na.fill("")
	print("其中匹配不上的有 " + str(cpa_prod_join_null.count()) + " 条")
	
	print()
	print("-----开始join匹配数据和测试数据-----")

	cpa_examine = cpa_match.join(cpa_check, \
							 [cpa_match.in_MOLE_NAME == cpa_check.MOLE_NAME, \
							 cpa_match.in_PRODUCT_NAME == cpa_check.PRODUCT_NAME, \
							 cpa_match.in_SPEC == cpa_check.SPEC, \
							 cpa_match.in_DOSAGE == cpa_check.DOSAGE, \
							 cpa_match.in_PACK_QTY == cpa_check.PACK_QTY, \
							 cpa_match.in_MANUFACTURER_NAME == cpa_check.MANUFACTURER_NAME], \
							 how="left")
	cpa_examine_count = cpa_examine.count()
	print("匹配数据共有 " + str(cpa_examine_count) + " 条")

	wrong = cpa_examine.filter(cpa_examine.PACK_ID_CHECK != cpa_examine.PACK_ID)
	# wrong.select("PRODUCT_NAME", "match_PRODUCT_NAME", "ed_PROD_NAME_CH").show(30)
	wrong_count = wrong.count()
	print("共有匹配错误 " + str(wrong_count) + " 条")  # 1590
	print("匹配率 " + str(round(100*(1 - wrong_count / cpa_examine_count), 2)) + "%")  # 1590

	
	wrong_hr = wrong.filter(cpa_examine.mark == "hr")
	total_hr = spark.read.parquet(out_path + "/cpa_hr_done")
	wrong_hr_count = wrong_hr.count()
	total_hr_count = total_hr.count()
	print("其中因为人工匹配表错误匹配 " + str(wrong_hr_count) + "/" + str(total_hr_count) + " 条")  # 54

	wrong_ed = wrong.filter(cpa_examine.mark == "ed").na.fill("")
	total_ed = spark.read.parquet(out_path + "/cpa_ed")
	wrong_ed_count = wrong_ed.count()
	total_ed_count = total_ed.count()
	print("其中因为编辑距离错误匹配 " + str(wrong_ed_count) + "/" + str(total_ed_count) + " 条")  # 1536
	
	# 计算编辑距离出错的写入s3
	wrong_ed.write.format("parquet").mode("overwrite").save(out_path + "/wrong_ed")
	print("写入 " + out_path + "/wrong_ed" + " 完成")
	
	# wrong_hr.write.format("parquet").mode("overwrite").save(out_path + "/wrong_hr")
	# print("写入 " + out_path + " 完成")


execute()