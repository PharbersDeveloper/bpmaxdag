# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

功能描述：job3：left join cpa和prod
  * @author yzy
  * @version 0.0
  * @since 2020/08/12
  * @note  落盘数据：cpa_prod_join
  
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as func

def execute(in_prod_path, in_hr_path, out_path, pk, mnf_name_mapping):
	"""
		please input your code below
	"""
	
	print("--"*80)
	print("程序start: job3_join")
	
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
	cpa_to_ed = spark.read.parquet(out_path + "/" + "cpa_to_ed")
	print("cpa_to_ed: " + str(cpa_to_ed.count()))
	prod_min_key_lst = ["MOLE_NAME_EN", "MOLE_NAME_CH", "PROD_NAME_CH", "SPEC", "DOSAGE", "PACK", "MNF_NAME_CH", "MNF_NAME_EN", "PACK_ID"]
	product_data = spark.read.parquet(in_prod_path).select(prod_min_key_lst)
	
	# 产品表替换生产厂商曾用名和子公司
	mnf_rename = spark.read.parquet(mnf_name_mapping)
	product_data = product_data.join(mnf_rename, mnf_rename.FORMER_MNF_NAME == product_data.MNF_NAME_CH, how="left")
	product_data = product_data.withColumn("MNF_NAME_CH", func.when((product_data.MNF_NAME == "") | (product_data.MNF_NAME.isNull()), product_data.MNF_NAME_CH). \
															  otherwise(product_data.MNF_NAME)) \
								 .drop("MNF_NAME", "FORMER_MNF_NAME")
	
	# 给input cpa列名前加in_ 
	cpa_renamed = cpa_to_ed
	for col in cpa_renamed.columns:
		cpa_renamed = cpa_renamed.withColumnRenamed(col, "in_" + col)
	cpa_renamed = cpa_renamed.withColumnRenamed("in_id", "id")

	prod_renamed = product_data
	for col in prod_renamed.columns:
		prod_renamed = prod_renamed.withColumnRenamed(col, "check_" + col)
	prod_renamed = prod_renamed.withColumnRenamed("check_PACK_ID", "PACK_ID")
	

	# left join 并把null值改成空字符串
	print(pk)
	if pk == "MOLE_NAME" :
		pk_cpa = "in_" + pk
		pk_prod = "check_" + pk + "_CH"
	elif pk == "MANUFACTURER_NAME":
		pk_cpa = "in_" + pk
		pk_prod = "check_MNF_NAME_CH"
	elif pk == "PRODUCT_NAME":
		pk_cpa = "in_" + pk
		pk_prod = "check_PROD_NAME_CH"
	else:
		pk_cpa = "in_" + pk
		pk_prod = "check_" + pk
	cpa_prod_join_data = cpa_renamed.join(prod_renamed,
								   cpa_renamed[pk_cpa] == prod_renamed[pk_prod],
								   how="left").na.fill("")
	cpa_prod_join_null = cpa_prod_join_data.filter(cpa_prod_join_data.PACK_ID == "")
	print("PK = " + str(pk))
	print("null:")
	print(cpa_prod_join_null.count())
	
	#  写入
	if cpa_prod_join_null.count() != 0:
		cpa_prod_join_null.write.format("parquet").mode("overwrite").save(out_path + "/" + "cpa_prod_join_null_" + pk)
		print("写入 " + out_path + "/" + "cpa_prod_join_null_" + pk + " 完成")
	
	cpa_prod_join_data = cpa_prod_join_data.filter(cpa_prod_join_data.PACK_ID != "")
	# cpa_prod_join_data.show(5)
	# print(cpa_prod_join_data.count())
	
	# 写入
	out_path = out_path + "/" + "cpa_prod_join"
	cpa_prod_join_data.write.format("parquet").mode("overwrite").save(out_path)
	print("写入 " + out_path + " 完成")

	print("程序end job3_join")
	print("--"*80)
	
