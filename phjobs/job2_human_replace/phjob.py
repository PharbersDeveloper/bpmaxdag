# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

功能描述：job2：用人工匹配表优先匹配cpa_distinct_data
  * @author yzy
  * @version 0.0
  * @since 2020/08/31
  * @note 落盘数据：
		1. cpa_hr_done 已经用人工匹配表匹配完的
		2. cpa_to_ed 需要计算编辑距离的
  
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as func

def execute(out_path, in_hrpackid_path):
	"""
		please input your code below
	"""
	
	print("--"*80)
	print("程序start: job2_human_replace")
	
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
	cpa_distinct_data = spark.read.parquet(out_path + "/" + "cpa_distinct")
	human_replace_packid = spark.read.parquet(in_hrpackid_path)
	
	# 加上cpa_min这一列
	cpa_distinct_data = cpa_distinct_data.withColumn("cpa_min", \
										func.concat(cpa_distinct_data["MOLE_NAME"], cpa_distinct_data["PRODUCT_NAME"], cpa_distinct_data["SPEC"], \
										cpa_distinct_data["DOSAGE"], cpa_distinct_data["PACK_QTY"], cpa_distinct_data["MANUFACTURER_NAME"]))
	# cpa_distinct_data.show(5) 

	# print(cpa_distinct_data.count()) # 17230
	cpa_join = cpa_distinct_data.join(human_replace_packid, \
							    cpa_distinct_data.cpa_min == human_replace_packid.min, \
							    how="left") \
						   .drop("cpa_min", "version", "min") \
						   .withColumn("mark", func.lit("hr"))
							   
	# cpa_join.show(10)
	# print(cpa_join.count()) # 17230
	cpa_join_null = cpa_join.filter(cpa_join["PACK_ID"].isNull()) \
							.drop("match_MOLE_NAME", "match_PRODUCT_NAME", "match_SPEC", "match_DOSAGE",\
							"match_PACK_QTY", "match_MANUFACTURER_NAME", "mark", "match_MOLE_NAME_CH", "match_MANUFACTURER_NAME_CH", "PACK_ID")
							
							
	cpa_join_null.show(5)
	print("需要进行编辑距离计算的：")
	print(cpa_join_null.count())  # 8168
	
	# 把待进行编辑距离计算的写入
	cpa_join_null.write.format("parquet").mode("overwrite").save(out_path + "/" + "cpa_to_ed")
	print("写入 " + out_path + "/" + "cpa_to_ed" + " 完成")
	
	cpa_join_hr = cpa_join.filter(cpa_join["PACK_ID"].isNotNull()) \
					  .withColumn("match_MOLE_NAME_EN", func.lit("")) \
					  .withColumn("match_MANUFACTURER_NAME_EN", func.lit("")) \
					  .withColumn("ed_SPEC", func.lit("")) \
					  .withColumn("ed_PROD_NAME_CH", func.lit("")) \
					  .withColumn("ed_MNF_NAME_CH", func.lit("")) \
					  .withColumn("ed_DOSAGE", func.lit("")) \
					  .withColumn("ed_MNF_NAME_EN", func.lit("")) \
					  .withColumn("ed_PACK", func.lit("")) \
					  .withColumn("ed_total", func.lit("")) \
					  .withColumn("ed_MOLE_NAME_CH", func.lit("")) 
	# cpa_join_hr.show(5)
	# print(cpa_join_hr.count())  # 9062
	
	# 把已经用人工匹配表匹配完pack_id的写入
	cpa_join_hr.write.format("parquet").mode("overwrite").save(out_path + "/" + "cpa_hr_done")
	print("写入 " + out_path + "/" + "cpa_hr_done" + " 完成")
	
	print("程序end job2_human_replace")
	print("--"*80)