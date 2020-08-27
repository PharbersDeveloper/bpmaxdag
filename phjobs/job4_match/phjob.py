# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

功能描述：job4: 匹配（人工匹配表/prod算出的最小编辑距离行）
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
	print("程序start: job4_match")
	
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
	out_path = "s3a://ph-max-auto/2020-08-11/BPBatchDAG/pfizer_check"
	cpa_ed = spark.read.parquet(out_path + "/" + "cpa_ed")
	human_replace_data = spark.read.parquet("s3a://ph-stream/common/public/human_replace/0.0.14")
	# human_replace_data = spark.read.parquet("s3a://ph-stream/common/public/human_replace_new/0.0.1")
	product_data = spark.read.parquet("s3a://ph-stream/common/public/prod/0.0.14")
	
	# cpa_ed.show(5)
	# # print(cpa_ed.count()) # 5851567
	# # human_replace_data.show(5)
	# # print(human_replace_data.count()) # 111594
	
	# # 选出编辑距离最小的一行
	# ed_df = Window.partitionBy("id").orderBy(func.col("ed_total"), func.col("ed_PACK"), func.col("ed_SPEC"), func.col("ed_PROD_NAME_CH"), func.col("ed_MNF_NAME_CH"), \
	# 										func.col("ed_DOSAGE"), func.col("ed_MNF_NAME_EN"), func.col("PACK_ID"))
	# min_df = cpa_ed.withColumn("rank", func.rank().over(ed_df)).filter("rank=1")
	# # min_df.select("in_MOLE_NAME", "in_SPEC", "check_SPEC", "ed_SPEC", "id", "PACK_ID", "ed_total").show(100)
	# min_df.show(100)
	# print(min_df.count())  # 54063

	
	# # # 将最小编辑距离行写入 其余行舍弃
	# out_path = "s3a://ph-max-auto/2020-08-11/BPBatchDAG/pfizer_check/cpa_min_ed"
	# min_df.write.format("parquet").mode("overwrite").save(out_path)
	# print("写入 " + out_path + " 完成")
	
	# cpa_min_ed = spark.read.parquet("s3a://ph-max-auto/2020-08-11/BPBatchDAG/pfizer_check/cpa_min_ed").drop("rank")
	# cpa_min_ed.show(4)
	# product_data.show(4)
	# print(cpa_min_ed.count())  # 54063
	
	# 给human replace 加上pack_id
	
	# @func.udf(returnType=StringType())
	# def change_pack(in_value):
	# 	return in_value.replace(".0", "")
		
	# # human_replace_data = spark.sql("select * from human_replace")
	# print(human_replace_data.count())
	# # human_replace_data.show(5)
	# prod = product_data.select("PACK_ID", "MOLE_NAME_CH", "PROD_NAME_CH", "SPEC", "DOSAGE", "PACK", "MNF_NAME_CH") \
	# 				   .withColumnRenamed("SPEC", "SPEC_prod").withColumnRenamed("DOSAGE", "DOSAGE_prod") \
	# 				   .withColumn("PACK", change_pack("PACK"))
	# prod.show(5)
	
	# human_replace = human_replace_data.join(prod, 
	# 							   [human_replace_data.MOLE_NAME == prod.MOLE_NAME_CH,
	# 							   human_replace_data.PRODUCT_NAME == prod.PROD_NAME_CH,
	# 							   human_replace_data.SPEC == prod.SPEC_prod,
	# 							   human_replace_data.DOSAGE == prod.DOSAGE_prod, 
	# 							   human_replace_data.PACK_QTY == prod.PACK,
	# 							   human_replace_data.MANUFACTURER_NAME == prod.MNF_NAME_CH],
	# 							   how="left").drop("MOLE_NAME_CH", "PROD_NAME_CH", "SPEC_prod", "DOSAGE_prod", "PACK", "MNF_NAME_CH" )
	# human_replace.show(5)
	# print(human_replace.count()) # 111594 
	
	# not_null = human_replace.filter(human_replace["PACK_ID"].isNotNull())
	# not_null.show(5)
	# print(not_null.count())  # 95370
	
	
	# # 写入human_replace + pack_id
	# out_path = "s3a://ph-max-auto/2020-08-11/BPBatchDAG/pfizer_check/human_replace_packid"
	# not_null.write.format("parquet").mode("overwrite").save(out_path)
	# print("写入 " + out_path + " 完成")
	
	# # human_replace_packid
	human_replace_packid = spark.read.parquet("s3a://ph-max-auto/2020-08-11/BPBatchDAG/pfizer_check/human_replace_packid")
	# human_replace_packid.show(100)
	# print(human_replace_packid.count())
	# print(human_replace_packid.select("PACK_ID").distinct().count())
	human_replace_packid = human_replace_packid.withColumnRenamed("MOLE_NAME", "match_MOLE_NAME_CH") \
												.withColumnRenamed("PRODUCT_NAME", "match_PRODUCT_NAME") \
												.withColumnRenamed("SPEC", "match_SPEC") \
												.withColumnRenamed("DOSAGE", "match_DOSAGE") \
												.withColumnRenamed("PACK_QTY", "match_PACK_QTY") \
												.withColumnRenamed("MANUFACTURER_NAME", "match_MANUFACTURER_NAME_CH")
												
	# 把cpa_distinct_data 先匹配人工匹配表										
	cpa_distinct_data = spark.read.parquet("s3a://ph-max-auto/2020-08-11/BPBatchDAG/pfizer_check/cpa_distinct")
	# # human_replace_packid.select("min").show(5)
	# # print(human_replace_packid.rdd.collect())
	# # print(human_replace_packid.count()) # 95370


	cpa_distinct_data = cpa_distinct_data.withColumn("cpa_min", \
										func.concat(cpa_distinct_data["MOLE_NAME"], cpa_distinct_data["PRODUCT_NAME"], cpa_distinct_data["SPEC"], \
										cpa_distinct_data["DOSAGE"], cpa_distinct_data["PACK_QTY"], cpa_distinct_data["MANUFACTURER_NAME"]))
	# cpa_distinct_data.show(5)  
	
	
	print(cpa_distinct_data.count()) # 36122
	cpa_join = cpa_distinct_data.join(human_replace_packid, \
									  cpa_distinct_data.cpa_min == human_replace_packid.min, \
									  how="left") \
							    .drop("cpa_min", "version", "min") \
							    .withColumn("mark", func.lit("human_replace"))
							   
	cpa_join.show(10)
	print(cpa_join.count()) # 46860

	
	# cpa_join_null = cpa_join.filter(cpa_join["PACK_ID"].isNull()) \
	# 						.drop("match_MOLE_NAME", "match_PRODUCT_NAME", "match_SPEC", "match_DOSAGE",\
	# 						"match_PACK_QTY", "match_MANUFACTURER_NAME", "mark", "match_MOLE_NAME_CH", "match_MANUFACTURER_NAME_CH", "PACK_ID")
							
							
	# # cpa_join_null.show(5)
	# # # print(cpa_join_null.count())  # 22215

	# cpa_join_ed = cpa_join_null.join(cpa_min_ed.drop("in_MOLE_NAME", "in_PRODUCT_NAME", "in_SPEC", "in_DOSAGE", "in_PACK_QTY", "in_MANUFACTURER_NAME" ), \
	# 								 "id", \
	# 								 how="left") \
	# 						   .withColumn("mark", func.lit("ed")) \
	# 						   .withColumnRenamed("check_MOLE_NAME_CH", "match_MOLE_NAME_CH") \
	# 						   .withColumnRenamed("check_MOLE_NAME_EN", "match_MOLE_NAME_EN") \
	# 						   .withColumnRenamed("check_PROD_NAME_CH", "match_PRODUCT_NAME") \
	# 						   .withColumnRenamed("check_SPEC", "match_SPEC") \
	# 						   .withColumnRenamed("check_DOSAGE", "match_DOSAGE") \
	# 						   .withColumnRenamed("check_PACK", "match_PACK_QTY") \
	# 					   	   .withColumnRenamed("check_MNF_NAME_CH", "match_MANUFACTURER_NAME_CH") \
	# 					   	   .withColumnRenamed("check_MNF_NAME_EN", "match_MANUFACTURER_NAME_EN") \

	# cpa_join_ed.show(5)
	

	
	# cpa_join_hr = cpa_join.filter(cpa_join["PACK_ID"].isNotNull()) \
	# 					  .withColumn("match_MOLE_NAME_EN", func.lit(None)) \
	# 					  .withColumn("match_MANUFACTURER_NAME_EN", func.lit(None)) \
	# 					  .withColumn("ed_SPEC", func.lit(None)) \
	# 					  .withColumn("ed_PROD_NAME_CH", func.lit(None)) \
	# 					  .withColumn("ed_MNF_NAME_CH", func.lit(None)) \
	# 					  .withColumn("ed_DOSAGE", func.lit(None)) \
	# 					  .withColumn("ed_MNF_NAME_EN", func.lit(None)) \
	# 					  .withColumn("ed_PACK", func.lit(None)) \
	# 					  .withColumn("ed_total", func.lit(None)) 
	# cpa_join_hr.show(5)
	# # print(cpa_join_hr.count())  # 24645
	
	# cpa_match = cpa_join_ed.union(cpa_join_hr)
	# cpa_match.show(5)
	# # print(cpa_match.count())  # 57099
	
	
	# # 写入cpa_match
	# out_path = "s3a://ph-max-auto/2020-08-11/BPBatchDAG/cpa_match"
	# cpa_match.write.format("parquet").mode("overwrite").save(out_path)
	# print("写入 " + out_path + " 完成")
	



	print("程序end job4_match")
	print("--"*80)
	

execute()	