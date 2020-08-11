# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is test pyspark job for zzyin
"""

from phlogs.phlogs import phlogger
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as func

def execute():
	print("--"*80)
	print("zyyin-test 程序start")
	
	# spark = SparkSession.builder \
	# 	.master("yarn") \
	# 	.appName("data from s3") \
	# 	.config("spark.driver.memory", "1g") \
	# 	.config("spark.executor.cores", "1") \
	# 	.config("spark.executor.instance", "1") \
	# 	.config("spark.executor.memory", "1g") \
	# 	.config('spark.sql.codegen.wholeStage', False) \
	# 	.getOrCreate()

	# access_key = os.getenv("AWS_ACCESS_KEY_ID")
	# secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
	# if access_key is not None:
	# 	spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
	# 	spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
	# 	spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
	# 	spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
	# 	# spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
	# 	spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.cn-northwest-1.amazonaws.com.cn")
  
	# 需要的所有四个表格
	# cpa_input_data = spark.read.parquet("s3a://ph-stream/common/public/cpa/0.0.1")
	# product_data = spark.read.parquet("s3a://ph-stream/common/public/prod/0.0.1")
	# human_replace_data = spark.read.parquet("s3a://ph-stream/common/public/human_replace/0.0.11")
	# mole_human_replace_data = spark.read.parquet("s3a://ph-stream/common/public/mole_human_replace/0.0.1")

	# cpa_input_data.show(4)
	# product_data.show(4)
	# human_replace_data.show(4)
	# mole_human_replace_data.show(4)
	

	# 创建试用数据
	# cpa数据
	spark = SparkSession.builder.appName("cpa test").getOrCreate()
	cpa_test = [("1", "CPA&GYC", u"利妥昔单抗", u"美罗华", "500MG50ML", u"溶液剂（注射剂）", 1, 8.0, ), 
	            ("2", "CPA&GYC", u"利妥昔单抗", u"美华", "500MG50ML", u"溶液剂（注射剂）", 1, 8.0, ), 
	            ("3", "CPA&GYC", u"利妥昔", u"美罗华", "500MG50ML", u"溶液剂（注射剂）", 1, 8.0, ), 
	            ("4", "CPA&GYC", u"利妥昔单抗", u"美罗华", "500MG50ML", u"溶液剂（注射剂）", 1, 8.0, ), 
				("5", "CPA&GYC", u"咪康唑", u"达克宁", "400MG20GM", u"软膏", 1, 80.0, ), 
				("6","CPA&GYC", u"对乙酰氨基酚", u"泰诺林", "1.5GM15ML", u"滴瓶剂", 2, 600.0), 
				("7", "CPA&GYC", u"培美曲塞二钠", u"赛珍", "200MG", u"粉剂（粉剂针）", 3, 24.0)]
	cpa_schema = StructType([StructField('COMPANY',StringType(),True),
		StructField('SOURCE',StringType(),True),
		StructField('MOLE_NAME',StringType(),True),
		StructField('PRODUCT_NAME',StringType(),True),
		StructField('PACK',StringType(),True),
		StructField('SPEC',StringType(),True),
		StructField('DOSAGE',IntegerType(),True),
		StructField('PACK_QTY',StringType(),True),])
	cpa_test_df = spark.createDataFrame(cpa_test, schema=cpa_schema)
	cpa_test_df.show()
	
	lst = ["MOLE_NAME", "PRODUCT_NAME", "PACK"]
	min_keys_distinct_df = cpa_test_df.select(lst).distinct()
	min_keys_distinct_df.show()
	
	for col in min_keys_distinct_df.columns:
		min_keys_distinct_df.withColumnRenamed("")
	
	
	


	
	# # prod数据
	# prod_test = [("111", u"EN利妥昔单抗", u"利妥昔单抗", u"desc", u"美罗华", u"溶液", 1, ), 
	# 			("222", u"EN咪康唑", u"咪康唑", u"desc", u"达克宁", u"软膏", 1, ), 
	# 			("333",u"EN对乙酰氨基酚", u"对乙酰氨基酚", u"desc", u"泰诺林", u"滴", 2, )]
	# prod_schema = StructType([StructField('PACK_ID',StringType(),True),
	# 	StructField('MOLE_NAME_EN',StringType(),True),
	# 	StructField('MOLE_NAME_CH',StringType(),True),
	# 	StructField('PROD_DESC',StringType(),True),
	# 	StructField('PROD_NAME_CH',StringType(),True),
	# 	StructField('SPEC',StringType(),True),
	# 	StructField('DOSAGE',StringType(),True),])
	# prod_test_df = spark.createDataFrame(prod_test, schema=prod_schema)
	# prod_test_df.show()
	

	# prod_test_df = prod_test_df.groupBy()


	
	# new = cpa_test_df.withColumn("SOURCE",func.lit(0))
	# new.show()

	print("zyyin-test 程序end")
	print("--"*80)

	
execute()
		
