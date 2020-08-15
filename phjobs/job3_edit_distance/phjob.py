# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

功能描述：job3: 计算编辑距离
  * @author yzy
  * @version 0.0
  * @since 2020/08/13
  * @note
  
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
	print("程序start: job3_edit_distanct")
	
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
	cpa_prod_join_data = spark.read.parquet("s3a://ph-max-auto/2020-08-11/BPBatchDAG/cpa_prod_join")
	cpa_prod_join_data = cpa_prod_join_data.na.fill("")
	print(cpa_prod_join_data.dtypes)
	# product_data = spark.read.parquet("s3a://ph-stream/common/public/prod/0.0.14")
	# cpa_prod_join_data.show(4) 
	# print(cpa_prod_join_data.count())  # 5851567

	@func.udf(returnType=BooleanType())
	def contain_or_not(in_value, check_value):
		# 针对 dosage 和 product name
		# 只要存在包含关系，编辑距离直接为0，填入true
		return (in_value in check_value) or (check_value in in_value)
		
	@func.udf(returnType=BooleanType())
	def pack_qty():
		return False  # 所有情况都需要直接计算编辑距离 因为这个是数字
		
	@func.udf(returnType=BooleanType())	
	def replace_and_contain(in_value, check_value):
		# 针对生产厂家
		redundancy_list = [u"股份", u"公司", u"有限", u"总公司", u"集团", u"制药", u"总厂", u"厂", u"药业", u"责任", \
						   u"健康", u"科技", u"生物", u"工业", u"保健", u"医药", "(", ")", u"（", u"）", " "]
		for redundancy in redundancy_list:
			# 这里将cpa数据与prod表的公司名称都去除了冗余字段
			in_value = in_value.replace(redundancy, "")
			check_value = check_value.replace(redundancy, "")
		return (in_value in check_value) or (check_value in in_value)
		
	@func.udf(returnType=IntegerType())			
	def spec(in_value, check_value):
		if isinstance(in_value, StringType):
			return 1
		if isinstance(in_value, FloatType):
			return 2
			
	mapping_config = {
		'in_DOSAGE': "check_DOSAGE",
		'in_PRODUCT_NAME': "check_PROD_NAME_CH",
		'in_PACK_QTY':"check_PACK",
		'in_MANUFACTURER_NAME': "check_MNF_NAME_CH",
		'in_SPEC': "check_SPEC",
	}
	
	# 判断是否需要计算编辑距离 并把bool类型的结果写入新列"bool_colname"
	# 如果编辑距离可以直接判断为0，为true，如果需要后续计算编辑距离，为true
	new = cpa_prod_join_data
	for in_name, check_name in mapping_config.items():
		if (in_name == "in_DOSAGE") or (in_name == "in_PRODUCT_NAME"):
			new = new.withColumn("bool_" + in_name, contain_or_not(in_name, check_name))
		elif in_name == "in_MANUFACTURER_NAME":
			new = new.withColumn("bool_" + in_name, replace_and_contain(in_name, check_name))
		elif in_name == "in_PACK_QTY":
			new = new.withColumn("bool_" + in_name, spec(in_name, check_name))
		# elif in_name == "in_SPEC":
		# 	new = new.withColumn("bool_" + in_name, spec(in_name, check_name))

	
	new.show(10)
	# new.select("in_DOSAGE", "check_DOSAGE", 'in_PRODUCT_NAME', "check_PROD_NAME_CH", "bool_in_DOSAGE", "bool_in_PRODUCT_NAME").show(10)	




	print("程序end job3_edit_distanct")
	print("--"*80)
	

execute()	