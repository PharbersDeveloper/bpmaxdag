# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

功能描述：job_utils
  * @author yzy
  * @version 0.0
  * @since 2020/08/25
  * @note:
  
"""

import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as func
import re
import numpy as np
from pyspark.sql.window import Window
import pandas as pd
import io
import boto3
from pyspark.sql.functions import pandas_udf, PandasUDFType
from nltk.metrics import jaccard_distance as jd



def execute():
	"""
		please input your code below
	"""
	
	print("--"*80)
	print("程序start: job_utils")
	
	os.environ["PYSPARK_PYTHON"] = "python3"
	# spark define
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
		
	in_prod_path = "s3a://ph-stream/common/public/prod/0.0.16"
	 
	@func.udf(returnType=StringType())
	def pack_id(in_value):
		return in_value.lstrip("0")
	 
		
	def human_replace_packid_check():
		# human_replace = spark.read.parquet("s3a://ph-max-auto/2020-08-11/BPBatchDAG/pfizer_check/human_replace_packid").withColumn("PACK_ID", pack_id("PACK_ID"))
		# human_replace.show(5)
		# 给human replace 加上pack_id
		
		human_replace_data = spark.read.parquet("s3a://ph-stream/common/public/human_replace/0.0.14")
		product_data = spark.read.parquet("s3a://ph-stream/common/public/prod/0.0.14")
	
	
		@func.udf(returnType=StringType())
		def change_pack(in_value):
			return in_value.replace(".0", "")
			
		# human_replace_data = spark.sql("select * from human_replace")
		print(human_replace_data.count())
		# human_replace_data.show(5)
		prod = product_data.select("PACK_ID", "MOLE_NAME_CH", "PROD_NAME_CH", "SPEC", "DOSAGE", "PACK", "MNF_NAME_CH") \
						   .withColumnRenamed("SPEC", "SPEC_prod").withColumnRenamed("DOSAGE", "DOSAGE_prod") \
						   .withColumn("PACK", change_pack("PACK"))
		prod.show(5)
		# print(prod.count())  # 41030
		# print(human_replace_data.count())  # 111594
		
		human_replace = human_replace_data.join(prod, 
									   [human_replace_data.MOLE_NAME == prod.MOLE_NAME_CH,
									   human_replace_data.PRODUCT_NAME == prod.PROD_NAME_CH,
									   human_replace_data.SPEC == prod.SPEC_prod,
									   human_replace_data.DOSAGE == prod.DOSAGE_prod, 
									   human_replace_data.PACK_QTY == prod.PACK,
									   human_replace_data.MANUFACTURER_NAME == prod.MNF_NAME_CH],
									   how="left").drop("MOLE_NAME_CH", "PROD_NAME_CH", "SPEC_prod", "DOSAGE_prod", "PACK", "MNF_NAME_CH" )
		human_replace.show(5)
		# print(human_replace.count()) # 111663 
		
		not_null = human_replace.filter(human_replace["PACK_ID"].isNotNull())
		not_null.show(5)
		# print(not_null.count())  # 65616
		
		# print(not_null.select("PACK_ID").distinct().count()) # 8884
	
	def prod_check():
		
		product_data = spark.read.parquet(in_prod_path)
		product_data.show(2)
		# print(product_data.count()) 
		
		df = product_data.groupBy("PACK_ID").count()
		df = df.filter(df["count"] == "1")
		df.show(3)
		# print(df.count())  # 44142 pack_id已经全部去重

	def ed_wrong_check():
		print("----------开始进行编辑距离错误数据检查----------")
		wrong_ed = spark.read.parquet("s3a://ph-max-auto/2020-08-11/BPBatchDAG/azsanofi_check/0.0.10/wrong_ed") \
							 .drop("version", "id", )
		# wrong_ed.show(3)
		print(wrong_ed.count())
		
		product_data = spark.read.parquet(in_prod_path) \
								.select("PACK_ID", "MOLE_NAME_CH", "MNF_NAME_CH", "DOSAGE", "SPEC", "PACK", "PROD_NAME_CH") \
								.withColumnRenamed("PACK_ID", "right_PACK_ID") \
								.withColumnRenamed("MOLE_NAME_CH", "right_MOLE_NAME") \
								.withColumnRenamed("MNF_NAME_CH", "right_MNF_NAME") \
								.withColumnRenamed("DOSAGE", "right_DOSAGE") \
								.withColumnRenamed("SPEC", "right_SPEC") \
								.withColumnRenamed("PACK", "right_PACK") \
								.withColumnRenamed("PROD_NAME_CH", "right_PROD_NAME")
												
		# product_data.show(4)
		
		check = wrong_ed.join(product_data, \
							  wrong_ed.PACK_ID_CHECK == product_data.right_PACK_ID, \
							  how="left")
		xixi1=check.toPandas()
		xixi1.to_excel('Pfizer_PFZ10_outlier.xlsx', index = False)
 
		check.select( "MOLE_NAME",\
					 #"MANUFACTURER_NAME", "match_MANUFACTURER_NAME_CH", "right_MNF_NAME", "ed_MNF_NAME_CH", \
					 #"ed_SPEC", "ed_PACK", "ed_PROD_NAME_CH", \
					 #"PRODUCT_NAME", "match_PRODUCT_NAME", "right_PROD_NAME",  \
					 "DOSAGE", "match_DOSAGE", "right_DOSAGE", "ed_DOSAGE", \
					 #"PACK_QTY", "match_PACK_QTY", "right_PACK", "PACK_ID_CHECK", \
					 #"SPEC", "match_SPEC", "right_SPEC","ed_SPEC", \
					 "PACK_ID_CHECK", "PACK_ID", "ed_total").show(100)
		
		
	def hr_check():
		wrong_hr = spark.read.parquet("s3a://ph-max-auto/2020-08-11/BPBatchDAG/pfi_check/0.0.4/wrong_hr") \
							 .drop("MOLE_NAME", "PRODUCT_NAME", "DOSAGE", "SPEC", "PACK_QTY", "MANUFACTURER_NAME", "version", "id", \
								   "ed_DOSAGE", "ed_PROD_NAME_CH", "ed_PACK", "ed_MNF_NAME_CH", "ed_MNF_NAME_EN", "ed_SPEC", "ed_total")
		# wrong_hr.show(4)
		product_data = spark.read.parquet(in_prod_path).select("PACK_ID", "MOLE_NAME_CH", "PROD_NAME_CH", "MNF_NAME_CH", "DOSAGE", "SPEC", "PACK") \
													   .withColumnRenamed("PACK_ID", "prod_PACK_ID") \
													   .withColumnRenamed("MOLE_NAME_CH", "prod_MOLE_NAME") \
													   .withColumnRenamed("PROD_NAME_CH", "prod_PROD_NAME") \
													   .withColumnRenamed("MNF_NAME_CH", "prod_MNF_NAME") \
													   .withColumnRenamed("DOSAGE", "prod_DOSAGE") \
													   .withColumnRenamed("SPEC", "prod_SPEC") \
													   .withColumnRenamed("PACK", "prod_PACK") 
														
		wrong_hr = wrong_hr.join(product_data, \
					  wrong_hr.PACK_ID_CHECK == product_data.prod_PACK_ID, \
					  how="left")
					  
		# wrong_hr.show(5)
		
		wrong_hr.select( \
						# "in_PRODUCT_NAME", "match_PRODUCT_NAME", "prod_PROD_NAME", \
						# "in_MOLE_NAME", "match_MOLE_NAME_CH", "prod_MOLE_NAME", \
						"in_MANUFACTURER_NAME", "match_MANUFACTURER_NAME_CH", "prod_MNF_NAME", \
						).show(54)
						
		# wrong_hr.select( \
		# 	            "in_PRODUCT_NAME", "match_PRODUCT_NAME", "prod_PROD_NAME", \
		# 	            ).show()
		# print(wrong_hr.count())  # 54
	
	def azsanofi_split():
		azsanofi = spark.read.parquet("s3a://ph-max-auto/v0.0.1-2020-06-08/AZ/202006/prod_mapping") 
		
		azsanofi = azsanofi.select("Molecule", "Brand", "Form", "Specifications", "Pack_Number", \
							"Source", "Manufacturer", "PFC（来自于文博的外部版本，文博版本的变动需要加到这里）")

		# azsanofi.show(5)
		# print(azsanofi.count())
		
		# az = azsanofi.filter(azsanofi.Source == "AZ") \
		# 			.withColumnRenamed("Molecule", "MOLE_NAME") \
		# 			.withColumnRenamed("Brand", "PRODUCT_NAME") \
		# 			.withColumnRenamed("Form", "DOSAGE") \
		# 			.withColumnRenamed("Specifications", "SPEC") \
		# 			.withColumnRenamed("Pack_Number", "PACK_QTY") \
		# 			.withColumnRenamed("Manufacturer", "MANUFACTURER_NAME") \
		# 			.withColumnRenamed("PFC（来自于文博的外部版本，文博版本的变动需要加到这里）", "PACK_ID_CHECK")
					
		# az.show(5)
		# print(az.count())
		
		# # 写入
		# out_path = "s3a://ph-max-auto/2020-08-11/BPBatchDAG/az_check/raw_data"
		# az.write.format("parquet").mode("overwrite").save(out_path)
		# print("写入 " + out_path + " 完成")
		
		sanofi = azsanofi.filter(azsanofi.Source == "Sanofi") \
					.withColumnRenamed("Molecule", "MOLE_NAME") \
					.withColumnRenamed("Brand", "PRODUCT_NAME") \
					.withColumnRenamed("Form", "DOSAGE") \
					.withColumnRenamed("Specifications", "SPEC") \
					.withColumnRenamed("Pack_Number", "PACK_QTY") \
					.withColumnRenamed("Manufacturer", "MANUFACTURER_NAME") \
					.withColumnRenamed("PFC（来自于文博的外部版本，文博版本的变动需要加到这里）", "PACK_ID_CHECK")
					
		sanofi.show(5)
		print(sanofi.count())
		
		# 写入
		out_path = "s3a://ph-max-auto/2020-08-11/BPBatchDAG/sanofi_check/raw_data"
		sanofi.write.format("parquet").mode("overwrite").save(out_path)
		print("写入 " + out_path + " 完成")
		
	# @func.udf(returnType=StringType())	
	def spec_reformat(input_str):
		def spec_transform(input_data):
			# TODO: （）后紧跟单位的情况无法处理
			# eg 1% (150+37.5)MG 15G 拆成['(150+37.5)', '1% MG', '15G']
			input_data = input_data.replace("μ", "U").replace("万", "T")
			bracket_regex = '\((.*?)\)'
			bracket_dict = re.findall(bracket_regex, input_data.upper())
			
			if len(bracket_dict) == 1:
				bracket_item = '(' + bracket_dict[0] + ')'
				bracket_dict = [bracket_item]
				other_str = input_data.upper().replace(bracket_item, "")
			elif len(bracket_dict) == 2:
				bracket_dict = ['(' + bracket_dict[0] + ')', '(' + bracket_dict[1] + ')']
				other_str = input_data.upper()
				for bracket in bracket_dict:
					other_str = other_str.replace(bracket, "")
			else:
				bracket_item = ""
				other_str = input_data.upper().replace(bracket_item, "")
	
			regex = r"CO|[0-9]\d*\.?\d*\s*[A-Za-z%]*/?\s*[A-Za-z%]+"
			# r"CO|[0-9]+.?[0-9]+\s*[A-Za-z%]*/?\s*[A-Za-z%]+"
			other_item = re.findall(regex, other_str)
			items = bracket_dict + other_item
			print(items)
			return items
		
		def unit_transform(spec_str):
			# 输入一个数字+单位的str，输出同一单位后的str
	
			# 拆分数字和单位
			digit_regex = '\d+\.?\d*e?-?\d*?'
			# digit_regex = '0.\d*'
			value = re.findall(digit_regex, spec_str)[0]
			unit = spec_str.strip(value)  # type = str
			# value = float(value)  # type = float
			try:
				value = float(value)  # type = float
			except ValueError:
				value = 0.0
			
			# value transform
			if unit == "G" or unit == "GM":
				value = round(value *1000, 2)
			elif unit == "UG":
				value = round(value /1000, 4)
			elif unit == "L":
				value = round(value *1000, 2)
			elif unit == "TU" or unit == "TIU":
				value = round(value *10000, 2)
			elif unit == "MU" or unit == "MIU" or unit == "M":
				value = round(value *1000000, 2)
	
			# unit transform
			unit_switch = {
					"G": "MG",
					"GM": "MG",
					"MG": "MG",
					"UG": "MG",
					"L": "ML",
					"AXAU": "U",
					"AXAIU": "U",
					"IU": "U",
					"TU": "U",
					"TIU": "U",
					"MU": "U",
					"MIU": "U",
					"M": "U",
				}
				
			try:	
				unit = unit_switch[unit]
			except KeyError:
				pass
	
			return str(value) + unit
				
	
			
		def classify_item(spec_dict):
			# 对拆分出来的每个item进行筛选 1. 去掉无用数据  2. 比例转换为百分数 百分数保留原样  3.同一单位
			# 输出一个字典 包含co 百分比 gross中的一个或多个
			
			final_lst = []
			final_dict = {"CO": "", "spec": [], 'percentage':"", }
			for item in spec_dict:
				item = item.replace(" ", "")
				if item.startswith("(") or item.startswith("（"):
					# 如果是带括号的情况
					item = item.replace("(", "").replace(")", "").replace("（", "").replace("）", "")
					if re.search('[0-9]+:[0-9]+', item): # 比例->百分数
						lst = item.split(":")
						lst.sort() #升序排序 注意sort（）方法没有返回值
						percentage = float(lst[0]) / (float(lst[1]) + float(lst[0])) * 100
						final_lst.append(str(percentage))
						final_dict["percentage"] = str(round(percentage, 2)) + "%"
					elif re.search('[0-9]+(\.\d+)?[A-Za-z]+/[A-Za-z]+', item): # 占比的另一种表示eg 20mg/ml 可删
						pass
					elif re.search('[0-9]+(\.\d+)?[A-Za-z]*[:+][0-9]+(\.\d+)?[A-Za-z]*', item): # 表示有多种成分(0.25G:or+0.25G) 执行unit transform
						multi_ingre_lst = re.split('[+:]', item)
						ingre_str = ""
						if multi_ingre_lst:
							for ingre in multi_ingre_lst:
								ingre_str = ingre_str + unit_transform(ingre) + "+"
						final_dict["spec"].append(ingre_str[:-1])
					elif re.search(r'^[\u4e00-\u9fa5]+', item):  # 是中文开头的情况
						pass
					elif re.search('[0-9]+(\.\d+)?[A-Za-z]+', item): # 只有数字+单位 执行unit transform
						final_dict["spec"].append(unit_transform(item))
					else: # 其余情况 舍弃
						pass
				
				elif item.endswith("%"):  # 如果是百分比，直接写入"percentage": ""
					final_lst.append(item)
					final_dict["percentage"] = item
				
				elif item == "CO":
					final_lst.append(item)
					final_dict["CO"] = item
					
				elif re.search('[0-9]+(\.\d+)?[A-Za-z]+', item):  #数字+单位->unit transform
					final_dict["spec"].append(unit_transform(item))
			return final_dict
			
		def get_final_spec(final_dict):
			# 输入上一步得到的分段字典 输出最终spec
			final_spec_str = final_dict["CO"] + " "
			if len(final_dict["spec"]) == 1:
				if final_dict["percentage"]:
					digit_regex = '[0-9.]*'
					value = re.findall(digit_regex, final_dict["spec"][0])[0]
					unit = final_dict["spec"][0].strip(value)
					percent = float(final_dict["percentage"].replace("%", "").replace(" ", ""))
					final_spec_str = final_spec_str + str(float(value) * percent / 100) + unit + " " + final_dict["spec"][0] + " "
				else:
					final_spec_str = final_spec_str + final_dict["spec"][0] + " "
					
			elif len(final_dict["spec"]) == 2:		
				if ([True, True] == [("%" not in l) for l in final_dict["spec"]]): # 两个都不是百分比 直接写入
					final_spec_str += final_dict["spec"][0] + " " + final_dict["spec"][1] + " "
				elif ([False, True] == [("%" not in l) for l in final_dict["spec"]]): # 【百分比，数字单位】 计算	
					digit_regex = '[0-9.]*'
					percent = float(final_dict["spec"][0].replace("%", "").replace(" ", ""))
					value = re.findall(digit_regex, final_dict["spec"][1])[0]
					unit = final_dict["spec"][1].strip(value)
					final_spec_str += str(float(value) * percent / 100) + unit + " " + final_dict["spec"][1]
			elif len(final_dict["spec"]) >= 3: # todo: 这里直接全部写入了 不知道特殊情况是否会造成误差
				for i in final_dict["spec"]:
					final_spec_str += i + " "
				
			return final_spec_str.strip()
	
		split_item_dict = spec_transform(input_str)  # 输入str 返回值是dict
		# print(split_item_dict)
		final_dict = classify_item(split_item_dict) # 输入dict 返回值是dict
		# print(final_dict)
		final_spec = get_final_spec(final_dict) # 输入dict 返回值是str
		return final_spec


	def s3excel2parquet():
		access_key = os.getenv("AWS_ACCESS_KEY_ID")
		secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
		
		SOURCE_BUCKET = 'ph-max-auto'
		SOURCE_PATH = "2020-08-11/BPBatchDAG/refactor/zyyin/spec_split_test_file/spec_split_test_file.xlsx"
		TARGET_BUCKET = 'ph-max-auto'
		TARGET_PATH = "2020-08-11/BPBatchDAG/refactor/zyyin/spec_split_test_file"
		
		print("开始读取")
		
		s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
		object_file = s3_client.get_object(Bucket=SOURCE_BUCKET, Key=SOURCE_PATH)
		data = object_file['Body'].read()
		pd_df = pd.read_excel(io.BytesIO(data))
		
		os.environ["PYSPARK_PYTHON"] = "python3"
		spark = SparkSession.builder \
		    .master("yarn") \
		    .appName("data cube cal measures") \
		    .config("spark.driver.memory", "1g") \
		    .config("spark.executor.cores", "2") \
		    .config("spark.executor.instance", "4") \
		    .config("spark.executor.memory", "2g") \
		    .config('spark.sql.codegen.wholeStage', False) \
		    .getOrCreate()
		
		if access_key is not None:
		    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
		    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
		    spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
		    spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
		    # spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
		    spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.cn-northwest-1.amazonaws.com.cn")
		
		sdf = spark.createDataFrame(pd_df.astype(str))
		# .select("Group")
		sdf.show(5)
		
		print("开始写入")
		save_path = "s3a://%s/%s" % (TARGET_BUCKET, TARGET_PATH)
		sdf.write.format("parquet").mode("overwrite").save(save_path)
		print("写入" + save_path + "完成")

	def rematch_null_raw_data():
		# 需要加job 进行mole_name / mnf_name 等匹配不上的数据进行二次匹配
		mole_replace_mine = spark.read.parquet("s3a://ph-max-auto/2020-08-11/BPBatchDAG/azsanofi_check/0.0.11/cpa_prod_join_null_PRODUCT_NAME") \
										.select("in_MOLE_NAME", "in_PRODUCT_NAME", "in_SPEC", "in_DOSAGE", "in_PACK_QTY", "in_MANUFACTURER_NAME")
		mole_replace_mine.show(3)
		# print(mole_replace_mine.count())
		
		azsanofi = spark.read.parquet("s3a://ph-max-auto/2020-08-11/BPBatchDAG/azsanofi_check/0.0.11/cpa_distinct")
		azsanofi.show(3)
		# print(azsanofi.count())
		
		mole_replace_mine = mole_replace_mine.join(azsanofi, \
													[mole_replace_mine.in_MOLE_NAME == azsanofi.MOLE_NAME, \
													mole_replace_mine.in_PRODUCT_NAME == azsanofi.PRODUCT_NAME, \
													mole_replace_mine.in_SPEC == azsanofi.SPEC, \
													mole_replace_mine.in_DOSAGE == azsanofi.DOSAGE, \
													mole_replace_mine.in_PACK_QTY == azsanofi.PACK_QTY, \
													mole_replace_mine.in_MANUFACTURER_NAME == azsanofi.MANUFACTURER_NAME,],\
													how="left").select("MOLE_NAME", "PRODUCT_NAME", "SPEC", "DOSAGE", "PACK_QTY", "MANUFACTURER_NAME", "PACK_ID_CHECK")
		mole_replace_mine.show(5)
		print(mole_replace_mine.count())
		# print(mole_replace_mine.filter(mole_replace_mine.PACK_ID_CHECK.isNull()).count())
		mole_replace_mine.write.format("parquet").mode("overwrite").save("s3a://ph-max-auto/2020-08-11/BPBatchDAG/azsanofi_check/0.0.12/raw_data")

	# rematch_null_raw_data()
	# prod_check()
	# ed_wrong_check()
	# spec_reformat_test()  # 将错误匹配的剂型信息对比一下
	# hr_check()
	# azsanofi_split()
	s3excel2parquet()

	# def spec_check():
	# print(spec_reformat("10g:200万IU") == "10000.0MG 2000000.0U")
	# print(spec_reformat("倍氯米松50μg") == "0.05MG")
	# print(spec_reformat("50UG/200DOS") == "0.05MG 200.0DOS")
	# print(spec_reformat("3MU 1ML") == "3000000.0U 1.0ML")
	# print(spec_reformat("3.40MU 1.2ML") == "3400000.0U 1.2ML")
	# print(spec_reformat("3.4mg/ml 1.2ML") == "3.4MG/ML 1.2ML")
	# print(spec_reformat("CO 1.25 GM") == "CO 1250.0MG")
	# print(spec_reformat("5% 100ML") == "5.0ML 100.0ML")
	# print(spec_reformat("5%(1G/20G) 20G") == "1.0G/20G 20000.0MG")
	# print(spec_reformat("1M 5G") == "1000000.0U 5000.0MG")
	# print(spec_reformat("1g(亚胺培南0.5g,西司他丁0.5g)") == "1000000.0U 5000.0MG")
	# print(spec_reformat("20.0ml 0.4g") == "20.0ML 400.0MG")
	# print(spec_reformat("50万U") == spec_reformat("0.5MU"))
	# print(spec_reformat("25% 250ML") == spec_reformat("62.5ML 250.0ML"))
	# print(spec_reformat("110MG(按伊曲康唑计100MG)") == "110.0MG")
	# print(spec_reformat(" (2:1) 2.25G") == "2.25G")
	# print(spec_reformat("依折麦布10mg,辛伐他汀20mg") == "2.25G")
	# print(spec_reformat(" (250MG+8.77MG)") == "2.25G")
	# print(spec_reformat("18ΜG"))
	


	print("程序end: job_utils")
	print("--"*80)
	
	
execute()