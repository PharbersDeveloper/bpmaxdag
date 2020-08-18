# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

功能描述：job3: 计算编辑距离
  * @author yzy
  * @version 0.0
  * @since 2020/08/13
  * @note
  
"""


# from phlogs.phlogs import phlogger
import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as func
import re
import numpy as np

def execute():
	"""
		please input your code below
	"""
	
	print("--"*80)
	print("程序start: job3_edit_distanct")
	
	def spec_reformat(input_str):
		def spec_transform(input_data):
			# TODO: （）后紧跟单位的情况无法处理
			# eg 1% (150+37.5)MG 15G 拆成['(150+37.5)', '1% MG', '15G']
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
	
			regex = r"CO|[0-9]*[.:]?[0-9]+\s*[A-Za-z%]*/?\s*[A-Za-z%]*"
			other_item = re.findall(regex, other_str)
			items = bracket_dict + other_item
	
			return items
		
		def unit_transform(spec_str):
			# 输入一个数字+单位的str，输出同一单位后的str
	
			# 拆分数字和单位
			digit_regex = '[0-9.]*'
			value = re.findall(digit_regex, spec_str)[0]
			unit = spec_str.strip(value)  # type = str
			value = float(value)  # type = float
			
			# value transform
			if unit == "G" or unit == "GM":
				value = round(value *1000, 2)
			elif unit == "UG":
				value = round(value /1000, 2)
			elif unit == "L":
				value = round(value *1000, 2)
			elif unit == "万U":
				value = round(value *10000, 2)
			elif unit == "MU" or unit == "MIU":
				value = round(value /1000, 4)
	
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
					"万U": "U",
					"MU": "U",
					"MIU": "U",
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
					elif re.search('[0-9]+(\.\d+)?[A-Za-z]+', item): # 只有数字+单位 执行unit transform
						final_dict["spec"].append(unit_transform(item))
					else: # 其余情况 舍弃
						pass
				
				elif item.endswith("%"):  # 如果是百分比，直接写入"percentage": ""
					print(item)
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
	# product_data = spark.read.parquet("s3a://ph-stream/common/public/prod/0.0.14")
	# cpa_prod_join_data.show(4) 
	# print(cpa_prod_join_data.count())  # 5851567
	
	def edit_distance(in_value, check_value):
		# 输入两个字符串 计算编辑距离 输出int
		return len(in_value) + len(check_value)

	@func.udf(returnType=IntegerType())
	def contain_or_not(in_value, check_value):
		# 针对 dosage 和 product name
		# 只要存在包含关系，编辑距离直接为0，填入true
		if (in_value in check_value) or (check_value in in_value):
			return 0
		else:
			return edit_distance(in_value, check_value)
		
	@func.udf(returnType=IntegerType())
	def pack_qty(in_value, check_value):
		return edit_distance(in_value, check_value)  # 所有情况都需要直接计算编辑距离 因为这个是数字
		
	@func.udf(returnType=IntegerType())	
	def replace_and_contain(in_value, check_value):
		# 针对生产厂家
		redundancy_list = [u"股份", u"公司", u"有限", u"总公司", u"集团", u"制药", u"总厂", u"厂", u"药业", u"责任", \
						   u"健康", u"科技", u"生物", u"工业", u"保健", u"医药", "(", ")", u"（", u"）", " "]
		for redundancy in redundancy_list:
			# 这里将cpa数据与prod表的公司名称都去除了冗余字段
			in_value = in_value.replace(redundancy, "")
			check_value = check_value.replace(redundancy, "")
			
		if (in_value in check_value) or (check_value in in_value):
			return 0
		else:
			return edit_distance(in_value, check_value)

	@func.udf(returnType=StringType())			
	def spec(in_value, check_value):
		new_in_spec = spec_reformat(in_value)
		new_check_spec = spec_reformat(check_value)
		return new_in_spec + new_check_spec
			
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
			new = new.withColumn("ed_" + in_name.replace("in_", ""), contain_or_not(in_name, check_name))
		elif in_name == "in_MANUFACTURER_NAME":
			new = new.withColumn("ed_" + in_name.replace("in_", ""), replace_and_contain(in_name, check_name))
		elif in_name == "in_PACK_QTY":
			new = new.withColumn("ed_" + in_name.replace("in_", ""), pack_qty(in_name, check_name))
		# if in_name == "in_SPEC":
		# 	new = new.withColumn("ed_" + in_name.replace("in_", ""), spec("in_SPEC", check_name))

	# print(cpa_prod_join_data.dtypes)
	# new.select("in_SPEC", "check_SPEC", "bool_in_SPEC").distinct().show(10)
	# new.select("in_DOSAGE", "check_DOSAGE", "bool_in_DOSAGE").show(100)
	# new.select("in_SPEC", "check_SPEC", "in_MOLE_NAME").distinct().show(100)
	new.show(5)

	# cpa_prod_join_data.show(3)


	print("程序end job3_edit_distanct")
	print("--"*80)
	

execute()	