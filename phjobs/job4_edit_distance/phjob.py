# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

功能描述：job4: 计算编辑距离
  * @author yzy
  * @version 0.0
  * @since 2020/08/13
  * @note 输入数据：cpa_prod_join_data（根据mole_name，一条cpa数据join出来很多条与prod的匹配数据）
		  落盘数据：cpa_ed （ed_total列是总编辑距离）
  
"""


# from phlogs.phlogs import phlogger
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

def execute(out_path):
	"""
		please input your code below
	"""
	
	print("--"*80)
	print("程序start: job4_edit_distanct")
	
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
	
		
	os.environ["PYSPARK_PYTHON"] = "python3"
	# 读取s3桶中的数据
	spark = SparkSession.builder \
		.master("yarn") \
		.appName("BPBatchDAG") \
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
	cpa_prod_join_data = spark.read.parquet(out_path + "/" + "cpa_prod_join")
	# print(cpa_prod_join_data.count())  # 1181917
	
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

	@func.udf(returnType=IntegerType())
	def dosage(in_value, check_value):
		# 针对 dosage 
		# 只要存在包含关系，编辑距离直接为0，填入true
		redundancy_list_dosage = [u"（注射剂）", u"（粉剂针）", u"（胶丸、滴丸）", ]
		for redundancy in redundancy_list_dosage:
			in_value = in_value.replace(redundancy, "")
			
		dosage_mapping = {
			"SOLN": "注射",
			"POWD": "粉针",
			'SUSP':"混悬",
			'OINT': "膏剂",
			'NA': "鼻",
			'SYRP': "口服",
			'PATC': "贴膏",
			'EMUL': "乳",
			'AERO': "气雾",
			'GRAN': "颗粒",
			'SUPP': "栓",
			'PILL': "丸",
			'MISC': "混合",  # TODO 这个到底怎么命名？
			'LIQD': "溶液",
			'TAB': "片",
			'CAP': "胶囊",
		}
		
		for en, ch in dosage_mapping.items():
			in_value = in_value.replace(en, ch)
		
		if in_value in check_value:
			return 0
		else:
			return edit_distance(in_value, check_value)
	
	@func.udf(returnType=IntegerType())
	def product(in_value, check_value):
		# 针对 product name
		# 只要存在包含关系，编辑距离直接为0

		if (in_value in check_value) or (check_value in in_value):
			return 0
		else:
			return edit_distance(in_value, check_value)
		
	@func.udf(returnType=IntegerType())
	def pack_qty(in_value, check_value):
		return edit_distance(str(in_value), str(check_value).replace(".0", "")) 
		# return edit_distance(in_value.replace(".0", ""), check_value.replace(".0", ""))  # 所有情况都需要直接计算编辑距离 因为这个是数字
	
	@func.udf(returnType=IntegerType())	
	def mnf_en(in_value, check_value):
		# 针对英文生产厂家
		in_value = in_value.upper()
		check_value = check_value.upper()
		
		redundancy_list = ["GROUP", "LTD", "FACTORY", "OF", "CORPORATION", "&", "COMPANY", "S.R.L", "SRL", "CO", "PHARMA", "PHARM", \
							"PHA", "PH", "SA", "CARE", "INC", "PHAR", "PHARMACAL"]
		for redundancy in redundancy_list:
			# 这里将cpa数据与prod表的公司名称都去除了冗余字段
			in_value = in_value.replace(redundancy, "")
			check_value = check_value.replace(redundancy, "")
		in_value = in_value.strip()
		check_value = check_value.strip()
		
		return 30*edit_distance(in_value, check_value)
	
	def mnf_transform(mnf):
		str_geo = ""
		str_core = ""
		str_name = ""
	
		geo = jieba.analyse.extract_tags(mnf, topK=20, withWeight=True, allowPOS=('ns',))
		len_geo = len(geo)
		if len_geo == 1:
			str_geo = geo[0][0]
		elif len_geo > 1:
			str_geo = geo[-1][0]
	
		words = pseg.cut(mnf.replace(str_geo, ""))
	
		for word, flag in words:
			if word in ["有限公司", "股份", "控股", "集团", "总公司", "总厂", "厂", "责任", "公司", "有限", "有限责任", \
					"药业", "医药", "制药", "控股集团", "医药集团", "控股集团", "集团股份", "生物医药"]:
				str_name += word
			else:
				str_core += word
	
		return str_geo, str_core, str_name
		
		
	@func.udf(returnType=IntegerType())	
	def mnf_ch(in_value, check_value):
		redundancy_list = [u"股份", u"有限", u"总公司", u"公司", u"集团", u"制药", u"总厂", u"厂", u"药业", \
							u"责任", u"健康", u"科技", u"生物", u"工业", u"保健", u"医药", u"(", u")", u"（", u"）"]
		for redundancy in redundancy_list:
			in_value_new = in_value.replace(redundancy, "")
			check_value_new = check_value.replace(redundancy, "")
		if (in_value_new in check_value_new) or (check_value_new in in_value_new):
			ed = 0
		else:
			# ed = 35*edit_distance(in_value, check_value)
			in_value = in_value.replace("(", "").replace(")", "").replace("（", "").replace("）", "")
			check_value = check_value.replace("(", "").replace(")", "").replace("（", "").replace("）", "")
			in_str_geo, in_str_core, in_str_name = mnf_transform(in_value)
			check_str_geo, check_str_core, check_str_name = mnf_transform(check_value)
	
			if (in_str_geo in check_str_geo) or (check_str_geo in in_str_geo):
				ed_geo = 0
			else:
				ed_geo = edit_distance(in_str_geo, check_str_geo)
		
			if (in_str_core in check_str_core) or (check_str_core in in_str_core):
				ed_core = 0
			else:
				ed_core = edit_distance(in_str_core, check_str_core)
		
			ed_name = edit_distance(in_str_name, check_str_name)
	
			ed = int(60*(0.3 * ed_geo + 0.6 * ed_core + 0.1 * ed_name))
		return ed

	@func.udf(returnType=IntegerType())			
	def spec(in_value, check_value):
		
		strip_lst = ["SOLN", "POWD", "SUSP", "OINT", "NA", "SYRP", "PATC", "EMUL", \
					 "AERO", "GRAN", "SUPP", "PILL", "MISC", "LIQD", "TAB", "CAP", \
					 "OR", "BU", "EX", "IJ", "IN", "OP", "OR", "RE", "SL"]
	
		for item in strip_lst:
			in_value = in_value.replace(item, "")
		strinfo = re.compile(r'×\d+')
		in_value = strinfo.sub("", in_value).strip()
	
		new_in_spec = spec_reformat(in_value)
		new_check_spec = spec_reformat(check_value)
		
		lsta = new_in_spec.replace("CO", "").split()
		lstb = new_check_spec.replace("CO", "").split()
	
		if lsta and lstb:
			if (len(lsta) == 1) and (len(lstb) == 2) and (lsta[0] in lstb):
				return 0
			elif (len(lstb) == 1) and (len(lsta) == 2) and (lstb[0] in lsta):
				return 0
			elif (len(lsta) == 2) and (len(lstb) == 2) and (lsta[0] == lstb[1]) and (lsta[1] == lstb[0]):
					return 0
			else:
				return edit_distance(new_in_spec, new_check_spec)
		else:
			return edit_distance(new_in_spec, new_check_spec)
		
	@func.udf(returnType=IntegerType())			
	def edit_distance_total(ed_DOSAGE, ed_SPEC, ed_PACK, ed_MNF_NAME_CH, ed_MNF_NAME_EN, ed_PROD_NAME_CH):
		# 计算总编辑距离
		ed = ed_DOSAGE + 10*ed_SPEC + 60*ed_PACK + min(ed_MNF_NAME_CH, ed_MNF_NAME_EN) + ed_PROD_NAME_CH
		return ed
			
	mapping_config = {
		"check_DOSAGE": "in_DOSAGE",
		"check_PROD_NAME_CH": "in_PRODUCT_NAME",
		'check_PACK':"in_PACK_QTY",
		'check_MNF_NAME_CH': "in_MANUFACTURER_NAME",
		'check_MNF_NAME_EN': "in_MANUFACTURER_NAME",
		'check_SPEC': "in_SPEC",
	}
	
	# 编辑距离计算（0或过算法计算）
	cpa_ed = cpa_prod_join_data
	for check_name, in_name in mapping_config.items():
		if check_name == "check_PROD_NAME_CH":
			cpa_ed = cpa_ed.withColumn(check_name.replace("check", "ed"), product(in_name, check_name))
		elif check_name == "check_DOSAGE":
			cpa_ed = cpa_ed.withColumn(check_name.replace("check", "ed"), dosage(in_name, check_name))
		elif check_name == "check_MNF_NAME_CH":
			cpa_ed = cpa_ed.withColumn(check_name.replace("check", "ed"), mnf_ch(in_name, check_name))
		elif check_name == "check_MNF_NAME_EN":
			cpa_ed = cpa_ed.withColumn(check_name.replace("check", "ed"), mnf_en(in_name, check_name))	
		elif check_name == "check_PACK":
			cpa_ed = cpa_ed.withColumn(check_name.replace("check", "ed"), pack_qty(in_name, check_name))
		elif check_name == "check_SPEC":
			cpa_ed = cpa_ed.withColumn(check_name.replace("check", "ed"), spec(in_name, check_name))
	cpa_ed = cpa_ed.withColumn("ed_total", edit_distance_total("ed_DOSAGE", "ed_SPEC", "ed_PACK", "ed_MNF_NAME_CH", "ed_MNF_NAME_EN", "ed_PROD_NAME_CH"))

	# cpa_ed.select("in_SPEC", "check_SPEC", "ed_SPEC", "ed_total").show(100)
	# print(cpa_ed.count())  # 1181917
	
	# 写入
	out_path = out_path + "/" + "cpa_ed"
	cpa_ed.write.format("parquet").mode("overwrite").save(out_path)
	print("写入 " + out_path + " 完成")


	print("程序end job4_edit_distanct")
	print("--"*80)
	
