# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from ph_logs.ph_logs import phs3logger
import math
from math import isnan, sqrt
import uuid
import re
import pandas as pd
import numpy as np
from pyspark.sql.types import *
from pyspark.sql.functions import regexp_replace, regexp_extract
from pyspark.sql.functions import when, explode, concat, upper, lit
from pyspark.sql.functions import pandas_udf, PandasUDFType
from nltk.metrics.distance import jaro_winkler_similarity


def execute(**kwargs):
	"""
		please input your code below
		get spark session: spark = kwargs["spark"]()
	"""
	logger = phs3logger(kwargs["job_id"])
	spark = kwargs["spark"]()
	
	logger.info(kwargs)

	# input	
	depends = get_depends_path(kwargs)
	df_second_round = spark.read.parquet(depends["input"])
	df_dosage_mapping = spark.read.parquet(kwargs["dosage_mapping_path"])
	g_repartition_shared = int(kwargs["g_repartition_shared"])
	
	# output 	
	job_id = get_job_id(kwargs)
	run_id = get_run_id(kwargs)
	result_path_prefix = get_result_path(kwargs, run_id, job_id)
	result_path = result_path_prefix + kwargs["spec_adjust_result"]
	mid_path = result_path_prefix + kwargs["spec_adjust_mid"]

	# 6. 第二轮更改优化eff的计算方法
	df_second_round = df_second_round.withColumnRenamed("EFFTIVENESS_SPEC", "EFFTIVENESS_SPEC_FIRST")
	# df_second_round = second_round_with_col_recalculate(df_second_round, df_dosage_mapping, df_encode, spark)
	df_second_round = second_round_with_col_recalculate(df_second_round, df_dosage_mapping, spark)
	# spec拆列之后的匹配算法
	df_second_round = spec_split_matching(df_second_round)
	df_second_round = df_second_round.withColumn("EFFTIVENESS_SPEC", when((df_second_round.EFFTIVENESS_SPEC_FIRST > df_second_round.EFFTIVENESS_SPEC_SPLIT), \
																		df_second_round.EFFTIVENESS_SPEC_FIRST) \
																		.otherwise(df_second_round.EFFTIVENESS_SPEC_SPLIT))
	df_second_round = df_second_round.withColumnRenamed("EFFTIVENESS_PRODUCT_NAME", "EFFTIVENESS_PRODUCT_NAME_FIRST") \
								.withColumnRenamed("EFFTIVENESS_DOSAGE", "EFFTIVENESS_DOSAGE_FIRST") \
								.withColumnRenamed("EFFTIVENESS_DOSAGE_SE", "EFFTIVENESS_DOSAGE") \
								.withColumnRenamed("EFFTIVENESS_MANUFACTURER_SE", "EFFTIVENESS_MANUFACTURER") \
								.withColumnRenamed("EFFTIVENESS_PRODUCT_NAME_SE", "EFFTIVENESS_PRODUCT_NAME")
								# .withColumnRenamed("EFFTIVENESS_MANUFACTURER", "EFFTIVENESS_MANUFACTURER_FIRST") \
								
	df_second_round.persist()
	df_second_round.repartition(g_repartition_shared).write.mode("overwrite").parquet(mid_path)

	cols = ["sid", "id","PACK_ID_CHECK",  "PACK_ID_STANDARD","DOSAGE","MOLE_NAME","PRODUCT_NAME","SPEC","PACK_QTY","MANUFACTURER_NAME","SPEC_ORIGINAL",
			"MOLE_NAME_STANDARD","PRODUCT_NAME_STANDARD","CORP_NAME_STANDARD","MANUFACTURER_NAME_STANDARD","MANUFACTURER_NAME_EN_STANDARD","DOSAGE_STANDARD","SPEC_STANDARD","PACK_QTY_STANDARD",
			"SPEC_valid_digit_STANDARD","SPEC_valid_unit_STANDARD","SPEC_gross_digit_STANDARD","SPEC_gross_unit_STANDARD","SPEC_STANDARD_ORIGINAL",
			"EFFTIVENESS_MOLE_NAME","EFFTIVENESS_PRODUCT_NAME","EFFTIVENESS_DOSAGE","EFFTIVENESS_PACK_QTY","EFFTIVENESS_MANUFACTURER","EFFTIVENESS_SPEC"]
	df_second_round = df_second_round.select(cols)
	df_second_round.repartition(g_repartition_shared).write.mode("overwrite").parquet(result_path)
	

	return {}


################--------------------- functions ---------------------################
"""
中间文件与结果文件路径
"""
def get_run_id(kwargs):
	run_id = kwargs["run_id"]
	if not run_id:
		run_id = "runid_" + "alfred_runner_test"
	return run_id


def get_job_id(kwargs):
	job_name = kwargs["job_name"]
	job_id = kwargs["job_id"]
	if not job_id:
		job_id = "jobid_" + uuid.uuid4().hex
	return job_name # + "_" + job_id 


def get_result_path(kwargs, run_id, job_id):
	path_prefix = kwargs["path_prefix"]
	return path_prefix + "/" + run_id + "/" + job_id + "/"


def get_depends_file_path(kwargs, job_name, job_key):
	run_id = get_run_id(kwargs)
	return get_result_path(kwargs, run_id, job_name) + job_key
	

def get_depends_path(kwargs):
	depends_lst = eval(kwargs["depend_job_names_keys"])
	result = {}
	for item in depends_lst:
		tmp_lst = item.split("#")
		depends_job = tmp_lst[0]
		depends_key = tmp_lst[1]
		depends_name = tmp_lst[2]
		result[depends_name] = get_depends_file_path(kwargs, depends_job, depends_key)
	return result


def second_round_with_col_recalculate(df_second_round, dosage_mapping, spark):
	# df_second_round.show()
	# df_second_round.printSchema()
	# dosage_mapping.show()
	df_second_round = df_second_round.join(dosage_mapping, "DOSAGE", how="left").na.fill("")
	# df_second_round = df_second_round.withColumn("MASTER_DOSAGE", when(df_second_round.MASTER_DOSAGE.isNull(), df_second_round.JACCARD_DISTANCE). \
						# otherwise(df_second_round.MASTER_DOSAGE))
	df_second_round = df_second_round.withColumn("EFFTIVENESS_DOSAGE_SE", dosage_replace(df_second_round.MASTER_DOSAGE, \
														df_second_round.DOSAGE_STANDARD, df_second_round.EFFTIVENESS_DOSAGE)) 
	df_second_round = mole_dosage_calculaltion(df_second_round)   # 加一列EFF_MOLE_DOSAGE，doubletype
	df_second_round = df_second_round.withColumn("EFFTIVENESS_PRODUCT_NAME_SE", \
							prod_name_replace(df_second_round.EFFTIVENESS_MOLE_NAME, 
											df_second_round.EFFTIVENESS_PRODUCT_NAME, df_second_round.MOLE_NAME, \
											df_second_round.PRODUCT_NAME_STANDARD, df_second_round.EFF_MOLE_DOSAGE))

	return df_second_round


@pandas_udf(DoubleType(), PandasUDFType.SCALAR)
def dosage_replace(dosage_lst, dosage_standard, eff):

	frame = { "MASTER_DOSAGE": dosage_lst, "DOSAGE_STANDARD": dosage_standard, "EFFTIVENESS_DOSAGE": eff }
	df = pd.DataFrame(frame)

	df["EFFTIVENESS"] = df.apply(lambda x: 1.0 if ((x["DOSAGE_STANDARD"] in x["MASTER_DOSAGE"]) ) \
											else x["EFFTIVENESS_DOSAGE"], axis=1)

	return df["EFFTIVENESS"]

	
def mole_dosage_calculaltion(df):
	@pandas_udf(DoubleType(), PandasUDFType.SCALAR)
	def pudf_jws_func_tmp(s1, s2):
		frame = {
			"LEFT": s1,
			"RIGHT": s2
		}
		df = pd.DataFrame(frame)
		
		df["RESULT"] = df.apply(lambda x: jaro_winkler_similarity(x["LEFT"], x["RIGHT"]), axis=1)
		return df["RESULT"]
		
	# 给df 增加一列：EFF_MOLE_DOSAGE
	df_dosage_explode = df.withColumn("MASTER_DOSAGES", explode("MASTER_DOSAGE"))
	df_dosage_explode = df_dosage_explode.withColumn("MOLE_DOSAGE", concat(df_dosage_explode.MOLE_NAME, df_dosage_explode.MASTER_DOSAGES))
	df_dosage_explode = df_dosage_explode.withColumn("jws", pudf_jws_func_tmp(df_dosage_explode.MOLE_DOSAGE, df_dosage_explode.PRODUCT_NAME_STANDARD))
	df_dosage_explode = df_dosage_explode.groupBy('id').agg({"jws":"max"}).withColumnRenamed("max(jws)","EFF_MOLE_DOSAGE")
	df_dosage = df.join(df_dosage_explode, "id", how="left")
	
	return df_dosage


def spec_split_matching(df):
	# df.printSchema()
	
	df = df.withColumn("SPEC_valid_total_STANDARD",  spec_valid_std_transfer_pandas_udf(df.SPEC_valid_digit_STANDARD))
	# df = df.where(df.SPEC_STANDARD == "84.5UG  /DOS  60")
	
	df = df.withColumn("SPEC", regexp_replace("SPEC", r"(微克)", "UG"))
	df = df.withColumn("SPEC", regexp_replace("SPEC", r"[()]", ""))
	df = df.withColumn("SPEC", regexp_replace("SPEC", r"(（)", ""))
	df = df.withColumn("SPEC", regexp_replace("SPEC", r"(）)", ""))
	df = df.withColumn("SPEC", regexp_replace("SPEC", r"(Μ)", "M"))
	# df = df.drop("SPEC_valid_digit_STANDARD", "SPEC_valid_unit_STANDARD", "SPEC_gross_digit_STANDARD", "SPEC_gross_unit_STANDARD", "SPEC_STANDARD")

	
	df = df.withColumn("SPEC", regexp_replace("SPEC", r"(万)", "T"))
	df = df.withColumn("SPEC", regexp_replace("SPEC", r"(μ)", "U"))
	df = df.withColumn("SPEC", upper(df.SPEC))
	df = df.replace(" ", "")
	# df = df.where(df.SPEC_STANDARD == "62.5UG+25UG/DOS 30")
	# df = df.withColumn("SPEC_gross", regexp_extract('SPEC', spec_regex, 2))
	# 拆分规格的成分
	# df = df.withColumn("SPEC_percent", regexp_extract('SPEC', r'(\d*.*\d+%)', 1))
	# df = df.withColumn("SPEC_co", regexp_extract('SPEC', r'(CO)', 1))
	spec_valid_regex =  r'([0-9]\d*\.?\d*\s*[A-Za-z]*/?\s*[A-Za-z]+)'
	df = df.withColumn("SPEC_valid", regexp_extract('SPEC', spec_valid_regex, 1))
	spec_gross_regex =  r'([0-9]\d*\.?\d*\s*[A-Za-z]*/?\s*[A-Za-z]+)[ ,/:∶+\s]*[\u4e00-\u9fa5]*([0-9]\d*\.?\d*\s*[A-Za-z]*/?\s*[A-Za-z]+)'
	df = df.withColumn("SPEC_gross", regexp_extract('SPEC', spec_gross_regex, 2))
	
	spec_valid_se_regex =  r'([0-9]\d*\.?\d*\s*[:/+][0-9]\d*\.?\d*\s*[A-Za-z]+)'
	df = df.withColumn("SPEC_valid_2", regexp_extract('SPEC', spec_valid_se_regex, 1))
	df = df.withColumn("SPEC_valid", when((df.SPEC_valid_2 != ""), df.SPEC_valid_2).otherwise(df.SPEC_valid))
	# df = df.drop("SPEC_valid_2")
	
	# spec_third_regex = r'([0-9]\d*\.?\d*\s*[A-Za-z]*/?\s*[A-Za-z]+)[ /:∶+\s]([0-9]\d*\.?\d*\s*[A-Za-z]*/?\s*[A-Za-z]+)[ /:∶+\s]([0-9]\d*\.?\d*\s*[A-Za-z]*/?\s*[A-Za-z]+)'
	# df = df.withColumn("SPEC_third", regexp_extract('SPEC', spec_third_regex, 3))

	# pure_number_regex_spec = r'(\s\d+$)'
	# df = df.withColumn("SPEC_pure_number", regexp_extract('SPEC', pure_number_regex_spec, 1))
	# dos_regex_spec = r'(/DOS)'
	# df = df.withColumn("SPEC_dos", regexp_extract('SPEC', dos_regex_spec, 1))

	
	# df = df.withColumn("SPEC_valid", dos_pandas_udf(df.SPEC_valid, df.SPEC_pure_number, df.SPEC_dos))
	
	# 单位转换
	df = df.withColumn("SPEC_valid", transfer_unit_pandas_udf(df.SPEC_valid))
	df = df.withColumn("SPEC_gross", transfer_unit_pandas_udf(df.SPEC_gross))
	df = df.drop("SPEC_gross_digit", "SPEC_gross_unit", "SPEC_valid_digit", "SPEC_valid_unit")
	# df = df.withColumn("SPEC_percent", percent_pandas_udf(df.SPEC_percent, df.SPEC_valid, df.SPEC_gross))
	
	df = df.na.fill("")
	df.show()
	
	# 把百分号补充到有效成分列中
	# df = df.withColumn("SPEC_percent", lit(""))
	# df = df.withColumn("SPEC_gross", when(((df.SPEC_gross == "") & (df.SPEC_valid != "")), df.SPEC_valid).otherwise(df.SPEC_gross))
	# df = df.withColumn("SPEC_valid", when((df.SPEC_percent != ""), df.SPEC_percent).otherwise(df.SPEC_valid))
	# df = df.withColumn("SPEC_valid", when((df.SPEC_valid == df.SPEC_gross), lit("")).otherwise(df.SPEC_valid))
	
	# 拆分成四列
	# digit_regex_spec = r'(\d+\.?\d*e?-?\d*?)'
	
	# df = df.withColumn("SPEC_valid_digit", regexp_extract('SPEC_valid', digit_regex_spec, 1))
	# df = df.withColumn("SPEC_valid_unit", regexp_replace('SPEC_valid', digit_regex_spec, ""))
	
	# df = df.withColumn("SPEC_gross_digit", regexp_extract('SPEC_gross', digit_regex_spec, 1))
	# df = df.withColumn("SPEC_gross_unit", regexp_replace('SPEC_gross', digit_regex_spec, ""))
	# df = df.na.fill("")
	# df.show()
	
	unit_regex_spec = r'([A-Z]+\d*)'
	
	df = df.withColumn("SPEC_valid_unit", regexp_extract('SPEC_valid', unit_regex_spec, 1))
	df = df.withColumn("SPEC_valid_digit", regexp_replace('SPEC_valid', unit_regex_spec, ""))
	
	df = df.withColumn("SPEC_gross_unit", regexp_extract('SPEC_gross', unit_regex_spec, 1))
	df = df.withColumn("SPEC_gross_digit", regexp_replace('SPEC_gross', unit_regex_spec, ""))
	df = df.withColumn("SPEC_valid_total_ORIGINAL",  spec_valid_std_transfer_pandas_udf(df.SPEC_valid_digit))
	df = df.withColumn("SPEC_total_ORIGINAL",  spec_total_cleanning_pandas_udf(df.SPEC_valid_digit, df.SPEC_valid_unit, df.SPEC_gross_digit, df.SPEC_gross_unit))
	
	df = df.na.fill("")
	df.select("SPEC_valid_digit", "SPEC_valid_total_ORIGINAL", "SPEC_valid_digit_STANDARD", "SPEC_valid_total_STANDARD").show()
	# df.show()
	
	# 开始计算effectiveness的逻辑
	df = df.withColumn("EFFTIVENESS_SPEC_SPLIT", lit(0))
	# 1. 如果 【四列】分别都相等，则eff为1
	df = df.withColumn("EFFTIVENESS_SPEC_SPLIT", when( \
						((df.SPEC_valid_digit_STANDARD == df.SPEC_valid_digit) & (df.SPEC_valid_unit_STANDARD == df.SPEC_valid_unit) \
						& (df.SPEC_gross_digit_STANDARD == df.SPEC_gross_digit) & (df.SPEC_gross_unit_STANDARD == df.SPEC_gross_unit)), \
						lit(1))\
						.otherwise(df.EFFTIVENESS_SPEC_SPLIT))
	# df = df.where(df.EFFTIVENESS_SPEC_SPLIT == 0)
	
	# 2. 如果original/standard【只有valid/只有gross】，且仅有的部分是可以对应的，则eff为1
	df = df.withColumn("EFFTIVENESS_SPEC_SPLIT", when( \
						((df.SPEC_valid_digit == "") & (df.SPEC_valid_unit == "") \
						& ((df.SPEC_gross_digit_STANDARD == df.SPEC_gross_digit) & (df.SPEC_gross_unit_STANDARD == df.SPEC_gross_unit)) \
						| ((df.SPEC_valid_digit_STANDARD == df.SPEC_gross_digit) & (df.SPEC_valid_unit_STANDARD == df.SPEC_gross_unit))), \
						lit(1))\
						.otherwise(df.EFFTIVENESS_SPEC_SPLIT))
	df = df.withColumn("EFFTIVENESS_SPEC_SPLIT", when( \
						((df.SPEC_gross_digit == "") & (df.SPEC_gross_unit == "") \
						& ((df.SPEC_valid_digit_STANDARD == df.SPEC_valid_digit) & (df.SPEC_valid_unit_STANDARD == df.SPEC_valid_unit)) \
						| ((df.SPEC_gross_digit_STANDARD == df.SPEC_valid_digit) & (df.SPEC_gross_unit_STANDARD == df.SPEC_valid_unit))), \
						lit(1))\
						.otherwise(df.EFFTIVENESS_SPEC_SPLIT))
						
	# 3.如果【总量两列】 = 【有效成分两列】& 【有效成分两列】= 【总量两列】，则eff为1
	df = df.withColumn("EFFTIVENESS_SPEC_SPLIT", when( \
						((df.SPEC_valid_digit_STANDARD == df.SPEC_gross_digit) & (df.SPEC_valid_unit_STANDARD == df.SPEC_gross_unit) \
						& (df.SPEC_gross_digit_STANDARD == df.SPEC_valid_digit) & (df.SPEC_gross_unit_STANDARD == df.SPEC_valid_unit)), \
						lit(1))\
						.otherwise(df.EFFTIVENESS_SPEC_SPLIT))
						
	# 4. 如果【源数据valid == 标准数据valid之和】，则eff为1
	df = df.withColumn("EFFTIVENESS_SPEC_SPLIT", when( \
						((df.SPEC_valid_total_STANDARD == df.SPEC_valid_digit) & (df.SPEC_valid_unit_STANDARD == df.SPEC_valid_unit)), \
						lit(1))\
						.otherwise(df.EFFTIVENESS_SPEC_SPLIT))
	# df.withColumn("SPEC_valid_total_STANDARD", when(df.SPEC_valid_total_STANDARD == "nan", lit("")).otherwise(df.SPEC_valid_total_STANDARD))
	# df = df.where(df.SPEC == "CO 1.003 GM")
	# 5. 一些骚操作（目前是针对azsanofi的）：
	# 如果 【源数据有效成分 == 标准有效成分的取整值/四舍五入值】，则eff为0.99
	df = df.withColumn("EFFTIVENESS_SPEC_SPLIT", when(df.EFFTIVENESS_SPEC_SPLIT == 1, df.EFFTIVENESS_SPEC_SPLIT) \
												.otherwise(spec_eff_int_or_carry(df.SPEC_valid_digit_STANDARD, df.SPEC_valid_total_ORIGINAL, df.SPEC_valid_unit_STANDARD, \
																	df.SPEC_valid_unit, df.SPEC_valid_total_STANDARD, df.EFFTIVENESS_SPEC_SPLIT)))
	# df = df.withColumn("EFFTIVENESS_SPEC_SPLIT", when(df.SPEC.contains("162.5"), lit(0.999999)). \
	# 											otherwise(df.EFFTIVENESS_SPEC_SPLIT ))
	
	# 6. 如果【标准总量/标准有效成分 == 源数据总+有效】，则eff为1
	df = df.withColumn("EFFTIVENESS_SPEC_SPLIT", when( \
						(((df.SPEC_total_ORIGINAL == df.SPEC_gross_digit_STANDARD) & (df.SPEC_gross_unit == df.SPEC_gross_unit_STANDARD)) \
						| ((df.SPEC_total_ORIGINAL == df.SPEC_valid_digit_STANDARD) & (df.SPEC_valid_unit == df.SPEC_valid_unit_STANDARD))), \
						lit(1)) \
						.otherwise(df.EFFTIVENESS_SPEC_SPLIT))
	# df.show()
	# df.select("SPEC", "SPEC_STANDARD", "EFFTIVENESS_SPEC_SPLIT").show(25)
	
	return df


@pandas_udf(StringType(), PandasUDFType.SCALAR)
def spec_valid_std_transfer_pandas_udf(value):
	
	def digit_addition(spec_str):
		lst = spec_str.split(",")
		value_total = 0.0
		# if (lst[0] == "nan") | (len(lst) == 0):
		if len(lst) <= 1:
			value = "0.0"
		else:
			# try:
			for num in lst:
				num = float(num)
				value_total = (value_total + num)
				value  = str(value_total)
			# except:
			# 	value = 0.0
		return value
				
	frame = { "SPEC": value }
	df = pd.DataFrame(frame)
	df["RESULT"] = df["SPEC"].apply(digit_addition)
	return df["RESULT"]


@pandas_udf(StringType(), PandasUDFType.SCALAR)
def transfer_unit_pandas_udf(value):
	def unit_trans(value, unit):
		# value transform
		if unit == "G" or unit == "GM":
			value = value *1000
		elif unit == "UG" or unit == "UG/DOS":
			value = value /1000
		elif unit == "L":
			value = value *1000
		elif unit == "TU" or unit == "TIU":
			value = value *10000
		elif unit == "MU" or unit == "MIU" or unit == "M":
			value = value *1000000
		elif (unit == "Y"):
			value = value /1000
		if value >= 1:
			value = round(value, 1)
		else:
			value = value

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
				"Y": "MG",
				"MC": "MC",
			}
		try:
			unit = unit_switch[unit]
		except KeyError:
			pass
		return value, unit
	
	
	def unit_transform(spec_str):
		spec_str = spec_str.replace(" ", "")
		# 拆分数字和单位
		digit_regex = '\d+\.?\d*e?-?\d*?'
		# digit_regex = '0.\d*'
		# try:
		if spec_str != "":
			values = re.findall(digit_regex, spec_str)
			if len(values) == 1:
				value = values[0]
				unit = spec_str.strip(value)  # type = str
				value = float(value)  # type = float
				value = unit_trans(value, unit)[0]
				unit = unit_trans(value, unit)[1]
			elif len(values) >= 2:
				# unit = unit
				# value = 12222
				value_result = ""
				unit_regex = '[A-Z]+\d*'
				unit = re.findall(unit_regex, spec_str)[0]
				# value = "000"
				for value in values:
					value = float(value)  # type = float
					value = unit_trans(value, unit)[0]
					value_result = value_result + str(value) + ","
				unit = unit_trans(value, unit)[1]
				value = value_result.strip(",")
				
		else:
			unit = ""
			value = ""

		return str(value) + unit

		# except Exception:
		# 	return spec_str

	frame = { "SPEC": value }
	df = pd.DataFrame(frame)
	df["RESULT"] = df["SPEC"].apply(unit_transform)
	return df["RESULT"]
	
	
@pandas_udf(StringType(), PandasUDFType.SCALAR)
def spec_total_cleanning_pandas_udf(SPEC_valid_digit, SPEC_valid_unit, SPEC_gross_digit, SPEC_gross_unit):
	
	def digit_addition(a, b, valid_unit, gross_unit):
		if (valid_unit == gross_unit) & ("," not in a) & (a != ""):
			a = float(a)
			b = float(b)
			return str(a+b)
		else:
			return ""
				
	frame = { "SPEC_valid_digit": SPEC_valid_digit, "SPEC_valid_unit": SPEC_valid_unit,
			  "SPEC_gross_digit": SPEC_gross_digit, "SPEC_gross_unit": SPEC_gross_unit,	}
	df = pd.DataFrame(frame)
	df["RESULT"] = df.apply(lambda x: digit_addition(x["SPEC_valid_digit"], x["SPEC_gross_digit"], x["SPEC_valid_unit"], x["SPEC_gross_unit"]), axis=1)
	return df["RESULT"]


@pandas_udf(DoubleType(), PandasUDFType.SCALAR)
def spec_eff_int_or_carry(SPEC_valid_digit_STANDARD, SPEC_valid_total_ORIGINAL, SPEC_valid_unit_STANDARD, SPEC_valid_unit, SPEC_valid_total_STANDARD, EFFTIVENESS_SPEC_SPLIT):

	frame = { "SPEC_valid_digit_STANDARD": SPEC_valid_digit_STANDARD, "SPEC_valid_total_ORIGINAL": SPEC_valid_total_ORIGINAL,
			  "SPEC_valid_unit_STANDARD": SPEC_valid_unit_STANDARD,  "SPEC_valid_unit": SPEC_valid_unit, 
			  "SPEC_valid_total_STANDARD": SPEC_valid_total_STANDARD, "EFFTIVENESS_SPEC_SPLIT": EFFTIVENESS_SPEC_SPLIT}
	df = pd.DataFrame(frame)
	
	
	# try:
	df["EFFTIVENESS_SPEC_SPLIT"] = df.apply(lambda x: 0.99 if ((int(float(x["SPEC_valid_total_ORIGINAL"])) == int(float(x["SPEC_valid_total_STANDARD"]))) \
														& (x["SPEC_valid_unit_STANDARD"] == x["SPEC_valid_unit"])) \
													else 0.999 if ((math.ceil(float(x["SPEC_valid_total_ORIGINAL"])) == math.ceil(float(x["SPEC_valid_total_STANDARD"]))) \
														& (x["SPEC_valid_unit_STANDARD"] == x["SPEC_valid_unit"])) \
											else x["EFFTIVENESS_SPEC_SPLIT"], axis=1)
	# except ValueError:
	# 	df["EFFTIVENESS_SPEC_SPLIT"] = df.apply(lambda x: 0.888, axis=1)

	return df["EFFTIVENESS_SPEC_SPLIT"]
	
	
@pandas_udf(DoubleType(), PandasUDFType.SCALAR)
def prod_name_replace(eff_mole_name, eff_product_name, mole_name, prod_name_standard, eff_mole_dosage):
	frame = { "EFFTIVENESS_MOLE_NAME": eff_mole_name, "EFFTIVENESS_PRODUCT_NAME": eff_product_name,
			  "MOLE_NAME": mole_name, "PRODUCT_NAME_STANDARD": prod_name_standard, "EFF_MOLE_DOSAGE": eff_mole_dosage,}
	df = pd.DataFrame(frame)

	df["EFFTIVENESS_PROD"] = df.apply(lambda x: max((x["EFFTIVENESS_PRODUCT_NAME"]), \
								(jaro_winkler_similarity(x["MOLE_NAME"], x["PRODUCT_NAME_STANDARD"])), \
								(x["EFF_MOLE_DOSAGE"])), axis=1)

	return df["EFFTIVENESS_PROD"]

################-----------------------------------------------------################
	