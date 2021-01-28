# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger
import uuid
import re
import pandas as pd
from pyspark.sql.functions import col , concat_ws
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf, pandas_udf, PandasUDFType
from pyspark.sql.functions import split
from pyspark.sql.functions import regexp_replace, upper, regexp_extract


def execute(**kwargs):
	"""
		please input your code below
		get spark session: spark = kwargs["spark"]()
	"""
	logger = phs3logger(kwargs["job_id"])
	spark = kwargs["spark"]()
	
	logger.info(kwargs)
	
	# input
	path_master_prod = kwargs["path_master_prod"]
	
	# output
	job_id = get_job_id(kwargs)
	run_id = get_run_id(kwargs)
	result_path_prefix = get_result_path(kwargs, run_id, job_id)
	result_path = result_path_prefix + kwargs["standard_result"]
	origin_path = result_path_prefix + kwargs["standard_origin"]

	df_standard = load_standard_prod(spark, path_master_prod)
	df_standard.write.mode("overwrite").parquet(origin_path)
	df_standard = spec_standify(df_standard)
	df_standard.write.mode("overwrite").parquet(result_path)
	
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


"""
读取标准表WW
"""
def load_standard_prod(spark, standard_prod_path):
	 df_standard = spark.read.parquet(standard_prod_path) \
					.select("PACK_ID",
							"MOLE_NAME_CH", "MOLE_NAME_EN",
							"PROD_DESC", "PROD_NAME_CH",
							"CORP_NAME_EN", "CORP_NAME_CH", "MNF_NAME_EN", "MNF_NAME_CH",
							"PCK_DESC", "DOSAGE", "SPEC", "PACK", 
							"SPEC_valid_digit", "SPEC_valid_unit", 
							"SPEC_gross_digit", "SPEC_gross_unit")
					# .drop("version")

	 df_standard = df_standard.withColumnRenamed("PACK_ID", "PACK_ID_STANDARD") \
					.withColumnRenamed("MOLE_NAME_CH", "MOLE_NAME_STANDARD") \
					.withColumnRenamed("PROD_NAME_CH", "PRODUCT_NAME_STANDARD") \
					.withColumnRenamed("CORP_NAME_CH", "CORP_NAME_STANDARD") \
					.withColumnRenamed("MNF_NAME_CH", "MANUFACTURER_NAME_STANDARD") \
					.withColumnRenamed("MNF_NAME_EN", "MANUFACTURER_NAME_EN_STANDARD") \
					.withColumnRenamed("DOSAGE", "DOSAGE_STANDARD") \
					.withColumnRenamed("SPEC", "SPEC_STANDARD") \
					.withColumnRenamed("PACK", "PACK_QTY_STANDARD") \
					.withColumnRenamed("SPEC_valid_digit", "SPEC_valid_digit_STANDARD") \
					.withColumnRenamed("SPEC_valid_unit", "SPEC_valid_unit_STANDARD") \
					.withColumnRenamed("SPEC_gross_digit", "SPEC_gross_digit_STANDARD") \
					.withColumnRenamed("SPEC_gross_unit", "SPEC_gross_unit_STANDARD")

	 df_standard = df_standard.select("PACK_ID_STANDARD", "MOLE_NAME_STANDARD",
										"PRODUCT_NAME_STANDARD", "CORP_NAME_STANDARD",
										"MANUFACTURER_NAME_STANDARD", "MANUFACTURER_NAME_EN_STANDARD",
										"DOSAGE_STANDARD", "SPEC_STANDARD", "PACK_QTY_STANDARD", 
										"SPEC_valid_digit_STANDARD", "SPEC_valid_unit_STANDARD", 
										"SPEC_gross_digit_STANDARD", "SPEC_gross_unit_STANDARD")

	 return df_standard
	 

"""
规格列规范
"""
# def spec_standify(df):
    
# 	# df = df.withColumn("SPEC_ORIGINAL", df.SPEC)
# # 	spec_valid_regex = r'([0-9]\d*\.?\d*\s*[A-Za-z]*/?\s*[A-Za-z]+)'
# # 	spec_gross_regex = r'([0-9]\d*\.?\d*\s*[A-Za-z]*/?\s*[A-Za-z]+)[ ,/:∶+\s][\u4e00-\u9fa5]*([0-9]\d*\.?\d*\s*[A-Za-z]*/?\s*[A-Za-z]+)'
# 	spec_valid_regex = r'(\d{0,}[.]{0,1}\d+[MU]{0,1}G|\d{0,}[.]{0,1}\d+[ITM]U[G]{0,1}|\d{0,}[.]{0,1}\d+(AXAIU)|\d{0,}[.]{0,1}\d+(AXAU)|\d{0,}[.]{0,1}\d+(TIU)|\d{0,}[.]{0,1}\d+[Y])'
# 	spec_gross_regex =  r'(\d{0,}[.]{0,1}\d+[M]{0,1}L|\d{0,}[.]{0,1}\d+[ITM]U[G]{0,1}|\d{0,}[.]{0,1}\d+[CM]M)'
# # 	spec_third_regex = r'([0-9]\d*\.?\d*\s*[A-Za-z]*/?\s*[A-Za-z]+)[ /:∶+\s]([0-9]\d*\.?\d*\s*[A-Za-z]*/?\s*[A-Za-z]+)[ /:∶+\s]([0-9]\d*\.?\d*\s*[A-Za-z]*/?\s*[A-Za-z]+)'
# 	df = df.withColumn("SPEC_STANDARD_ORIGINAL", df.SPEC_STANDARD)\
# 			.withColumn("SPEC_STANDARD", upper(df.SPEC_STANDARD))\
# 			.withColumn("SPEC_STANDARD", regexp_replace("SPEC_STANDARD", r"(万)", "T"))\
# 			.withColumn("SPEC_STANDARD", regexp_replace("SPEC_STANDARD", r"(μ)", "U"))\
# 			.withColumn("SPEC_STANDARD", regexp_replace("SPEC_STANDARD", r"(ΜG)", "MG"))\
# 			.replace(" ", "")\
# 			.withColumn("SPEC_percent", regexp_extract('SPEC_STANDARD', r'(\d{1,3}[.]{0,1}\d+%)', 1))\
# 			.withColumn("SPEC_valid", regexp_extract('SPEC_STANDARD', spec_valid_regex, 1))\
# 			.withColumn("SPEC_gross", regexp_extract('SPEC_STANDARD', spec_gross_regex, 1))\
# 			.na.fill("")

# # 	df = df.withColumn("SPEC_percent", percent_pandas_udf(df.SPEC_percent, df.SPEC_valid, df.SPEC_gross))
# 	df = df.withColumn("SPEC_valid", transfer_unit_pandas_udf(df.SPEC_valid))
# 	df = df.withColumn("SPEC_gross", transfer_unit_pandas_udf(df.SPEC_gross))
# 	df = df.withColumn("SPEC_STANDARD", concat_ws('/',df.SPEC_percent ,df.SPEC_valid , df.SPEC_gross))\
# 			.drop("SPEC_percent", "SPEC_valid", "SPEC_gross")
# 	return df


def spec_standify(df):

	spec_valid_regex = r'(\d+\.?\d*(((GM)|[MU]?G)|Y|(ΜG)))'
	spec_gross_regex =  r'(\d+\.?\d*(M?L))'
	spec_other_regex =  r'((\d+\.?\d*(([TM]IU)|(AXAI?U)|([ITM]?U(?!G))|(M[CM]?(?![GL]))|CM2?))|((CO)|(喷)))'
	df = df.withColumn("SPEC_STANDARD_ORIGINAL", df.SPEC_STANDARD)\
			.withColumn("SPEC_STANDARD", upper(df.SPEC_STANDARD))\
			.withColumn("SPEC_STANDARD", regexp_replace("SPEC_STANDARD", r"(万)", "T"))\
			.withColumn("SPEC_STANDARD", regexp_replace("SPEC_STANDARD", r"(μ)", "U"))\
			.withColumn("SPEC_STANDARD", regexp_replace("SPEC_STANDARD", r"(ΜG)", "MG"))\
			.replace(" ", "")\
			.withColumn("SPEC_percent", regexp_extract("SPEC_STANDARD", r'(\d+\.?\d*%)', 1))\
			.withColumn("SPEC_valid", regexp_extract("SPEC_STANDARD", spec_valid_regex, 1))\
			.withColumn("SPEC_gross", regexp_extract("SPEC_STANDARD", spec_gross_regex, 1))\
			.withColumn("SPEC_other_unit",regexp_extract("SPEC_STANDARD", spec_other_regex,1))\
			.na.fill("")
    
# 	df = df.withColumn("SPEC_percent", percent_pandas_udf(df.SPEC_percent, df.SPEC_valid, df.SPEC_gross))    
	df = df.withColumn("SPEC_valid", transfer_unit_pandas_udf(df.SPEC_valid))
	df = df.withColumn("SPEC_gross", transfer_unit_pandas_udf(df.SPEC_gross))
# 	df = df.withColumn("SPEC_other_unit", transfer_unit_pandas_udf(df.SPEC_other_unit))
	df = df.withColumn("SPEC_STANDARD", concat_ws('/',df.SPEC_percent ,df.SPEC_valid , df.SPEC_gross , df.SPEC_other_unit))\
			.drop("SPEC_percent", "SPEC_valid", "SPEC_gross", "SPEC_other_unit")
	return df



	
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
def percent_pandas_udf(percent, valid, gross):
	def percent_calculation(percent, valid, gross):
		digit_regex = '\d+\.?\d*e?-?\d*?'
		if percent != "" and valid != "" and gross == "":
			num = float(percent.strip("%"))
			value = re.findall(digit_regex, valid)[0]
			unit = valid.strip(value)  # type = str
			if unit == "ML":
				final_num = round(num*float(value)*10, 3)
				result = str(final_num) + "MG"
			elif unit == "MG":
				final_num = num*float(value)*0.01
				result = str(final_num) + "MG"
			else:
				result = unit

		elif percent != "" and valid!= "" and gross != "":
			result = ""

		else:
			result = percent
		return result

	frame = { "percent": percent, "valid": valid, "gross": gross }
	df = pd.DataFrame(frame)
	df["RESULT"] = df.apply(lambda x: percent_calculation(x["percent"], x["valid"], x["gross"]), axis=1)
	return df["RESULT"]
################-----------------------------------------------------################