# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger
import uuid
import re
import pandas as pd
from pyspark.sql.functions import col, lit, concat
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf, pandas_udf, PandasUDFType
from pyspark.sql.functions import split
from pyspark.sql.functions import regexp_replace, upper, regexp_extract
from pyspark.sql.functions import when


def execute(**kwargs):
	"""
		please input your code below
		get spark session: spark = kwargs["spark"]()
	"""
	logger = phs3logger(kwargs["job_id"])
	spark = kwargs["spark"]()

	logger.info(kwargs)

	# input
	raw_data_path = kwargs["path_cleaning_data"]
	interfere_path = kwargs["path_human_interfere"]

	# output
	job_id = get_job_id(kwargs)
	run_id = get_run_id(kwargs)
	result_path_prefix = get_result_path(kwargs, run_id, job_id)
	result_path = result_path_prefix + kwargs["cleaning_result"]
	origin_path = result_path_prefix + kwargs["cleaning_origin"]

	# 1. human interfere 与 数据准备
	df_cleanning = modify_pool_cleanning_prod(spark, raw_data_path)
	df_cleanning.persist()
	df_cleanning.write.mode("overwrite").parquet(origin_path)
    #从spec中抽取pack_id
	df_cleanning_id = get_pack(df_cleanning)
	df_interfere = load_interfere_mapping(spark, interfere_path)
	df_cleanning = human_interfere(spark, df_cleanning, df_interfere)

	
	# TODO: 以后去掉
	df_cleanning = df_cleanning.withColumn("SPEC_ORIGINAL", df_cleanning.SPEC)
	df_cleanning = df_cleanning.withColumn("PRODUCT_NAME", split(df_cleanning.PRODUCT_NAME, "-")[0])

	# df_cleanning = dosage_standify(df_cleanning)  # 剂型列规范
	df_cleanning = spec_standify(df_cleanning)  # 规格列规范
	df_cleanning = get_inter(spark,df_cleanning)
	df_cleanning.write.mode("overwrite").parquet(result_path)
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
更高的并发数
"""
def modify_pool_cleanning_prod(spark, raw_data_path):
	if raw_data_path.endswith(".csv"):
		return spark.read.csv(path=raw_data_path, header=True).withColumn("id", pudf_id_generator(col("MOLE_NAME")))
	else:
		return spark.read.parquet(raw_data_path).withColumn("id", pudf_id_generator(col("MOLE_NAME")))
	

@pandas_udf(StringType(), PandasUDFType.SCALAR)
def pudf_id_generator(oid):
	frame = {
		"_ID": oid
	}
	df = pd.DataFrame(frame)
	df["RESULT"] = df["_ID"].apply(lambda x: str(uuid.uuid4()))
	return df["RESULT"]


"""
规格列规范
"""
def spec_standify(df):
	# df = df.withColumn("SPEC_ORIGINAL", df.SPEC)
	df = df.withColumn("SPEC", regexp_replace("SPEC", r"(万)", "T"))
	df = df.withColumn("SPEC", regexp_replace("SPEC", r"(μ)", "U"))
	df = df.withColumn("SPEC", upper(df.SPEC))
	df = df.replace(" ", "")
	# df = df.withColumn("SPEC_gross", regexp_extract('SPEC', spec_regex, 2))
	# 拆分规格的成分s
	df = df.withColumn("SPEC_percent", regexp_extract('SPEC', r'(\d+%)', 1))
	df = df.withColumn("SPEC_co", regexp_extract('SPEC', r'(CO)', 1))
	spec_valid_regex =  r'([0-9]\d*\.?\d*\s*[A-Za-z]*/?\s*[A-Za-z]+)'
	df = df.withColumn("SPEC_valid", regexp_extract('SPEC', spec_valid_regex, 1))
	spec_gross_regex =  r'([0-9]\d*\.?\d*\s*[A-Za-z]*/?\s*[A-Za-z]+)[ ,/:∶+\s][\u4e00-\u9fa5]*([0-9]\d*\.?\d*\s*[A-Za-z]*/?\s*[A-Za-z]+)'
	df = df.withColumn("SPEC_gross", regexp_extract('SPEC', spec_gross_regex, 2))
	spec_third_regex = r'([0-9]\d*\.?\d*\s*[A-Za-z]*/?\s*[A-Za-z]+)[ /:∶+\s]([0-9]\d*\.?\d*\s*[A-Za-z]*/?\s*[A-Za-z]+)[ /:∶+\s]([0-9]\d*\.?\d*\s*[A-Za-z]*/?\s*[A-Za-z]+)'
	df = df.withColumn("SPEC_third", regexp_extract('SPEC', spec_third_regex, 3))


	pure_number_regex_spec = r'(\s\d+$)'
	df = df.withColumn("SPEC_pure_number", regexp_extract('SPEC', pure_number_regex_spec, 1))

	digit_regex_spec = r'(\d+\.?\d*e?-?\d*?)'
	df = df.withColumn("SPEC_gross_digit", regexp_extract('SPEC_gross', digit_regex_spec, 1))
	df = df.withColumn("SPEC_gross_unit", regexp_replace('SPEC_gross', digit_regex_spec, ""))
	df = df.withColumn("SPEC_valid_digit", regexp_extract('SPEC_valid', digit_regex_spec, 1))
	df = df.withColumn("SPEC_valid_unit", regexp_replace('SPEC_valid', digit_regex_spec, ""))
	df = df.na.fill("")
	df = df.withColumn("SPEC_valid", transfer_unit_pandas_udf(df.SPEC_valid))
	df = df.withColumn("SPEC_gross", transfer_unit_pandas_udf(df.SPEC_gross))
	df = df.drop("SPEC_gross_digit", "SPEC_gross_unit", "SPEC_valid_digit", "SPEC_valid_unit")
	df = df.withColumn("SPEC_percent", percent_pandas_udf(df.SPEC_percent, df.SPEC_valid, df.SPEC_gross))
	df = df.withColumn("SPEC_ept", lit("/"))
	df = df.withColumn("SPEC", concat( "SPEC_percent", "SPEC_ept", "SPEC_valid", "SPEC_ept", "SPEC_gross", "SPEC_ept", "SPEC_third")) \
					.drop("SPEC_ept", "SPEC_percent", "SPEC_co", "SPEC_valid", "SPEC_gross", "SPEC_pure_number", "SPEC_third")
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
	

"""
读取人工干预表
"""
def load_interfere_mapping(spark, human_replace_packid_path):
	 df_interfere = spark.read.parquet(human_replace_packid_path) \
						 .withColumnRenamed("match_MOLE_NAME_CH", "MOLE_NAME_INTERFERE") \
						 .withColumnRenamed("match_PRODUCT_NAME", "PRODUCT_NAME_INTERFERE")  \
						 .withColumnRenamed("match_SPEC", "SPEC_INTERFERE") \
						 .withColumnRenamed("match_DOSAGE", "DOSAGE_INTERFERE") \
						 .withColumnRenamed("match_PACK_QTY", "PACK_QTY_INTERFERE") \
						 .withColumnRenamed("match_MANUFACTURER_NAME_CH", "MANUFACTURER_NAME_INTERFERE") \
						 .withColumnRenamed("PACK_ID", "PACK_ID_INTERFERE")
	 return df_interfere

	
	
def human_interfere(spark, df_cleanning, df_interfere):
	 # 1. 人工干预优先，不太对后期改
	 # 干预流程将数据直接替换，在走平常流程，不直接过滤，保证流程的统一性
	 df_cleanning = df_cleanning.withColumn("min", concat(df_cleanning["MOLE_NAME"], df_cleanning["PRODUCT_NAME"], df_cleanning["SPEC"], \
										df_cleanning["DOSAGE"], df_cleanning["PACK_QTY"], df_cleanning["MANUFACTURER_NAME"]))

	 # 2. join 干预表，替换原有的原始数据列
	 df_cleanning = df_cleanning.join(df_interfere, on="min",  how="left") \
					.na.fill({
							"MOLE_NAME_INTERFERE": "unknown",
							"PRODUCT_NAME_INTERFERE": "unknown",
							"SPEC_INTERFERE": "unknown",
							"DOSAGE_INTERFERE": "unknown",
							"PACK_QTY_INTERFERE": "unknown",
							"MANUFACTURER_NAME_INTERFERE": "unknown"})

	 df_cleanning = df_cleanning.withColumn("MOLE_NAME", interfere_replace_udf(df_cleanning.MOLE_NAME, df_cleanning.MOLE_NAME_INTERFERE)) \
					.withColumn("PRODUCT_NAME", interfere_replace_udf(df_cleanning.PRODUCT_NAME, df_cleanning.PRODUCT_NAME_INTERFERE)) \
					.withColumn("SPEC", interfere_replace_udf(df_cleanning.SPEC, df_cleanning.SPEC_INTERFERE)) \
					.withColumn("DOSAGE", interfere_replace_udf(df_cleanning.DOSAGE, df_cleanning.DOSAGE_INTERFERE)) \
					.withColumn("PACK_QTY", interfere_replace_udf(df_cleanning.PACK_QTY, df_cleanning.PACK_QTY_INTERFERE)) \
					.withColumn("MANUFACTURER_NAME", interfere_replace_udf(df_cleanning.MANUFACTURER_NAME, df_cleanning.MANUFACTURER_NAME_INTERFERE))

	 df_cleanning = df_cleanning.select("id", "PACK_ID_CHECK", "MOLE_NAME", "PRODUCT_NAME", "DOSAGE", "SPEC", "PACK_QTY", "MANUFACTURER_NAME")
	 # df_cleanning.persist()

	 return df_cleanning
	 

@udf(returnType=StringType())
def interfere_replace_udf(origin, interfere):
	if interfere != "unknown":
		origin = interfere
	return origin

def get_inter(spark,df_cleanning):
	df_inter = spark.read.parquet("s3a://ph-max-auto/2020-08-11/data_matching/refactor/data/DF_CONF")
	df_cleanning = df_cleanning.join(df_inter, df_cleanning.MOLE_NAME == df_inter.MOLE_NAME_LOST, 'left')
	df_cleanning = df_cleanning.withColumn('new', when(df_cleanning.MOLE_NAME_LOST.isNull(), df_cleanning.MOLE_NAME)\
											.otherwise(df_cleanning.MOLE_NAME_STANDARD))\
											.drop("MOLE_NAME", "MOLE_NAME_LOST", "MOLE_NAME_STANDARD")\
											.withColumnRenamed("new", "MOLE_NAME")\
									.select(['id','PACK_ID_CHECK','MOLE_NAME','PRODUCT_NAME','DOSAGE','SPEC','PACK_QTY','MANUFACTURER_NAME','SPEC_ORIGINAL'])
	return df_cleanning


#抽取spec中pack_id数据
def get_pack(df_cleanning):
	df_cleanning = df_cleanning.withColumnRenamed('PACK_QTY','PACK_QTY_ORIGIN').drop('PACK_QTY')\
						.withColumn('PACK_QTY',regexp_extract(df_cleanning.SPEC,'[×*](\d{1,3})',1).cast('float'))
	df_cleanning = df_cleanning.withColumn('PACK_QTY',when(df_cleanning.PACK_QTY.isNull(), df_cleanning.PACK_QTY_ORIGIN).otherwise(df_cleanning.PACK_QTY))
	return df_cleanning



################-----------------------------------------------------################