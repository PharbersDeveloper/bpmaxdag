# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger
import uuid
import re
import pandas as pd
from pyspark.sql.functions import col , concat , concat_ws
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf, pandas_udf, PandasUDFType
from pyspark.sql.functions import split
from pyspark.sql.functions import regexp_replace, upper, regexp_extract 
from pyspark.sql.functions import when , lit


def execute(**kwargs):
	"""
		please input your code below
		get spark session: spark = kwargs["spark"]()
	"""
	logger = phs3logger(kwargs["job_id"])
	spark = kwargs["spark"]()

	logger.info(kwargs)

############-----------input-------------------------###################
	raw_data_path = kwargs["path_cleaning_data"]
	interfere_path = kwargs["path_human_interfere"]
	second_interfere_path = kwargs["path_second_human_interfere"]
	chc_gross_unit_path = kwargs["path_chc_gross_unit"]
############-----------input-------------------------###################

###########------------output------------------------###################
	job_id = get_job_id(kwargs)
	run_id = get_run_id(kwargs)
	result_path_prefix = get_result_path(kwargs, run_id, job_id)
	result_path = result_path_prefix + kwargs["cleaning_result"]
	origin_path = result_path_prefix + kwargs["cleaning_origin"]
#########--------------output------------------------####################

###########--------------load file----------------------- ################
	# 1. human interfere 与 数据准备
	df_cleanning = modify_pool_cleanning_prod(spark, raw_data_path)
	df_interfere = load_interfere_mapping(spark, interfere_path)
	df_second_interfere = load_second_interfere(spark,second_interfere_path)
	df_chc_gross_unit = loda_chc_gross_unit(spark,chc_gross_unit_path)
	df_cleanning.persist()
# 	df_cleanning.write.mode("overwrite").parquet(origin_path)
#########---------------load file------------------------################

   
    
#########--------------main function--------------------#################   

    #添加标准总量单位
	df_cleanning = add_chc_standard_gross_unit(df_cleanning,df_chc_gross_unit)
    #SPEC数据预处理
	df_cleanning = pre_to_standardize_data(df_cleanning)
    #基于不同的总量单位进行SPEC数据提取
	df_cleanning = extract_useful_spec_data(df_cleanning)
    
	'''
    #从spec中抽取pack_id
	df_cleanning = get_pack(df_cleanning)
	df_cleanning = human_interfere(df_cleanning, df_interfere)

	
	# TODO: 以后去掉
	df_cleanning = df_cleanning.withColumn("SPEC_ORIGINAL", df_cleanning.SPEC)
	df_cleanning = df_cleanning.withColumn("PRODUCT_NAME", split(df_cleanning.PRODUCT_NAME, "-")[0])
# 	df_cleanning = spec_standify(df_cleanning)  # 规格列规范
	df_cleanning = get_inter(df_cleanning,df_second_interfere)
# 	df_cleanning.write.mode("overwrite").parquet(result_path)
########------------main fuction-------------------------################

	'''

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



def load_second_interfere(spark,second_interfere_path):
    df_second_interfere = spark.read.parquet(second_interfere_path)
    return df_second_interfere
 
def loda_chc_gross_unit(spark,chc_gross_unit_path):
    df_chc_gross_unit = spark.read.parquet(chc_gross_unit_path)
    return df_chc_gross_unit

def add_chc_standard_gross_unit(df_cleanning,df_chc_gross_unit):

	df_chc_gross_unit_mg = df_chc_gross_unit.select('CHC_GROSS_UNIT_MG').withColumnRenamed('CHC_GROSS_UNIT_MG','DOSAGE')\
											.withColumn('CHC_GROSS_UNIT',lit('MG'))
	df_chc_gross_unit_ml = df_chc_gross_unit.select('CHC_GROSS_UNIT_ML').withColumnRenamed('CHC_GROSS_UNIT_ML','DOSAGE')\
											.withColumn('CHC_GROSS_UNIT',lit('ML'))
	df_chc_gross_unit_cm = df_chc_gross_unit.select('CHC_GROSS_UNIT_CM').withColumnRenamed('CHC_GROSS_UNIT_CM','DOSAGE')\
											.withColumn('CHC_GROSS_UNIT',lit('CM'))
	df_chc_gross_unit_pen = df_chc_gross_unit.select('CHC_GROSS_UNIT_PEN').withColumnRenamed('CHC_GROSS_UNIT_PEN','DOSAGE')\
											.withColumn('CHC_GROSS_UNIT',lit('喷'))

	df_chc_gross_unit = df_chc_gross_unit_mg.union(df_chc_gross_unit_ml).union(df_chc_gross_unit_cm).union(df_chc_gross_unit_pen).filter(col('DOSAGE').isNotNull())
	df_cleanning = df_cleanning.join(df_chc_gross_unit,df_cleanning.DOSAGE == df_chc_gross_unit.DOSAGE , 'left').drop(df_chc_gross_unit.DOSAGE)
	df_cleanning.show(100)
	return df_cleanning
   
    
def pre_to_standardize_data(df_cleanning):
	df_cleanning = df_cleanning.replace(" ", "")\
			.withColumn("SPEC", upper(col('SPEC')))\
			.withColumn("SPEC", regexp_replace("SPEC", r"(万单位)", "×10MG"))\
			.withColumn("SPEC", regexp_replace("SPEC", r"(μ)", "U"))\
			.withColumn("SPEC", regexp_replace("SPEC", r"(ΜG)", "MG"))\
			.withColumn("SPEC", regexp_replace("SPEC" , r"(×)", "x"))\
			.withColumn("SPEC", regexp_replace("SPEC" , r"((IU)|(AXAI?U))", "U"))\
			.withColumn("SPEC", regexp_replace("SPEC" , r"(MCI)", "MC"))\
			.withColumn("SPEC", regexp_replace("SPEC" , r"(M1)" ,"ML"))\
			.withColumn("SPEC", regexp_replace("SPEC", r"(揿|掀)", "喷"))\
			.withColumn("SPEC", regexp_replace("SPEC", r"(CM2)", "CM"))\
			.withColumn("SPEC", regexp_replace("SPEC", r"(∶)", ":"))
	df_cleanning.show(100)    
	return df_cleanning
 
    
def extract_useful_spec_data(df_cleanning):
    
    #总量数据的提取
	extract_spec_value_MG = r'(\d+\.?\d*(((GM)|[MU]?G)|Y|(ΜG)))'
	extract_spec_value_ML = r'(\d+\.?\d*(M?L))'
	extract_spec_value_U = r'(\d+\.?\d*((I?U)|(TIU)))'
	extract_spec_value_PEN = r'(\d+\.?\d*(喷))'
	extract_spec_value_CM = r'(\d+\.?\d*(CM)?[×:*](\d+\.?\d*(CM)?)([×*](\d+\.?\d*(CM)?))?|(\d+\.?\d*(CM)))'
	df_cleanning = df_cleanning.withColumn('SPEC_GROSS_VALUE', when(col('CHC_GROSS_UNIT') == 'MG' , regexp_extract(col('SPEC'), extract_spec_value_MG, 1))\
															.when(col('CHC_GROSS_UNIT') == 'ML' , regexp_extract(col('SPEC'), extract_spec_value_ML, 1))\
															.when(col('CHC_GROSS_UNIT') == 'U' , regexp_extract(col('SPEC'), extract_spec_value_U, 1))\
															.when(col('CHC_GROSS_UNIT') == '喷' , regexp_extract(col('SPEC'), extract_spec_value_PEN, 1))\
															.when(col('CHC_GROSS_UNIT') == 'CM' , regexp_extract(col('SPEC'), extract_spec_value_CM, 1)))\
															.withColumn('SPEC_GROSS_VALUE', when( col('SPEC_GROSS_VALUE') == '', 'Nomatch').otherwise(col('SPEC_GROSS_VALUE')))

# 								.otherwise('Nomatch'))


	df_cleanning.show(100)
	print(df_cleanning.count())
	print(df_cleanning.filter(col('SPEC_GROSS_VALUE') == 'Nomatch').count() , int(df_cleanning.filter(col('SPEC_GROSS_VALUE') == 'Nomatch').count()) / int(df_cleanning.count()) )   
    #有效性的提取
	extract_spec_valid_value_MG = r'(([:/]\d+.?\d*[UM]?G)|(\d+.?\d*[MΜ]?G[×/](?![M]G))|(每(?!\d+.?\d*[MΜ]?G).*?(\d+.?\d*[MΜ]?G)))'
	extract_spec_valid_value_ML = r'([:]\d+.?\d*(([UM]?G(?![:]))|U)|(\d+.?\d*ΜG/ML)|((\d+.?\d*)ML×\d{1,2})|((\d+.?\d*)ML(?![:(/,含的中)])))'
	extract_spec_valid_value_U = r'(:?(\d+\.?\d*)((MIU)|([UKM](?![LG])U?)))'
	extract_spec_valid_value_PEN = r'((\d+\.?\d*)([Μ]G[/]喷))'
	extract_spec_valid_value_CM = r'(\d+\.?\d*(CM)?[×:*](\d+\.?\d*(CM)?)([×*](\d+\.?\d*(CM)?))?|(\d+\.?\d*(CM)))' 
	df_cleanning = df_cleanning.withColumn("SPEC_VALID_VALUE", when(col('CHC_GROSS_UNIT') == 'MG' , regexp_extract(col('SPEC'), extract_spec_valid_value_MG, 1))\
															.when(col('CHC_GROSS_UNIT') == 'ML' , regexp_extract(col('SPEC'), extract_spec_valid_value_ML, 1))\
															.when(col('CHC_GROSS_UNIT') == 'U' , regexp_extract(col('SPEC'), extract_spec_valid_value_U, 1))\
															.when(col('CHC_GROSS_UNIT') == '喷' , regexp_extract(col('SPEC'), extract_spec_valid_value_PEN, 1))\
															.when(col('CHC_GROSS_UNIT') == 'CM' , regexp_extract(col('SPEC'), extract_spec_valid_value_CM, 1))\
																				)
	df_cleanning.show(500)   
	print(df_cleanning.printSchema())
	return df_cleanning
    
    

# """
# 规格列规范
# """
# def spec_standify(df):
# 	# df = df.withColumn("SPEC_ORIGINAL", df.SPEC)
# # 	spec_valid_regex = r'([0-9]\d*\.?\d*\s*[A-Za-z]*/?\s*[A-Za-z]+)'
# # 	spec_gross_regex = r'([0-9]\d*\.?\d*\s*[A-Za-z]*/?\s*[A-Za-z]+)[ ,/:∶+\s][\u4e00-\u9fa5]*([0-9]\d*\.?\d*\s*[A-Za-z]*/?\s*[A-Za-z]+)'
# 	spec_valid_regex = r'(\d+\.?\d*(((GM)|[MU]?G)|Y|(ΜG)))'
# 	spec_gross_regex =  r'(\d+\.?\d*(M?L))'
# 	spec_other_regex =  r'((\d+\.?\d*(([TM]IU)|(AXAI?U)|([ITM]?U(?!G))|(M[CM]?(?![GL]))|CM2?))|((CO)|(喷)))'
# 	df = df.withColumn("SPEC", upper(df.SPEC))\
# 			.withColumn("SPEC", regexp_replace("SPEC", r"(万)", "T"))\
# 			.withColumn("SPEC", regexp_replace("SPEC", r"(μ)", "U"))\
# 			.withColumn("SPEC", regexp_replace("SPEC", r"(ΜG)", "MG"))\
# 			.replace(" ", "")\
# 			.withColumn("SPEC_percent", regexp_extract('SPEC', r'(\d+\.?\d*%)', 1))\
# 			.withColumn("SPEC_valid", regexp_extract('SPEC', spec_valid_regex, 1))\
# 			.withColumn("SPEC_gross", regexp_extract('SPEC', spec_gross_regex, 1))\
# 			.withColumn("SPEC_other_unit",regexp_extract('SPEC', spec_other_regex,1))\
# 			.na.fill("")
    
# # 	df = df.withColumn("SPEC_percent", percent_pandas_udf(df.SPEC_percent, df.SPEC_valid, df.SPEC_gross))    
# 	df = df.withColumn("SPEC_valid", transfer_unit_pandas_udf(df.SPEC_valid))
# 	df = df.withColumn("SPEC_gross", transfer_unit_pandas_udf(df.SPEC_gross))
# # 	df = df.withColumn("SPEC_other_unit", transfer_unit_pandas_udf(df.SPEC_other_unit))
# 	df = df.withColumn("SPEC", concat_ws('/',df.SPEC_percent ,df.SPEC_valid , df.SPEC_gross , df.SPEC_other_unit))\
# 			.drop("SPEC_percent", "SPEC_valid", "SPEC_gross", "SPEC_other_unit")
# 	return df

	
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
		digit_regex = '\d+\.?\d*'
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
		digit_regex = '\d+\.?\d*'
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

	
	
def human_interfere(df_cleanning, df_interfere):
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

def get_inter(df_cleanning,df_second_interfere):
	df_cleanning = df_cleanning.join(df_second_interfere, df_cleanning.MOLE_NAME == df_second_interfere.MOLE_NAME_LOST, 'left')
	df_cleanning = df_cleanning.withColumn('new', when(df_cleanning.MOLE_NAME_LOST.isNull(), df_cleanning.MOLE_NAME)\
											.otherwise(df_cleanning.MOLE_NAME_STANDARD))\
											.drop("MOLE_NAME", "MOLE_NAME_LOST", "MOLE_NAME_STANDARD")\
											.withColumnRenamed("new", "MOLE_NAME")\
											.select(['id','PACK_ID_CHECK','MOLE_NAME','PRODUCT_NAME','DOSAGE','SPEC','PACK_QTY','MANUFACTURER_NAME','SPEC_ORIGINAL'])
	return df_cleanning


#抽取spec中pack_id数据
def get_pack(df_cleanning):
	df_cleanning = df_cleanning.withColumnRenamed('PACK_QTY','PACK_QTY_ORIGIN').drop('PACK_QTY')\
						.withColumn('PACK_QTY',regexp_extract(df_cleanning.SPEC,'[××*](\d{1,3})',1).cast('float'))
	df_cleanning = df_cleanning.withColumn('PACK_QTY',when(df_cleanning.PACK_QTY.isNull(), df_cleanning.PACK_QTY_ORIGIN).otherwise(df_cleanning.PACK_QTY))
	return df_cleanning

################-----------------------------------------------------################