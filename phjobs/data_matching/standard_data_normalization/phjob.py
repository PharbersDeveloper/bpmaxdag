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
from pyspark.sql.functions import split , count , when , lit
from pyspark.sql.functions import regexp_replace, upper, regexp_extract


def execute(**kwargs):
	"""
		please input your code below
		get spark session: spark = kwargs["spark"]()
	"""
	logger = phs3logger(kwargs["job_id"])
	spark = kwargs["spark"]()
	
	logger.info(kwargs)
	
###############----------input--------------------################
	path_master_prod = kwargs["path_master_prod"]
	path_standard_gross_unit = kwargs["path_standard_gross_unit"]
	path_for_replace_standard_dosage = kwargs["path_for_replace_standard_dosage"]
###############----------input--------------------################
	
###############----------output-------------------################
	job_id = get_job_id(kwargs)
	run_id = get_run_id(kwargs)
	result_path_prefix = get_result_path(kwargs, run_id, job_id)
	result_path = result_path_prefix + kwargs["standard_result"]
	origin_path = result_path_prefix + kwargs["standard_origin"]
###############----------output--------------------################

###########--------------load file----------------------- ################
	df_standard = load_standard_prod(spark, path_master_prod)
	df_standard.write.mode("overwrite").parquet(origin_path)
	df_standard_gross_unit  = load_standard_gross_unit(spark,path_standard_gross_unit)
	df_replace_standard_dosage  = load_replace_standard_dosage(spark,path_for_replace_standard_dosage) 
###########--------------load file----------------------- ################


#########--------------main function--------------------################# 

    #DOSAGE预处理
	df_standard = make_dosage_standardization(df_standard,df_replace_standard_dosage)
    #添加标准总量单位
	df_standard = add_standard_gross_unit(df_standard,df_standard_gross_unit)
    #SPEC数据预处理
	df_standard = pre_to_standardize_data(df_standard)
    #基于不同的总量单位进行SPEC数据提取
	df_standard = extract_useful_spec_data(df_standard)
    #数据提纯
	df_standard = make_spec_gross_and_valid_pure(df_standard)
    #单位归一化处理
	df_standard = make_spec_unit_standardization(df_standard)
   #组合成新SPEC
	df_standard = create_new_spec_col(df_standard)
	'''
	df_standard = spec_standify(df_standard)
	'''
	df_standard.write.mode("overwrite").parquet(result_path)
#########--------------main function--------------------################# 

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
	 

def load_standard_gross_unit(spark,path_standard_gross_unit):
	df_standard_gross_unit = spark.read.parquet(path_standard_gross_unit)
	return df_standard_gross_unit
    
    
def load_replace_standard_dosage(spark,path_for_replace_standard_dosage):
	df_replace_standard_dosage = spark.read.parquet(path_for_replace_standard_dosage)
	return df_replace_standard_dosage

    
def make_dosage_standardization(df_standard,df_replace_standard_dosage):
    #标准表DOSAGE中干扰项剔除
	replace_dosage_str = r'(([(（].*[)）])|(\s+))'
	df_standard = df_standard.withColumn("DOSAGE_STANDARD", regexp_replace(col("DOSAGE_STANDARD"),replace_dosage_str,""))\
							.dropna(subset="DOSAGE_STANDARD")
	df_standard = df_standard.withColumn("DOSAGE_STANDARD", when(col("DOSAGE_STANDARD") == "鼻喷剂","鼻用喷雾剂")\
							.when(col("DOSAGE_STANDARD") == "胶囊","胶囊剂")\
							.when(col("DOSAGE_STANDARD") == "阴道洗剂","洗剂")\
							.when(col("DOSAGE_STANDARD") == "混悬剂","干混悬剂")\
							.when(col("DOSAGE_STANDARD") == "颗粒","颗粒剂")\
							.when(col("DOSAGE_STANDARD") == "糖浆","糖浆剂")\
							.when(col("DOSAGE_STANDARD") == "泡腾颗粒","泡腾颗粒剂")\
							.otherwise(col("DOSAGE_STANDARD")))  

	return df_standard
    
def add_standard_gross_unit(df_standard,df_standard_gross_unit):
    
	df_standard_gross_unit_mg = df_standard_gross_unit.select('STANDARD_GROSS_UNIT_MG')\
													.withColumnRenamed('STANDARD_GROSS_UNIT_MG','DOSAGE_STANDARD')\
													.withColumn('STANDARD_GROSS_UNIT',lit('MG'))
	df_standard_gross_unit_ml = df_standard_gross_unit.select('STANDARD_GROSS_UNIT_ML')\
														.withColumnRenamed('STANDARD_GROSS_UNIT_ML','DOSAGE_STANDARD')\
														.withColumn('STANDARD_GROSS_UNIT',lit('ML'))
	df_standard_gross_unit_cm = df_standard_gross_unit.select('STANDARD_GROSS_UNIT_CM')\
													.withColumnRenamed('STANDARD_GROSS_UNIT_CM','DOSAGE_STANDARD')\
													.withColumn('STANDARD_GROSS_UNIT',lit('CM'))
	df_standard_gross_unit_pen = df_standard_gross_unit.select('STANDARD_GROSS_UNIT_PEN')\
														.withColumnRenamed('STANDARD_GROSS_UNIT_PEN','DOSAGE_STANDARD')\
														.withColumn('STANDARD_GROSS_UNIT',lit('喷'))
	df_standard_gross_unit_mm = df_standard_gross_unit.select('STANDARD_GROSS_UNIT_MM')\
													.withColumnRenamed('STANDARD_GROSS_UNIT_MM','DOSAGE_STANDARD')\
													.withColumn('STANDARD_GROSS_UNIT',lit('MM'))
	df_standard_gross_unit = df_standard_gross_unit_mg\
							.union(df_standard_gross_unit_ml)\
							.union(df_standard_gross_unit_cm)\
							.union(df_standard_gross_unit_pen)\
							.union(df_standard_gross_unit_mm)\
							.filter(col('DOSAGE_STANDARD').isNotNull())
	df_standard = df_standard.join(df_standard_gross_unit,df_standard.DOSAGE_STANDARD == df_standard_gross_unit.DOSAGE_STANDARD , 'left').drop(df_standard_gross_unit.DOSAGE_STANDARD)

	return df_standard


def pre_to_standardize_data(df_standard):
    #剔除SPEC中空格
	remove_spaces_spec = r'(\s+)'
	df_standard = df_standard.withColumn("SPEC_STANDARD", regexp_replace(col("SPEC_STANDARD"), remove_spaces_spec , ""))\
							.withColumn("SPEC_STANDARD", regexp_replace(col("SPEC_STANDARD"), r"(/DOS)" , "喷"))\
							.withColumn("SPEC_STANDARD", upper(col("SPEC_STANDARD")))
	return df_standard

def extract_useful_spec_data(df_standard):
    #总量数据的提取
	extract_spec_value_MG = r'(\d+\.?\d*(((GM)|(MU)|[MU]?G)|Y|(ΜG)|(PNA)))'
	extract_spec_value_ML = r'(\d+\.?\d*((M?L)|(PE)))'
	extract_spec_value_U = r'(\d+\.?\d*((I?U)|(TIU)))'
	extract_spec_value_PEN = r'(喷(\d+\.?\d*))'
	extract_spec_value_CM = r'(\d+\.?\d*(CM)?[×:*](\d+\.?\d*(CM)?)([×*](\d+\.?\d*(CM)?))?|(\d+\.?\d*(CM)))'
	extract_pure_spec_valid_value = r'(喷?(\d+\.?\d*)((M?L)|([MU]?G)|I?[U喷KY]|(C?M)))'
	df_standard = df_standard.withColumn('STANDARD_SPEC_GROSS_VALUE', when(col('STANDARD_GROSS_UNIT') == 'MG' , regexp_extract(col('SPEC_STANDARD'), extract_spec_value_MG, 1))\
															.when(col('STANDARD_GROSS_UNIT') == 'ML' , regexp_extract(col('SPEC_STANDARD'), extract_spec_value_ML, 1))\
															.when(col('STANDARD_GROSS_UNIT') == 'U' , regexp_extract(col('SPEC_STANDARD'), extract_spec_value_U, 1))\
															.when(col("STANDARD_GROSS_UNIT") == "喷" , regexp_extract(col('SPEC_STANDARD'), extract_spec_value_PEN, 1))\
															.when(col('STANDARD_GROSS_UNIT') == 'CM' , regexp_extract(col('SPEC_STANDARD'), extract_spec_value_CM, 1)))\
															.withColumn('STANDARD_SPEC_GROSS_VALUE', when( col('STANDARD_SPEC_GROSS_VALUE') == '', regexp_extract(col('SPEC_STANDARD'), extract_pure_spec_valid_value, 1))\
															.otherwise(col('STANDARD_SPEC_GROSS_VALUE')))

	print(  '标准表数据总数：' + str(df_standard.count()))
	print('匹配失败数据：' + ' ' + str(df_standard.filter(col('STANDARD_SPEC_GROSS_VALUE') == '').count()) ,  '匹配率:' +' ' +  str(( 1 - int(df_standard.filter(col('STANDARD_SPEC_GROSS_VALUE') == '').count()) / int(df_standard.count())) * 100) + '%' ) 
	df_standard.filter(col('STANDARD_SPEC_GROSS_VALUE') == '').groupBy("SPEC_STANDARD").agg(count(col("SPEC_STANDARD"))).show(200)
    
	return df_standard

def make_spec_gross_and_valid_pure(df_standard):
    
    #数据提纯
	extract_pure_spec_valid_value = r'(\d+\.?\d*)((MU)|(M?L)|([MU]?G)|I?[U喷KY]|(C?M))'
	df_standard = df_standard.withColumn("SPEC_GROSS_VALUE_PURE", regexp_extract(col("STANDARD_SPEC_GROSS_VALUE"),extract_pure_spec_valid_value,1))\
								.withColumn("SPEC_GROSS_UNIT_PURE", regexp_extract(col("STANDARD_SPEC_GROSS_VALUE"),extract_pure_spec_valid_value,2))
    
	return df_standard

def make_spec_unit_standardization(df_standard):

	df_standard = df_standard.withColumn("SPEC_GROSS_VALUE_PURE", col("SPEC_GROSS_VALUE_PURE").cast("double"))\
								.withColumn("SPEC_valid_digit_STANDARD", col("SPEC_valid_digit_STANDARD").cast("double"))
#总量数值归一化
	df_standard = df_standard.withColumn("SPEC_GROSS_VALUE_PURE",when(col("SPEC_GROSS_UNIT_PURE") == "G", col("SPEC_GROSS_VALUE_PURE")*int(1000))\
								.when(col("SPfEC_GROSS_UNIT_PURE") == "UG", col("SPEC_GROSS_VALUE_PURE")*int(0.001))\
								.when(col("SPEC_GROSS_UNIT_PURE") == "Y", col("SPEC_GROSS_VALUE_PURE")*int(0.001))\
								.when(col("SPEC_GROSS_UNIT_PURE") == "L", col("SPEC_GROSS_VALUE_PURE")*int(1000))\
								.when(col("SPEC_GROSS_UNIT_PURE") == "MU", col("SPEC_GROSS_VALUE_PURE")*int(1000000))\
								.when(col("SPEC_GROSS_UNIT_PURE") == "MIU", col("SPEC_GROSS_VALUE_PURE")*int(1000000))\
								.when(col("SPEC_GROSS_UNIT_PURE") == "K", col("SPEC_GROSS_VALUE_PURE")*int(1000))\
								.otherwise(col("SPEC_GROSS_VALUE_PURE")))
	df_standard = df_standard.withColumn("SPEC_GROSS_VALUE_PURE" , when(col("SPEC_GROSS_VALUE_PURE").isNull(),regexp_extract(col("SPEC_STANDARD"),r"喷(\d+\.?\d*)",1))\
								.otherwise(col("SPEC_GROSS_VALUE_PURE")))\
								.withColumn("SPEC_GROSS_VALUE_PURE", col("SPEC_GROSS_VALUE_PURE").cast("double"))
    
#删除辅助列print(df_standard.columns)
	df_standard =df_standard.withColumnRenamed("SPEC_STANDARD","SPEC_ORIGIN")\
							.withColumnRenamed("SPEC_GROSS_VALUE_PURE","SPEC_GROSS_VALUE_PURE_STANDARD")\
							.withColumnRenamed("STANDARD_GROSS_UNIT","SPEC_GROSS_UNIT_PURE_STANDARD")\
							.drop("SPEC_gross_digit_STANDARD","SPEC_gross_unit_STANDARD","SPEC_gross_unit_STANDARD","SPEC_GROSS_UNIT_PURE","STANDARD_SPEC_GROSS_VALUE")

	return df_standard


def create_new_spec_col(df_standard):
    
	df_standard = df_standard.withColumn("SPEC_STANDARD",concat_ws('/',col("SPEC_valid_digit_STANDARD"),col("SPEC_valid_unit_STANDARD"),col("SPEC_GROSS_VALUE_PURE_STANDARD"),col("SPEC_GROSS_UNIT_PURE_STANDARD")))
	col_list = ['MOLE_NAME_STANDARD', 'PRODUCT_NAME_STANDARD', 'DOSAGE_STANDARD', 'SPEC_STANDARD', 'MANUFACTURER_NAME_STANDARD','MANUFACTURER_NAME_EN_STANDARD', 'CORP_NAME_STANDARD','PACK_QTY_STANDARD', 'PACK_ID_STANDARD', 'SPEC_valid_digit_STANDARD', 'SPEC_valid_unit_STANDARD', 'SPEC_GROSS_VALUE_PURE_STANDARD','SPEC_GROSS_UNIT_PURE_STANDARD']

	df_standard = df_standard.select(col_list) 
	print(df_standard.columns)
	return df_standard


################-----------------------functions---------------------------################