# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger
import math
from math import isnan, sqrt
import uuid
import re
import pandas as pd
import numpy as np
from pyspark.sql.types import *
from pyspark.sql.functions import regexp_replace, regexp_extract
from pyspark.sql.functions import when, explode, concat, upper, lit ,col
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

#################-------------------input------------------###################

	depends = get_depends_path(kwargs)
	path_effective_result = depends["input"]
	path_dosage_mapping_path = kwargs["dosage_mapping_path"]
	g_repartition_shared = int(kwargs["g_repartition_shared"])
##################-------------------input------------------###################

##################-----------------output------------------------###################
	job_id = get_job_id(kwargs)
	run_id = get_run_id(kwargs)
	result_path_prefix = get_result_path(kwargs, run_id, job_id)
	result_path = result_path_prefix + kwargs["spec_adjust_result"]
	mid_path = result_path_prefix + kwargs["spec_adjust_mid"]
##################-----------------output------------------------###################

#################----------loading files---------------######################
	df_second_round = load_effective_result(spark, path_effective_result)
	df_dosage_mapping = load_dosage_mapping(spark, path_dosage_mapping_path)
################-----------loading files---------------######################


###################--------------main function------------------------#################  
	# 6. 第二轮更改优化eff的计算方法
	df_second_round = second_round_with_col_recalculate(df_second_round, df_dosage_mapping)
	df_second_round = recalculation_spec_effectiveness(df_second_round)
	df_second_round.repartition(g_repartition_shared).write.mode("overwrite").parquet(mid_path)
	# spec拆列之后的匹配算法
	'''
	df_second_round = spec_split_matching(df_second_round)
	'''
    #选取指定的列用于和adjust_mnf job 进行union操作
	df_second_round = select_specified_cols(df_second_round)
	df_second_round.repartition(g_repartition_shared).write.mode("overwrite").parquet(result_path)
###################-----------------main function-------------------#################  

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

def load_effective_result(spark, path_effective_result):
	print(path_effective_result)
	df_second_round = spark.read.parquet(path_effective_result)  
	return df_second_round

def load_dosage_mapping(spark, path_dosage_mapping_path):  
	df_dosage_mapping = spark.read.parquet(path_dosage_mapping_path)
	return df_dosage_mapping


def second_round_with_col_recalculate(df_second_round, dosage_mapping):
	df_second_round = df_second_round.join(dosage_mapping, "DOSAGE", how="left").na.fill("")
	df_second_round = df_second_round.withColumn("MASTER_DOSAGE", when(df_second_round.MASTER_DOSAGE.isNull(), df_second_round.JACCARD_DISTANCE).otherwise(df_second_round.MASTER_DOSAGE))
	df_second_round = df_second_round.withColumn("EFFTIVENESS_DOSAGE_SE", dosage_replace(df_second_round.MASTER_DOSAGE , df_second_round.DOSAGE_STANDARD, df_second_round.EFFTIVENESS_DOSAGE))
	df_second_round = mole_dosage_calculaltion(df_second_round)   # 加一列EFF_MOLE_DOSAGE，doubletype
	df_second_round = df_second_round.withColumn("EFFTIVENESS_PRODUCT_NAME_SE", \
							prod_name_replace(df_second_round.EFFTIVENESS_MOLE_NAME, 
											df_second_round.EFFTIVENESS_PRODUCT_NAME, df_second_round.MOLE_NAME, \
											df_second_round.PRODUCT_NAME_STANDARD, df_second_round.EFF_MOLE_DOSAGE))

	df_second_round = df_second_round.withColumnRenamed("EFFTIVENESS_PRODUCT_NAME", "EFFTIVENESS_PRODUCT_NAME_FIRST") \
								.withColumnRenamed("EFFTIVENESS_DOSAGE", "EFFTIVENESS_DOSAGE_FIRST") \
								.withColumnRenamed("EFFTIVENESS_DOSAGE_SE", "EFFTIVENESS_DOSAGE") \
								.withColumnRenamed("EFFTIVENESS_MANUFACTURER_SE", "EFFTIVENESS_MANUFACTURER") \
								.withColumnRenamed("EFFTIVENESS_PRODUCT_NAME_SE", "EFFTIVENESS_PRODUCT_NAME")
								# .withColumnRenamed("EFFTIVENESS_MANUFACTURER", "EFFTIVENESS_MANUFACTURER_FIRST") \
								

	df_second_round.persist()
	return df_second_round

def recalculation_spec_effectiveness(df_second_round):
	print(df_second_round.columns)
	df_second_round = df_second_round.withColumn("EFFTIVENESS_SPEC_FIRST", col("EFFTIVENESS_SPEC"))
	df_second_round = df_second_round.withColumn("EFFTIVENESS_SPEC",\
									when((col("CHC_GROSS_UNIT")==col("SPEC_GROSS_UNIT_PURE_STANDARD"))&(col("SPEC_VALID_UNIT_PURE")==col("SPEC_valid_unit_STANDARD")),\
									modify_first_spec_effectiveness(df_second_round.SPEC_valid_digit_STANDARD, df_second_round.SPEC_GROSS_VALUE_PURE_STANDARD,df_second_round.SPEC_VALID_VALUE_PURE, df_second_round.SPEC_GROSS_VALUE_PURE, df_second_round.EFFTIVENESS_SPEC_FIRST))\
									.otherwise(col("EFFTIVENESS_SPEC_FIRST")))
# 	df_second_round.filter(col("EFFTIVENESS_SPEC") == 1.0).select('DOSAGE','SPEC','DOSAGE_STANDARD','SPEC_STANDARD','EFFTIVENESS_SPEC','EFFTIVENESS_SPEC_FIRST').show(1000)
	return df_second_round

def select_specified_cols(df_second_round):

	cols = ["SID", "ID","PACK_ID_CHECK",  "PACK_ID_STANDARD","DOSAGE","MOLE_NAME","PRODUCT_NAME","SPEC","PACK_QTY","MANUFACTURER_NAME","SPEC_ORIGINAL",
			"MOLE_NAME_STANDARD","PRODUCT_NAME_STANDARD","CORP_NAME_STANDARD","MANUFACTURER_NAME_STANDARD","MANUFACTURER_NAME_EN_STANDARD","DOSAGE_STANDARD","SPEC_STANDARD","PACK_QTY_STANDARD",
			"SPEC_valid_digit_STANDARD","SPEC_valid_unit_STANDARD","SPEC_GROSS_VALUE_PURE_STANDARD","SPEC_GROSS_UNIT_PURE_STANDARD","SPEC_GROSS_VALUE_PURE","CHC_GROSS_UNIT","SPEC_VALID_VALUE_PURE",
			"SPEC_VALID_UNIT_PURE","EFFTIVENESS_MOLE_NAME","EFFTIVENESS_PRODUCT_NAME","EFFTIVENESS_DOSAGE","EFFTIVENESS_PACK_QTY","EFFTIVENESS_MANUFACTURER","EFFTIVENESS_SPEC"]
	df_second_round = df_second_round.select(cols)
   
	return df_second_round


@pandas_udf(DoubleType(), PandasUDFType.SCALAR)
def modify_first_spec_effectiveness(standard_valid, standard_gross, target_valid, target_gross, EFFTIVENESS_SPEC_FIRST):

	frame = { "standard_valid": standard_valid, "standard_gross": standard_gross, "target_valid": target_valid, "target_gross":target_gross ,"EFFTIVENESS_SPEC_FIRST" : EFFTIVENESS_SPEC_FIRST}  
	df = pd.DataFrame(frame)
	df["EFFTIVENESS_SPEC"] = df.apply(lambda x : 1.0 if (math.isclose(float(x['standard_valid']),float(x['target_valid']), rel_tol=0, abs_tol=0 ))|\
                                      (math.isclose(float(x['standard_gross']),float(x['target_gross']), rel_tol=0, abs_tol=0 )) else x['EFFTIVENESS_SPEC_FIRST'], axis=1)
	return df["EFFTIVENESS_SPEC"]


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
	df_dosage_explode = df_dosage_explode.groupBy('ID').agg({"jws":"max"}).withColumnRenamed("max(jws)","EFF_MOLE_DOSAGE")
	df_dosage = df.join(df_dosage_explode, "ID", how="left")
	
	return df_dosage

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
	