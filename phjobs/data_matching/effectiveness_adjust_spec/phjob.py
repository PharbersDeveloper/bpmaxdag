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
	path_adjust_dosage_result = depends["input"]
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
	df_second_round = load_adjust_dosage_result(spark, path_adjust_dosage_result)
################-----------loading files---------------######################


###################--------------main function------------------------#################  
	df_second_round = recalculation_spec_effectiveness(df_second_round)
	df_second_round.repartition(g_repartition_shared).write.mode("overwrite").parquet(mid_path)
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

def load_adjust_dosage_result(spark, path_adjust_dosage_result):
    df_second_round = spark.read.parquet(path_adjust_dosage_result)
    return df_second_round


def recalculation_spec_effectiveness(df_second_round):
	print(df_second_round.columns)
	df_second_round = df_second_round.withColumn("EFFTIVENESS_SPEC_FIRST", col("EFFTIVENESS_SPEC"))
	df_second_round = df_second_round.withColumn("EFFTIVENESS_SPEC",\
									when((col("CHC_GROSS_UNIT")==col("SPEC_GROSS_UNIT_PURE_STANDARD"))&(col("SPEC_VALID_UNIT_PURE")==col("SPEC_valid_unit_STANDARD")),\
									modify_first_spec_effectiveness(df_second_round.SPEC_valid_digit_STANDARD, df_second_round.SPEC_GROSS_VALUE_PURE_STANDARD,df_second_round.SPEC_VALID_VALUE_PURE, df_second_round.SPEC_GROSS_VALUE_PURE, df_second_round.EFFTIVENESS_SPEC_FIRST))\
									.otherwise(col("EFFTIVENESS_SPEC_FIRST")))
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
	df["EFFTIVENESS_SPEC"] = df.apply(lambda x : 0.995 if (math.isclose(float(x['standard_valid']),float(x['target_valid']), rel_tol=0, abs_tol=0 ))|\
                                      (math.isclose(float(x['standard_gross']),float(x['target_gross']), rel_tol=0, abs_tol=0 )) else x['EFFTIVENESS_SPEC_FIRST'], axis=1)
	return df["EFFTIVENESS_SPEC"]

################-----------------------------------------------------################
	