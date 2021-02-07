# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger
from pyspark.sql.functions import max
import uuid


def execute(**kwargs):
	"""
		please input your code below
		get spark session: spark = kwargs["spark"]()
	"""
	logger = phs3logger(kwargs["job_id"])
	spark = kwargs["spark"]()
	
	logger.info(kwargs)

#######################---------------input-------------#######################	
	depends = get_depends_path(kwargs)
	df_mnf_adjusted = spark.read.parquet(depends["mnf_adjust"])
	df_spec_adjusted = spark.read.parquet(depends["spec_adjust"])
	g_repartition_shared = int(kwargs["g_repartition_shared"])
#######################---------------input-------------#######################	

#######################--------------output-------------######################## 
	job_id = get_job_id(kwargs)
	run_id = get_run_id(kwargs)
	result_path_prefix = get_result_path(kwargs, run_id, job_id)
	result_path = result_path_prefix + kwargs["prod_adjust_result"]
#######################--------------output--------------########################



########################--------------main function--------------------#################  
	df_result = df_mnf_adjusted.union(df_spec_adjusted)
	df_result = df_result.groupBy("SID") \
					.agg(
						max(df_result.EFFTIVENESS_MOLE_NAME).alias("EFFTIVENESS_MOLE_NAME"),
						max(df_result.EFFTIVENESS_PRODUCT_NAME).alias("EFFTIVENESS_PRODUCT_NAME"),
						max(df_result.EFFTIVENESS_DOSAGE).alias("EFFTIVENESS_DOSAGE"),
						max(df_result.EFFTIVENESS_SPEC).alias("EFFTIVENESS_SPEC"),
						max(df_result.EFFTIVENESS_PACK_QTY).alias("EFFTIVENESS_PACK_QTY"),
						max(df_result.EFFTIVENESS_MANUFACTURER).alias("EFFTIVENESS_MANUFACTURER"),
					)
	cols = ['SID', 'ID', 'PACK_ID_CHECK', 'PACK_ID_STANDARD', 'DOSAGE', 'MOLE_NAME', 'PRODUCT_NAME', 'SPEC', 'PACK_QTY', 'MANUFACTURER_NAME', 'SPEC_ORIGINAL', 'MOLE_NAME_STANDARD', 'PRODUCT_NAME_STANDARD', 'CORP_NAME_STANDARD', 'MANUFACTURER_NAME_STANDARD', 'MANUFACTURER_NAME_EN_STANDARD', 'DOSAGE_STANDARD', 'SPEC_STANDARD', 'PACK_QTY_STANDARD', 'SPEC_valid_digit_STANDARD', 'SPEC_valid_unit_STANDARD', 'SPEC_GROSS_VALUE_PURE_STANDARD', 'SPEC_GROSS_UNIT_PURE_STANDARD', 'SPEC_GROSS_VALUE_PURE', 'CHC_GROSS_UNIT', 'SPEC_VALID_VALUE_PURE', 'SPEC_VALID_UNIT_PURE']
	df_mnf_distinct_col = df_mnf_adjusted.select(cols)
	df_result = df_result.join(df_mnf_distinct_col, on="SID", how="left")
	
	df_result.repartition(g_repartition_shared).write.mode("overwrite").parquet(result_path)
######################--------------main function--------------------#################   
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
################-----------------------------------------------------################
