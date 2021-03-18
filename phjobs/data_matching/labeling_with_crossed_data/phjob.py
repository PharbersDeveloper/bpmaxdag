# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger
import uuid
from pyspark.sql.functions import when , col 


def execute(**kwargs):
	"""
		please input your code below
		get spark session: spark = kwargs["spark"]()
	"""
################========configure=============############
	logger = phs3logger(kwargs["job_id"])
	spark = kwargs["spark"]()
	logger.info(kwargs)
############========configure==============#############

##############-----------input------------##############
	depends = get_depends_path(kwargs)
	path_max_eff = depends["max_eff_result"]
#############------------input-------------##############

#############------------output-----------##############
	job_id = get_job_id(kwargs)
	run_id = get_run_id(kwargs)
	result_path_prefix = get_result_path(kwargs, run_id, job_id)
	result_path = result_path_prefix + kwargs["label_result"]
############-------------output------------#############

###########----------loading files-------------############
	df_result = load_df_max_eff(spark, path_max_eff)
##########-----------loading files--------############

###########---------mian functions ----------######################
	df_result = make_label(df_result)
	df_result = make_features_normalization(df_result)
	df_result.repartition(10).write.mode("overwrite").parquet(result_path)
	logger.info("第二轮完成，写入完成")
###########---------mian functions ----------######################

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

def load_df_max_eff(spark, path_max_result):
	df_result = spark.read.parquet(path_max_result)
	return df_result

def make_label(df_result):
	df_result = df_result.withColumn("PACK_ID_CHECK_NUM", df_result.PACK_ID_CHECK.cast("int")).na.fill({"PACK_ID_CHECK_NUM": -1})
	df_result = df_result.withColumn("PACK_ID_STANDARD_NUM", df_result.PACK_ID_STANDARD.cast("int")).na.fill({"PACK_ID_STANDARD_NUM": -1})
	df_result = df_result.withColumn("label",
					when((df_result.PACK_ID_CHECK_NUM > 0) & (df_result.PACK_ID_STANDARD_NUM > 0) & (df_result.PACK_ID_CHECK_NUM == df_result.PACK_ID_STANDARD_NUM), 1.0).otherwise(0.0)) \
					.drop("PACK_ID_CHECK_NUM", "PACK_ID_STANDARD_NUM")
	return df_result

def make_features_normalization(df_result):
	df_result = df_result.withColumn("EFFTIVENESS_MOLE_NAME", col("EFFTIVENESS_MOLE_NAME").cast("double"))\
						.withColumn("EFFTIVENESS_PRODUCT_NAME", col("EFFTIVENESS_PRODUCT_NAME").cast("double"))\
						.withColumn("EFFTIVENESS_DOSAGE", col("EFFTIVENESS_DOSAGE").cast("double"))\
						.withColumn("EFFTIVENESS_SPEC", col("EFFTIVENESS_SPEC").cast("double"))\
						.withColumn("EFFTIVENESS_PACK_QTY", col("EFFTIVENESS_PACK_QTY").cast("double"))\
						.withColumn("EFFTIVENESS_MANUFACTURER", col("EFFTIVENESS_MANUFACTURER").cast("double"))  
	df_result = df_result.withColumn("EFFTIVENESS_SPEC", when(col("EFFTIVENESS_SPEC").isNull(), 0.0).otherwise(col("EFFTIVENESS_SPEC")))
	return df_result
################-----------------------------------------------------################