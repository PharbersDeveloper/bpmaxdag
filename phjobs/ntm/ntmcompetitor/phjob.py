# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
import pandas as pd
import uuid
from pyspark.sql.types import *
from pyspark.sql.functions import rand, lit
from pyspark.sql.functions import udf, pandas_udf, PandasUDFType


def execute(**kwargs):
	"""
		please input your code below
		get spark session: spark = kwargs["spark"]()
	"""
#####################============configure================#################
	logger = phs3logger(kwargs["job_id"])
	spark = kwargs["spark"]()
	logger.info(kwargs)
#####################=============configure===============#################


#################-----------input---------------################
	depends = get_depends_path(kwargs)
	cal_path = depends["cal_path"]
	competitor_path = depends["competitor_path"]
################------------input----------------################


###############----------------output-------------################
	job_id = get_job_id(kwargs)
	run_id = get_run_id(kwargs)
	result_path_prefix = get_result_path(kwargs, run_id, job_id)
	competitor_result = result_path_prefix + kwargs["competitor_result"]
# 	drop_path = result_path_prefix + kwargs["cross_drop"]
###############----------------output--------------##################

	
	tp = spark.read.parquet(cal_path).select("total_potential").take(1)[0]["total_potential"]
	competitor_data = spark.read.parquet(competitor_path)
	
	competitor_data = competitor_data.withColumn("p_share", competitor_data.p_share.cast("double"))
	competitor_data = competitor_data.withColumn("total_potential", lit(tp))
	competitor_data = competitor_data.withColumn("p_sales", competitor_data.total_potential / 4.0 * competitor_data.p_share)
	competitor_data = competitor_data.withColumn("share", competitor_data.p_share * (rand() / 5 + 0.9))
	competitor_data = competitor_data.withColumn("sales", competitor_data.total_potential / 4 * competitor_data.share)
	competitor_data = competitor_data.withColumn("sales_growth", competitor_data.sales / competitor_data.p_sales - 1)
	competitor_data = competitor_data.select("product", "sales", "share", "sales_growth")

	# competitor_data.persist()
	# competitor_data.show()
	
	competitor_data.repartition(1).write.mode("overwrite").parquet(competitor_result)
 
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
	

################--------------------- functions ---------------------################