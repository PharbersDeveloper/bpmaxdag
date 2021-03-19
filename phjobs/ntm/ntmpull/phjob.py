# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import pandas_udf, PandasUDFType
import pandas as pd
import os
import uuid


def execute(**kwargs):
	"""
		please input your code below
	"""
	logger = phs3logger(kwargs["job_id"], LOG_DEBUG_LEVEL)
	
	sparkClassPath = os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.postgresql:postgresql:42.2.14 pyspark-shell'

	spark = SparkSession.builder \
		.master("yarn") \
		.appName("ntm pull data from db") \
		.config("spark.driver.memory", "1g") \
		.config("spark.executor.cores", "1") \
		.config("spark.executor.instance", "1") \
		.config("spark.executor.memory", "1g") \
		.config("spark.driver.extraClassPath", sparkClassPath) \
		.getOrCreate()

	access_key = os.getenv("AWS_ACCESS_KEY_ID")
	secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

	if access_key is not None:
		spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
		spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
		spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
		spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
		spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.cn-northwest-1.amazonaws.com.cn")


#####################============configure================#################
	logger = phs3logger(kwargs["job_id"])
	logger.info(kwargs)
#####################=============configure===============#################


#################-----------input---------------################
	# depends = get_depends_path(kwargs)
	g_postgres_uri = kwargs["postgres_uri"]
	g_postgres_user = kwargs["postgres_user"]
	g_postgres_pass = kwargs["postgres_pass"]
################------------input----------------################


###############----------------output-------------################
	job_id = get_job_id(kwargs)
	run_id = get_run_id(kwargs)
	result_path_prefix = get_result_path(kwargs, run_id, job_id)
	result_path = result_path_prefix + kwargs["pull_result"]
###############----------------output--------------##################


	postgres_table = """(select * from answer where "periodAnswers"='') temp"""
	# postgres_table = """(select * from answer where "periodAnswers"='o8mpVPkJTVrUFfgk6KR-') temp"""
	df = spark.read.format("jdbc") \
			.option("url", g_postgres_uri) \
			.option("dbtable", postgres_table) \
			.option("user", g_postgres_user) \
			.option("password", g_postgres_pass) \
			.option("driver", "org.postgresql.Driver") \
			.load()
			
	df.show()
	print(df.count())

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