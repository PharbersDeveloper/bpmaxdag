# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.functions import broadcast, lit, sum
import psycopg2
import psycopg2.extras
from urllib.parse import urlparse
import pandas as pd
import os
import uuid
import random


def execute(**kwargs):
	"""
		please input your code below
	"""
	logger = phs3logger(kwargs["job_id"], LOG_DEBUG_LEVEL)
	
	sparkClassPath = os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.postgresql:postgresql:42.2.14 pyspark-shell'

	spark = SparkSession.builder \
		.master("yarn") \
		.appName("ntm push data to db from s3") \
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
	g_proposal_id = kwargs["proposal_id"]
	g_project_id = kwargs["project_id"]
	g_period_id = kwargs["period_id"]
	g_is_push = int(kwargs["g_is_push"])
	g_phase = int(kwargs["phase"])
	g_postgres_uri = kwargs["postgres_uri"]
	g_postgres_user = kwargs["postgres_user"]
	g_postgres_pass = kwargs["postgres_pass"]
	depends = get_depends_path(kwargs)
	cal_path = depends["cal_path"]
	competitor_path = depends["competitor_path"]
	assessment_path = depends["assessment_path"]
################------------input----------------################


###############----------------output-------------################
	job_id = get_job_id(kwargs)
	run_id = get_run_id(kwargs)
	result_path_prefix = get_result_path(kwargs, run_id, job_id)
	report_path = result_path_prefix + kwargs["report_result"]
	final_result = result_path_prefix + kwargs["final_result"]
###############----------------output--------------###############

	cal_data = spark.read.parquet(cal_path)
	# competitor_data = spark.read.parquet(competitor_path)

	cal_data = cal_data.select("dest_id", "representative_id", "goods_id", "potential", "sales", "total_sales", "quota", "total_quota", "total_budget")
	
	cal_data = cal_data.withColumnRenamed("dest_id", "hospital") \
						.withColumnRenamed("representative_id", "resource")	\
						.withColumnRenamed("goods_id", "product")	\
						.withColumnRenamed("sales", "sales")	\
						.withColumnRenamed("quota", "salesQuota")
	cal_data.persist()	
	
	cal_hosp_data = cal_report_meta(cal_data, "Hospital", "hospital")
	cal_res_data = cal_report_meta(cal_data, "Resource", "resource")
	cal_prod_data = cal_report_meta(cal_data, "Product", "product")

	# cal_report = cal_hosp_data.unionByName(cal_res_data, allowMissingColumns=True).unionByName(cal_prod_data, allowMissingColumns=True)
	cal_report = cal_hosp_data.union(cal_res_data).union(cal_prod_data)
	cal_report = cal_report.drop("total_sales", "total_quota")
	
	competitor_data = spark.read.parquet(competitor_path)
	competitor_data = competitor_data.select("goods_id", "sales") \
									.withColumnRenamed("goods_id", "product") \
									.withColumn("salesQuota", lit(0.0)) \
									.withColumn("category", lit("Product")) \
									.withColumn("salesContri", lit(0.0)) \
									.withColumn("quotaContri", lit(0.0)) \
									.withColumn("achievements", lit(0.0)) \
									.withColumn("resource", lit("")) \
									.withColumn("hospital", lit(""))

	cal_report = cal_report.union(competitor_data)
	cal_report = cal_report.withColumn("patientNum", lit(0)) \
						.withColumn("drugEntrance", lit("已准入")) \
						.withColumn("quotaGrowthMOM", lit(0.0)) \
						.withColumn("required", lit(0.0)) \
						.withColumn("salesGrowthYOY", lit(0.0)) \
						.withColumn("salesGrowthMOM", lit(0.0)) \
						.withColumn("salesGrowthMOM", lit(0.0))

			
	cal_report = cal_report.withColumn("proposalId", lit(g_proposal_id)) \
							.withColumn("projectId", lit(g_project_id)) \
							.withColumn("periodId", lit(g_period_id)) \
							.withColumn("phase", lit(0)) \
							.withColumn("region", lit("")) \
							.withColumn("periodReports", lit(g_period_id)).na.fill("")
	cal_report = cal_report.withColumn("id", general_report_id(cal_report.projectId))
	
	cal_report.persist()
	cal_report.repartition(1).write.mode("overwrite").parquet(report_path)
	if g_is_push is 1:
		cal_report.write.format("jdbc") \
				.option("url", g_postgres_uri) \
				.option("dbtable", "report") \
				.option("user", g_postgres_user) \
				.option("password", g_postgres_pass) \
				.option("driver", "org.postgresql.Driver") \
				.mode("append") \
				.save()
	
	row_tmp = cal_data.select("total_sales", "total_quota", "total_budget").take(1)[0]
	total_sales = row_tmp["total_sales"]
	total_quota = row_tmp["total_quota"]
	total_budget = row_tmp["total_budget"]
	
	assessments = spark.read.parquet(assessment_path)
	assessments.show()
	
	final = assessments.drop("proposal_id") \
						.drop("project_id") \
						.drop("period_id") \
						.withColumn("id", general_report_id(assessments.general_performance)) \
						.withColumn("sales", lit(total_sales)) \
						.withColumn("quota", lit(total_quota)) \
						.withColumn("budget", lit(total_budget)) \
						.withColumn("projectFinals", lit(g_project_id)) \
						.withColumn("roi", lit(0.0)) \
						.withColumn("newAccount", lit(0.0)) \
						.withColumn("salesForceProductivity", lit(0.0))

	final = final.withColumn("quotaAchv", final.sales / final.quota) \
				.withColumnRenamed("general_performance", "generalPerformance") \
				.withColumnRenamed("resource_assigns", "resourceAssigns") \
				.withColumnRenamed("region_division", "regionDivision") \
				.withColumnRenamed("target_assigns", "targetAssigns") \
				.withColumnRenamed("manage_time", "manageTime") \
				.withColumnRenamed("manage_team", "manageTeam")

	final.persist()
	final.show()
	
	###############---------------- write relevance final_id for project table -------------################
	if g_is_push is 1:
		pg_connection = connect_pg(g_postgres_uri, g_postgres_user, g_postgres_pass)
		cursor = pg_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
		
		final_id = final.take(1)[0]['id']
		
		cursor.execute("SELECT * FROM project WHERE id ='{id}'".format(id=g_project_id))
		finals = cursor.fetchone()['finals']
		finals.append(final_id)
		
		cursor.execute("UPDATE project SET finals = '{finals}' WHERE id = '{id}'".format(
			finals="{" + ",".join(finals) + "}", id=g_project_id)
		)
		pg_connection.commit()
		
		cursor.close()
		pg_connection.close()
	###############---------------- write relevance final_id for project table -------------################
	
	final.repartition(1).write.mode("overwrite").parquet(final_result)
	if g_is_push is 1:
		final.write.format("jdbc") \
				.option("url", g_postgres_uri) \
				.option("dbtable", "final") \
				.option("user", g_postgres_user) \
				.option("password", g_postgres_pass) \
				.option("driver", "org.postgresql.Driver") \
				.mode("append") \
				.save()
	
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


def cal_report_meta(df, cat, gp_col):
	df = df.groupBy(gp_col, "total_sales", "total_quota").agg(
						sum(df.sales).alias("sales"),
						sum(df.salesQuota).alias("salesQuota")
					)
	df = df.withColumn("category", lit(cat)) \
			.withColumn("salesContri", df.sales / df.total_sales) \
			.withColumn("quotaContri", df.salesQuota / df.total_quota) \
			.withColumn("achievements", df.sales / df.salesQuota)
			
	cols = ["hospital", "resource", "product"]
	cols = [n for n in cols if n != gp_col]
	for item in cols:
		df = df.withColumn(item, lit(""))
			
	return df
	

@pandas_udf(StringType(), PandasUDFType.SCALAR)	
def general_report_id(a):
	frame = {
		"a": a
	}
	df = pd.DataFrame(frame)
	
	def general_report_id_acc(t):
		charset = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ' + \
					'abcdefghijklmnopqrstuvwxyz' + \
					'0123456789-_'
		
		charsetLength = len(charset)
		
		keyLength = 3 * 5
		
		result = []
		for _ in range(keyLength):
			result.append(charset[random.randint(0, charsetLength - 1)])
		
		return "".join(result)
	
	df["result"] = df["a"].apply(lambda x: general_report_id_acc(x))
	return df["result"]
	

def connect_pg(url, postgres_user, postgres_pass):
	username = postgres_user
	password = postgres_pass
	hostname = url.split("/")[-2]
	database = url.split("/")[-1]
	connection = psycopg2.connect(
	    database = database,
	    user = username,
	    password = password,
	    host = hostname
	)
	return connection

################--------------------- functions ---------------------################