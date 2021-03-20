# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.functions import broadcast, lit
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
	g_proposal_id = kwargs["proposal_id"]
	g_project_id = kwargs["project_id"]
	g_period_id = kwargs["period_id"]
	g_postgres_uri = kwargs["postgres_uri"]
	g_postgres_user = kwargs["postgres_user"]
	g_postgres_pass = kwargs["postgres_pass"]
################------------input----------------################


###############----------------output-------------################
	job_id = get_job_id(kwargs)
	run_id = get_run_id(kwargs)
	result_path_prefix = get_result_path(kwargs, run_id, job_id)
	result_path = result_path_prefix + kwargs["pull_result"]
	competitor_result = result_path_prefix + kwargs["competitor_result"]
###############----------------output--------------##################

	hospital_table = """(select id,name,level from hospital) hospital"""
	hospitals = spark.read.format("jdbc") \
					.option("url", g_postgres_uri) \
					.option("dbtable", hospital_table) \
					.option("user", g_postgres_user) \
					.option("password", g_postgres_pass) \
					.option("driver", "org.postgresql.Driver") \
					.load()
	hospitals = hospitals.withColumnRenamed("id", "dest_id") \
						.withColumnRenamed("name", "hospital") \
						.withColumnRenamed("level", "hospital_level")

	product_table = """(select id,name,"lifeCycle" from product) product"""
	products = spark.read.format("jdbc") \
					.option("url", g_postgres_uri) \
					.option("dbtable", product_table) \
					.option("user", g_postgres_user) \
					.option("password", g_postgres_pass) \
					.option("driver", "org.postgresql.Driver") \
					.load()
	products = products.withColumnRenamed("id", "goods_id") \
						.withColumnRenamed("name", "product") \
						.withColumnRenamed("lifeCycle", "life_cycle")

	resource_table = """(select id,name,"totalTime" from resource) resource"""
	resources = spark.read.format("jdbc") \
					.option("url", g_postgres_uri) \
					.option("dbtable", resource_table) \
					.option("user", g_postgres_user) \
					.option("password", g_postgres_pass) \
					.option("driver", "org.postgresql.Driver") \
					.load()
	resources = resources.withColumnRenamed("id", "representative_id") \
						.withColumnRenamed("name", "representative") \
						.withColumnRenamed("totalTime", "representative_time")
	

	answer_table = """(select * from answer where "periodAnswers"='{}') answers""".format(g_period_id)
	answers = spark.read.format("jdbc") \
				.option("url", g_postgres_uri) \
				.option("dbtable", answer_table) \
				.option("user", g_postgres_user) \
				.option("password", g_postgres_pass) \
				.option("driver", "org.postgresql.Driver") \
				.load()
	answers.persist()
	
	resource_answer = answers.where(answers.category == "Resource")
	resource_answer = resource_answer.select("resource", "vocationalDevelopment", \
											"regionTraining", "performanceTraining", "salesAbilityTraining", \
											"assistAccessTime", "abilityCoach", "productKnowledgeTraining") \
									.withColumnRenamed("vocationalDevelopment", "career_development_guide") \
									.withColumnRenamed("productKnowledgeTraining", "product_knowledge_training") \
									.withColumnRenamed("regionTraining", "territory_management_training") \
									.withColumnRenamed("performanceTraining", "performance_review") \
									.withColumnRenamed("salesAbilityTraining", "sales_skills_training") \
									.withColumnRenamed("assistAccessTime", "field_work") \
									.withColumnRenamed("abilityCoach", "one_on_one_coaching")

	management_answer = answers.where(answers.category == "Management")
	management_answer = management_answer.select("strategAnalysisTime", "adminWorkTime", "clientManagementTime", "kpiAnalysisTime", "teamMeetingTime") \
										.withColumnRenamed("strategAnalysisTime", "business_strategy_planning") \
										.withColumnRenamed("adminWorkTime", "admin_work") \
										.withColumnRenamed("clientManagementTime", "kol_management") \
										.withColumnRenamed("kpiAnalysisTime", "employee_kpi_and_compliance_check") \
										.withColumnRenamed("teamMeetingTime", "team_meeting") 
	resource_answer = resource_answer.crossJoin(broadcast(management_answer))
	business_answer = answers.where(answers.category == "Business")
	business_answer = business_answer.select("resource", "product", "target", "budget", "salesTarget", "meetingPlaces", "visitTime") \
									.withColumnRenamed("salesTarget", "quota") \
									.withColumnRenamed("meetingPlaces", "meeting_attendance") \
									.withColumnRenamed("visitTime", "call_time")

	answers = business_answer.join(resource_answer, on=["resource"], how="left")
	answers.persist()


	preset_table = """(select * from preset where "proposal"='{}') presets""".format(g_proposal_id)
	presets = spark.read.format("jdbc") \
				.option("url", g_postgres_uri) \
				.option("dbtable", preset_table) \
				.option("user", g_postgres_user) \
				.option("password", g_postgres_pass) \
				.option("driver", "org.postgresql.Driver") \
				.load()
	presets.persist()
	
	presets = presets.where(presets.phase == 0)
	
	resource_preset = presets.where(presets.category == 2.0) \
							.select("resource", "currentTMA", "currentSalesSkills", "currentProductKnowledge", 
									"currentBehaviorEfficiency", "currentWorkMotivation", 
									"currentTargetDoctorNum", "currentTargetDoctorCoverage", "currentClsADoctorVT",
									"currentClsBDoctorVT", "currentClsCDoctorVT") \
							.withColumnRenamed("currentTMA", "p_territory_management_ability")	\
							.withColumnRenamed("currentSalesSkills", "p_sales_skills")	\
							.withColumnRenamed("currentProductKnowledge", "p_product_knowledge")	\
							.withColumnRenamed("currentBehaviorEfficiency", "p_behavior_efficiency")	\
							.withColumnRenamed("currentWorkMotivation", "p_work_motivation") \
							.withColumnRenamed("currentTargetDoctorNum", "p_target") \
							.withColumnRenamed("currentTargetDoctorCoverage", "p_target_coverage") \
							.withColumnRenamed("currentClsADoctorVT", "p_high_target") \
							.withColumnRenamed("currentClsBDoctorVT", "p_middle_target") \
							.withColumnRenamed("currentClsCDoctorVT", "p_low_target") \
	

	business_preset = presets.where(presets.category == 8.0) \
							.select("hospital", "lastSales", "lastQuota", "lastBudget", "lastShare", "potential") \
							.withColumnRenamed("lastSales", "p_sales") \
							.withColumnRenamed("lastQuota", "p_quota") \
							.withColumnRenamed("lastShare", "p_share") \
							.withColumnRenamed("lastBudget", "p_budget") \
							.withColumnRenamed("hospital", "target")
	
	cal_data = answers.join(resource_preset, on="resource", how="left")
	cal_data = cal_data.join(business_preset, on="target", how="left")

	cal_data = cal_data.withColumnRenamed("target", "dest_id")	
	cal_data = cal_data.withColumnRenamed("product", "goods_id")	
	cal_data = cal_data.withColumnRenamed("resource", "representative_id")	

	cal_data = cal_data.join(hospitals, on="dest_id", how="left") \
						.join(products, on="goods_id", how="left") \
						.join(resources, on="representative_id", how="left")

	cal_data.withColumn("proposal_id", lit("g_proposal_id")) \
			.withColumn("project_id", lit("g_project_id")) \
			.withColumn("period_id", lit("g_period_id")) \
			.repartition(1).write.mode("overwrite").parquet(result_path)

	competitors = presets.where(presets.category == 16.0)
	competitors = competitors.select("product", "lastShare") \
							.withColumnRenamed("product", "goods_id") \
							.withColumnRenamed("lastShare", "p_share")
							
	competitors = competitors.join(products, on="goods_id", how="left")
	competitors.withColumn("proposal_id", lit("g_proposal_id")) \
			.withColumn("project_id", lit("g_project_id")) \
			.withColumn("period_id", lit("g_period_id")) \
			.repartition(1).write.mode("overwrite").parquet(competitor_result)
	
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