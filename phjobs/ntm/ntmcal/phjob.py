# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import cast
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
	cal_data = spark.read.parquet(kwargs["cal_data_path"])
	weigthages = spark.read.csv(kwargs["weight_path"], header="true")
	manager = spark.read.parquet(kwargs["manage_path"])
	standard_time = spark.read.csv(kwargs["standard_time_path"], header="true")
	level_data = spark.read.csv(kwargs["level_data_path"], header="true")
	curves = spark.read.csv(kwargs["curves_path"], header="true")
################------------input----------------################


###############----------------output-------------################
	job_id = get_job_id(kwargs)
	run_id = get_run_id(kwargs)
	result_path_prefix = get_result_path(kwargs, run_id, job_id)
	result_path = result_path_prefix + kwargs["cal_result"]
###############----------------output--------------##################

	curves = curves.withColumn(curves.x.cast("double").alias("x"))
	curves = curves.withColumn(curves.y.cast("double").alias("y"))
	curves = curves.toPandas()
	print(curves)
	
	manager = withColumn("tid", lit("1"))
	weigthages = withColumn("tid", lit("1"))
	mw = manager.join(weigthages, on="tid", how="left")
	cal_data = withColumn("tid", lit("1"))
	cal_data = cal_data.join(mw, on="tid", how="left")
	cal_data = cal_data.drop("tid")
	cal_data.persist()
	
	manager.show()
	weigthages.show()
	standard_time.show()

	cal_data = cal_data.withColumn(cal_data, "level_factor", 
				0.8 * (cal_data.potential / cal_data.total_potential) + 0.2 * (cal_data.p_sales / cal_data.total_p_sales))
	cal_data = cal_data.withColumn(cal_data, "work_motivation", 
				cal_data.p_work_motivation + 0.15 * (10 - cal_data.p_work_motivation) * (cal_data.performance_review + cal_data.career_development_guide)
	cal_data = cal_data.withColumn(cal_data, "territory_management_ability", 
				cal_data.p_territory_management_ability + 0.3 * (10 - cal_data.p_territory_management_ability) * cal_data.territory_management_training)
	cal_data = cal_data.withColumn(cal_data, "sales_skills", 
				cal_data.p_sales_skills + 0.3 * (10 - cal_data.p_sales_skills) * cal_data.sales_skills_training)
	cal_data = cal_data.withColumn(cal_data, "product_knowledge", 
				cal_data.p_product_knowledge + 0.3 * (10 - cal_data.p_product_knowledge) * cal_data.product_knowledge_training)
	cal_data = cal_data.withColumn(cal_data, "quota_growth", 
				cal_data.quota/cal_data.p_quota)
	cal_data = cal_data.withColumn(cal_data, "budget_prop", 
				(cal_data.budget_factor * cal_data.budget_factor_w + cal_data.meeting_attendance_factor * cal_data.meeting_attendance_factor_w) * 100)
	cal_data = cal_data.withColumn(cal_data, "level", when(cal_data.level_factor > 0.15, 3).otherwise(when(cal_data.level_factor <= 0.05, 1).otherwise(2)))
	
	cal_data = cal_curves_result_prepare(cal_data, ["curve09"])
	cal_data = cal_data.withColumn(cal_data, "behavior_efficiency_factor", cal_curves_result(cal_data.one_on_one_coaching))
	
	cal_data = cal_curves_result(cal_data, ["curve10", "curve11", "curve12"], "level", [1, 2, 3])
	cal_data = cal_data.withColumn(cal_data, "call_time_index", cal_curves_result(cal_data.call_time))
	
	cal_data = cal_curves_result(cal_data, ["curve14"])
	cal_data = cal_data.withColumn(cal_data, "quota_restriction_index", cal_curves_result(cal_data.quota_restriction_factor))
	
	cal_data = cal_curves_result(cal_data, ["curve16"])
	cal_data = cal_data.withColumn(cal_data, "field_work_index", cal_curves_result(cal_data.field_work))
	
	cal_data = cal_curves_result(cal_data, ["curve18"])
	cal_data = cal_data.withColumn(cal_data, "business_strategy_planning_index", cal_curves_result(cal_data.business_strategy_planning))
	
	cal_data = cal_curves_result(cal_data, ["curve18"])
	cal_data = cal_data.withColumn(cal_data, "admin_work_index", cal_curves_result(cal_data.admin_work))
	
	cal_data = cal_curves_result(cal_data, ["curve18"])
	cal_data = cal_data.withColumn(cal_data, "employee_kpi_and_compliance_check_index", cal_curves_result(cal_data.employee_kpi_and_compliance_check))
	
	cal_data = cal_curves_result(cal_data, ["curve18"])
	cal_data = cal_data.withColumn(cal_data, "team_meeting_index", cal_curves_result(cal_data.team_meeting))
	
	cal_data = cal_curves_result(cal_data, ["curve18"])
	cal_data = cal_data.withColumn(cal_data, "kol_management_index", cal_curves_result(cal_data.kol_management))
	
	cal_data = cal_curves_result(cal_data, ["curve02", "curve03", "curve04"], "level", [1, 2, 3])
	cal_data = cal_data.withColumn(cal_data, "budget_factor", cal_curves_result(cal_data.budget_prop))
	
	cal_data = cal_curves_result(cal_data, ["curve05", "curve06", "curve07"], "level", [1, 2, 3])
	cal_data = cal_data.withColumn(cal_data, "meeting_attendance_factor", cal_curves_result(cal_data.meeting_attendance))
	cal_data.persist()

	cal_data = cal_data.withColumn(cal_data, "behavior_efficiency", 
				cal_data.p_behavior_efficiency + 0.3 * (10 - cal_data.p_behavior_efficiency) * cal_data.behavior_efficiency_factor)
	cal_data = cal_data.withColumn(cal_data, "deployment_quality", 
				cal_data.business_strategy_planning_index * cal_data.business_strategy_planning_index_w + \
				cal_data.admin_work_index * cal_data.admin_work_index_w + \
				cal_data.employee_kpi_and_compliance_check_index * cal_data.employee_kpi_and_compliance_check_index_w + \
				cal_data.kol_management_index * cal_data.kol_management_index_w + \
				cal_data.team_meeting_index * cal_data.team_meeting_index_w)
	cal_data = cal_data.withColumn(cal_data, "customer_relationship", 
				(cal_data.budget_factor * cal_data.budget_factor_w + cal_data.meeting_attendance_factor * cal_data.meeting_attendance_factor_w) * 100)
				
	cal_data = cal_data.withColumn(cal_data, "general_ability", 
				10 * (cal_data.territory_management_ability * cal_data.territory_management_ability_w + \
					cal_data.sales_skills * cal_data.sales_skills_w + \
					cal_data.product_knowledge * cal_data.product_knowledge_w + \
					cal_data.behavior_efficiency * cal_data.behavior_efficiency_w + \
					cal_data.work_motivation * cal_data.work_motivation_w))

	cal_data = cal_data.withColumn(cal_data, "rep_ability_efficiency", 
				(cal_data.general_ability * cal_data.general_ability_w + \
				cal_data.call_time_index * cal_data.call_time_index_w + \
				cal_data.quota_restriction_index * cal_data.quota_restriction_index_w))
	
	cal_data = cal_data.withColumn(cal_data, "sales_performance", 
				cal_data.rep_ability_efficiency * cal_data.rep_ability_efficiency_w + \
				cal_data.field_work_index * cal_data.field_work_index_w + \
				cal_data.deployment_quality * cal_data.deployment_quality_w)
				
	cal_data = cal_data.withColumn(cal_data, "offer_attractiveness", 
				cal_data.sales_performance * cal_data.sales_performance_w + \
				cal_data.customer_relationship * cal_data.customer_relationship_w)
	
	cal_data = cal_curves_result(cal_data, ["curve28"])
	cal_data = cal_data.withColumn(cal_data, "share_delta_factor", cal_curves_result(cal_data.offer_attractiveness))
	
	cal_data = cal_data.withColumn(cal_data, "share", cal_data.p_share * (1.0 + cal_data.share_delta_factor))
	cal_data.persist()
	cal_data.show()
	
	cal_data.write.mode("overwrite").parquet(cal_result)

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
	

def cal_curves_result_prepare(df, curves, adjust_col, condi = []):
	if len(condi) is 0
		df = df.withColumn(df, "curve_name", lit(curves[0]))
	else:
		df = df.withColumn(df, "curve_name", lit(curves[condi.index(df[adjust_col])]))

	# df = df.withColumn(df, "input_col", lit(input_col))
	# df = df.withColumn(df, "output_col", lit(output_col))
	return df

@pandas_udf(ArrayType(DoubleType()), PandasUDFType.SCALAR)
def cal_curves_result(cn, s):
	frame = {
		"cn": cn,
		"source", s
	}
	df = pd.DataFrame(frame)

	def curve_func(cn, input):
		curve_data = curves["name" == cn] 
		if input < min(curve_data.x):
			return curve_data[which.min(curve_data.x), 2]
		
		if input > max(curve_data.x):
			return curve_data[which.max(curve_data.x), 2]

		left = curve_data[which.min(abs(input - curve_data.x)), ]    
		tmp = curve_data[-which.min(abs(input - curve_data.x)), ]    
		right = tmp[which.min(abs(input - tmp.x)), ]

		if left.x <= right.x:
			return (1.0 - (input - left.x) / (right.x - left.x)) * left.y + (1.0 - (right.x - input) / (right.x - left.x)) * right.y
		else:
			return (1.0 - (input - right.x) / (left$x - right.x)) * right.y + (1.0 - (left.x - input) / (left.x - right.x)) * left.y)

	df["result"] = df.apply(lambda x: curve_func(x["cn"], x["source"]), axis=1)
	return df["result"]
################--------------------- functions ---------------------################