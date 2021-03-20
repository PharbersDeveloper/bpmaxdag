# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
import pandas as pd
import uuid
from pyspark.sql.types import *
from pyspark.sql.functions import rand, broadcast, lit, when, sum, abs, first
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
	standard_time = spark.read.csv(kwargs["standard_time_path"], header="true")
	level_data = spark.read.csv(kwargs["level_data_path"], header="true")
#####################=============configure===============#################


#################-----------input---------------################
	depends = get_depends_path(kwargs)
	cal_path = depends["cal_path"]
	competitor_path = depends["competitor_path"]
	g_proposal_id = kwargs["proposal_id"]
	g_project_id = kwargs["project_id"]
	g_period_id = kwargs["period_id"]
################------------input----------------################


###############----------------output-------------################
	job_id = get_job_id(kwargs)
	run_id = get_run_id(kwargs)
	result_path_prefix = get_result_path(kwargs, run_id, job_id)
	assessment_result = result_path_prefix + kwargs["assessment_result"]
# 	drop_path = result_path_prefix + kwargs["cross_drop"]
###############----------------output--------------##################


	cal_data = spark.read.parquet(cal_path)
	cal_data.persist()
	
	cal_data_agg_for_assessment = cal_data.groupBy("representative", "general_ability", "total_potential", "total_p_sales", \
													"total_quota", "representative_time", "total_budget", "total_sales", \
													"total_place", "manager_time") \
											.agg(
												sum(cal_data.potential).alias("potential"), 
												sum(cal_data.quota).alias("quota"), 
												sum(cal_data.sales).alias("sales"), 
												sum(cal_data.budget).alias("budget"), 
												sum(cal_data.call_time).alias("call_time"), 
												sum(cal_data.meeting_attendance).alias("meeting_attendance"), 
												sum(cal_data.one_on_one_coaching).alias("one_on_one_coaching"), 
												sum(cal_data.employee_kpi_and_compliance_check).alias("employee_kpi_and_compliance_check"), 
												sum(cal_data.admin_work).alias("admin_work"), 
												sum(cal_data.kol_management).alias("kol_management"), 
												sum(cal_data.business_strategy_planning).alias("business_strategy_planning"), 
												sum(cal_data.team_meeting).alias("team_meeting"), 
												sum(cal_data.field_work).alias("field_work"), 
												sum(cal_data.p_quota).alias("p_quota"), 
												sum(cal_data.p_sales).alias("p_sales"),
												first(cal_data.p_product_knowledge).alias("p_product_knowledge"),
												first(cal_data.p_sales_skills).alias("p_sales_skills"),
												first(cal_data.p_territory_management_ability).alias("p_territory_management_ability"),
												first(cal_data.p_work_motivation).alias("p_work_motivation"),
												first(cal_data.p_behavior_efficiency).alias("p_behavior_efficiency")
											)
	
	assessment_region_division_tmp = cal_data_agg_for_assessment.selectExpr("sum(general_ability -50) as sumga_scale")
	assessment_region_division  = cal_data_agg_for_assessment.crossJoin(broadcast(assessment_region_division_tmp))
	
	assessment_region_division  = assessment_region_division.withColumn("potential_prop", assessment_region_division.potential / assessment_region_division.total_potential)
	assessment_region_division  = assessment_region_division.withColumn("p_sales_prop", assessment_region_division.p_sales / assessment_region_division.total_p_sales)
	
	assessment_region_division  = assessment_region_division.withColumn("ga_prop", (assessment_region_division.general_ability - 50) / assessment_region_division.sumga_scale)
	assessment_region_division  = assessment_region_division.withColumn("ptt_ps_prop", assessment_region_division.potential_prop * 0.6 + assessment_region_division.p_sales_prop * 0.4)
	
	assessment_region_division  = assessment_region_division.withColumn("score_s", abs(assessment_region_division.ptt_ps_prop - assessment_region_division.ga_prop))
	assessment_region_division  = assessment_region_division.selectExpr("mean(score_s) as score")
	assessment_region_division = assessment_region_division.withColumn("index_m", lit("region_division"))
	assessment_region_division.persist()
	assessment_region_division.show()

	# -------------------------------------------- #

	assessment_target_assigns = cal_data_agg_for_assessment.select("potential", "p_sales", "quota", "sales", "total_potential", "total_p_sales", "total_quota", "p_quota", "total_sales")
	assessment_target_assigns = assessment_target_assigns.withColumn("potential_prop", assessment_target_assigns.potential / assessment_target_assigns.total_potential)
	assessment_target_assigns = assessment_target_assigns.withColumn("p_sales_prop", assessment_target_assigns.p_sales / assessment_target_assigns.total_p_sales)
	
	assessment_target_assigns  = assessment_target_assigns.withColumn("ptt_ps_prop", assessment_target_assigns.potential_prop * 0.6 + assessment_target_assigns.p_sales_prop * 0.4)
	assessment_target_assigns  = assessment_target_assigns.withColumn("quota_prop", assessment_target_assigns.quota / assessment_target_assigns.total_quota)
	
	assessment_target_assigns  = assessment_target_assigns.withColumn("ptt_ps_score", abs(assessment_target_assigns.ptt_ps_prop - assessment_target_assigns.quota_prop))
	assessment_target_assigns  = assessment_target_assigns.withColumn("quota_growth", assessment_target_assigns.quota - assessment_target_assigns.p_quota)
	assessment_target_assigns  = assessment_target_assigns.withColumn("sales_growth", assessment_target_assigns.sales / assessment_target_assigns.p_sales)
	
	assessment_target_assigns_total_sales = assessment_target_assigns.selectExpr("sum(sales) as sales")
	assessment_target_assigns = assessment_target_assigns.crossJoin(broadcast(assessment_target_assigns_total_sales))
	
	assessment_target_assigns = assessment_target_assigns.withColumn("qg_prop", assessment_target_assigns.quota_growth / (assessment_target_assigns.total_quota - assessment_target_assigns.total_p_sales))
	assessment_target_assigns = assessment_target_assigns.withColumn("sg_prop", assessment_target_assigns.sales_growth / (assessment_target_assigns.total_sales - assessment_target_assigns.total_p_sales))
	assessment_target_assigns = assessment_target_assigns.withColumn("q_s_score", assessment_target_assigns.qg_prop - assessment_target_assigns.sg_prop)
	
	assessment_target_assigns = assessment_target_assigns.selectExpr("mean(ptt_ps_score) * 0.7 + mean(q_s_score) * 0.3 as score")
	assessment_target_assigns = assessment_target_assigns.withColumn("index_m", lit("target_assigns"))
	assessment_target_assigns.persist()	
	assessment_target_assigns.show()

	# -------------------------------------------- #

	assessment_resource_assigns = cal_data_agg_for_assessment.select("potential", "p_sales", "budget", "call_time", "representative_time", "meeting_attendance", "total_potential", "total_p_sales", "total_budget", "total_place")
	assessment_resource_assigns = assessment_resource_assigns.withColumn("potential_prop", assessment_resource_assigns.potential / assessment_resource_assigns.total_potential)
	assessment_resource_assigns = assessment_resource_assigns.withColumn("p_sales_prop", assessment_resource_assigns.p_sales / assessment_resource_assigns.total_p_sales)
	
	assessment_resource_assigns  = assessment_resource_assigns.withColumn("ptt_ps_prop", assessment_resource_assigns.potential_prop * 0.6 + assessment_resource_assigns.p_sales_prop * 0.4)
	assessment_resource_assigns  = assessment_resource_assigns.withColumn("budget_prop", assessment_resource_assigns.budget / assessment_resource_assigns.total_budget)
	assessment_resource_assigns  = assessment_resource_assigns.withColumn("time_prop", assessment_resource_assigns.call_time / assessment_resource_assigns.representative_time * 5)
	assessment_resource_assigns  = assessment_resource_assigns.withColumn("place_prop", assessment_resource_assigns.meeting_attendance / assessment_resource_assigns.total_place)
	
	assessment_resource_assigns  = assessment_resource_assigns.withColumn("budget_score", abs(assessment_resource_assigns.ptt_ps_prop - assessment_resource_assigns.budget_prop))
	assessment_resource_assigns  = assessment_resource_assigns.withColumn("time_score", abs(assessment_resource_assigns.ptt_ps_prop - assessment_resource_assigns.time_prop))
	assessment_resource_assigns  = assessment_resource_assigns.withColumn("place_score", abs(assessment_resource_assigns.ptt_ps_prop - assessment_resource_assigns.place_prop))

	assessment_resource_assigns = assessment_resource_assigns.selectExpr("mean(budget_score) * 0.45 +mean(time_score) * 0.25 + mean(place_score) * 0.3 as score")
	assessment_resource_assigns = assessment_resource_assigns.withColumn("index_m", lit("resource_assigns"))
	assessment_resource_assigns.persist()	
	assessment_resource_assigns.show()
	
	# -------------------------------------------- #

	manage_time = cal_data_agg_for_assessment.select( "representative", "field_work", "one_on_one_coaching", "employee_kpi_and_compliance_check", \
													"admin_work", "kol_management", "business_strategy_planning", "team_meeting", "manager_time").distinct()

	manage_time = manage_time.groupBy("employee_kpi_and_compliance_check", "admin_work", "kol_management", "business_strategy_planning", "team_meeting", "manager_time") \
								.agg(
									sum(manage_time.field_work).alias("field_work"),
									sum(manage_time.one_on_one_coaching).alias("one_on_one_coaching")
									)
	
	# manage_time.show()
	# standard_time = spark.read.csv(standard_time_path, header="true")
	# standard_time.show(truncate=False)
	
	manage_time = manage_time.crossJoin(broadcast(standard_time))
	manage_time = manage_time.selectExpr("(abs(employee_kpi_and_compliance_check - employee_kpi_and_compliance_check_std) / manager_time \
									+ abs(admin_work - admin_work_std) / manager_time \
									+ abs(kol_management - kol_management_std) / manager_time \
									+ abs(business_strategy_planning - business_strategy_planning_std) / manager_time \
									+ abs(team_meeting - team_meeting_std) / manager_time \
									+ abs(field_work - field_work_std) / manager_time \
									+ abs(one_on_one_coaching - one_on_one_coaching_std) / manager_time) / 7 as score")
	manage_time = manage_time.withColumn("index_m", lit("manage_time"))
	manage_time.show()

	# -------------------------------------------- #

	manage_team = cal_data_agg_for_assessment.select("representative", "general_ability", "p_product_knowledge", \
												"p_sales_skills", "p_territory_management_ability", "p_work_motivation", "p_behavior_efficiency").distinct()

	manage_team = manage_team.withColumn("p_general_ability", (manage_team.p_territory_management_ability * 0.2 + manage_team.p_sales_skills * 0.25 + manage_team.p_product_knowledge * 0.25
															+ manage_team.p_behavior_efficiency * 0.15 + manage_team.p_work_motivation * 0.15) * 10)

	manage_team = manage_team.withColumn("space_delta", 100 - manage_team.p_general_ability)
	manage_team = manage_team.withColumn("growth_delta", manage_team.general_ability - manage_team.p_general_ability)
	manage_team = manage_team.selectExpr("0.2 - mean(growth_delta) / mean(space_delta) as score")
	manage_team = manage_team.withColumn("index_m", lit("manage_team"))
	manage_team.show()
	
	assessments = assessment_region_division.union(assessment_target_assigns) \
											.union(assessment_resource_assigns).union(manage_time).union(manage_team)
	
	# level_data = spark.read.csv(level_data_path, header="true") 
	level_data = level_data.withColumn("level1", level_data.level1.cast("double"))
	level_data = level_data.withColumn("level2", level_data.level2.cast("double"))
	
	particular_assessment = assessments.join(level_data, assessments.index_m == level_data.index, how="left")
	particular_assessment = particular_assessment.withColumn("level", 
								when(particular_assessment.score < particular_assessment.level1, 1) \
								.otherwise(when(particular_assessment.score > particular_assessment.level2, 3).otherwise(2)))
	particular_assessment = particular_assessment.select("index", "code", "level")
	particular_assessment.persist()
	particular_assessment.show()
	
	general_assessment = particular_assessment.selectExpr("cast(mean(level) as int) as level")
	general_assessment = general_assessment.withColumn("index", lit("general_performance"))
	general_assessment = general_assessment.withColumn("code", lit(5))
	general_assessment.persist()
	general_assessment.show()
	
	summary_data = general_assessment.unionByName(particular_assessment).drop("code")
	summary_data.createOrReplaceTempView('df')
	summary_data = spark.sql("""
		SELECT * FROM df PIVOT
		(
			SUM(`level`)
			FOR `index` in ('general_performance','resource_assigns','region_division', 'target_assigns', 'manage_time', 'manage_team')
		)
	""")
	summary_data.show()
	summary_data.drop("index", "level")
	summary_data.withColumn("proposal_id", lit("g_proposal_id")) \
			.withColumn("project_id", lit("g_project_id")) \
			.withColumn("period_id", lit("g_period_id")) \
			repartition(1).write.mode("overwrite").parquet(assessment_result)
	
	return {}


###############--------------------- functions ---------------------################
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
###############--------------------- functions ---------------------################