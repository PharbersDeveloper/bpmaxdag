# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
import pandas as pd
import uuid
from functools import partial
from pyspark.sql.types import *
from pyspark.sql.functions import sum, rand, lit, when, col
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
	cal_data = spark.read.parquet(depends["cal_path"])
	weigthages = spark.read.csv(kwargs["weight_path"], header="true")
	# manager = spark.read.parquet(kwargs["manage_path"])
	# standard_time = spark.read.csv(kwargs["standard_time_path"], header="true")
	# level_data = spark.read.csv(kwargs["level_data_path"], header="true")
	curves = spark.read.csv(kwargs["curves_path"], header="true")
################------------input----------------################


###############----------------output-------------################
	job_id = get_job_id(kwargs)
	run_id = get_run_id(kwargs)
	result_path_prefix = get_result_path(kwargs, run_id, job_id)
	cal_result = result_path_prefix + kwargs["cal_result"]
###############----------------output--------------##################

	curves = curves.withColumn("x", curves.x.cast("double"))
	curves = curves.withColumn("y", curves.y.cast("double"))
	curves = curves.toPandas()
	# print(curves)
	
	# manager = withColumn("tid", lit("1"))
	weigthages = weigthages.withColumn("tid", lit("1"))
	# mw = manager.join(weigthages, on="tid", how="left")
	cal_data = cal_data.withColumn("tid", lit("1"))
	cal_data = cal_data.join(weigthages, on="tid", how="left")
	cal_data = cal_data.drop("tid")
	cal_data.persist()
	
	# weigthages.show()
	# standard_time.show()
	
	cal_data_tp = cal_data.selectExpr("sum(potential) as total_potential")
	cal_data_ts = cal_data.selectExpr("sum(p_sales) as total_p_sales")
	cal_data_tb = cal_data.selectExpr("sum(budget) as total_budget")
	cal_data_tq = cal_data.selectExpr("sum(quota) as total_quota")
	cal_data_tt = cal_data.selectExpr("sum(meeting_attendance) as total_place")
	cal_data = cal_data.crossJoin(cal_data_tp).crossJoin(cal_data_ts).crossJoin(cal_data_tb).crossJoin(cal_data_tq).crossJoin(cal_data_tt)
	cal_data = cal_data.withColumn("manager_time", lit(100))
	# cal_data.printSchema()

	cal_data = cal_data.withColumn("level_factor", 
				0.8 * (cal_data.potential / cal_data.total_potential) + 0.2 * (cal_data.p_sales / cal_data.total_p_sales))
	cal_data = cal_data.withColumn("work_motivation", 
				cal_data.p_work_motivation + 0.15 * (10 - cal_data.p_work_motivation) * (cal_data.performance_review + cal_data.career_development_guide))
	cal_data = cal_data.withColumn("territory_management_ability", 
				cal_data.p_territory_management_ability + 0.3 * (10 - cal_data.p_territory_management_ability) * cal_data.territory_management_training)
	cal_data = cal_data.withColumn("sales_skills", 
				cal_data.p_sales_skills + 0.3 * (10 - cal_data.p_sales_skills) * cal_data.sales_skills_training)
	cal_data = cal_data.withColumn("product_knowledge", 
				cal_data.p_product_knowledge + 0.3 * (10 - cal_data.p_product_knowledge) * cal_data.product_knowledge_training)
	cal_data = cal_data.withColumn("quota_growth", cal_data.quota/cal_data.p_quota)
	cal_data = cal_data.withColumn("budget_prop", cal_data.budget / cal_data.total_budget)
	cal_data = cal_data.withColumn("level", when(cal_data.level_factor > 0.15, 3).otherwise(when(cal_data.level_factor <= 0.05, 1).otherwise(2)))
	cal_data = cal_data.withColumn("quota_restriction_factor", when(((cal_data.quota_growth >= 0.5) & (cal_data.quota_growth <= 2.0)), 1.0).otherwise(0.6))

	cal_data = cal_curves_result_prepare(cal_data, ["curve09"])
	cal_data = cal_curves_result_acc(cal_data, curves, "one_on_one_coaching", "behavior_efficiency_factor")
	# cal_data = cal_data.withColumn("behavior_efficiency_factor", cal_curves_result(cal_data.curve_name, cal_data.one_on_one_coaching))
	
	cal_data = cal_curves_result_prepare(cal_data, ["curve10", "curve11", "curve12"], "level", [1, 2, 3])
	cal_data = cal_curves_result_acc(cal_data, curves, "call_time", "call_time_index")
	# cal_data = cal_data.withColumn("call_time_index", cal_curves_result(cal_data.curve_name, cal_data.call_time))
	
	cal_data = cal_curves_result_prepare(cal_data, ["curve14"])
	cal_data = cal_curves_result_acc(cal_data, curves, "quota_restriction_factor", "quota_restriction_index")
	# cal_data = cal_data.withColumn("quota_restriction_index", cal_curves_result(cal_data.curve_name, cal_data.quota_restriction_factor))
	
	cal_data = cal_curves_result_prepare(cal_data, ["curve16"])
	cal_data = cal_curves_result_acc(cal_data, curves, "field_work", "field_work_index")
	# cal_data = cal_data.withColumn("field_work_index", cal_curves_result(cal_data.curve_name, cal_data.field_work))
	
	cal_data = cal_curves_result_prepare(cal_data, ["curve18"])
	cal_data = cal_curves_result_acc(cal_data, curves, "business_strategy_planning", "business_strategy_planning_index")
	# cal_data = cal_data.withColumn("business_strategy_planning_index", cal_curves_result(cal_data.curve_name, cal_data.business_strategy_planning))
	
	cal_data = cal_curves_result_prepare(cal_data, ["curve18"])
	cal_data = cal_curves_result_acc(cal_data, curves, "admin_work", "admin_work_index")
	# cal_data = cal_data.withColumn("admin_work_index", cal_curves_result(cal_data.curve_name, cal_data.admin_work))
	
	cal_data = cal_curves_result_prepare(cal_data, ["curve18"])
	cal_data = cal_curves_result_acc(cal_data, curves, "employee_kpi_and_compliance_check", "employee_kpi_and_compliance_check_index")
	# cal_data = cal_data.withColumn("employee_kpi_and_compliance_check_index", cal_curves_result(cal_data.curve_name, cal_data.employee_kpi_and_compliance_check))
	
	cal_data = cal_curves_result_prepare(cal_data, ["curve18"])
	cal_data = cal_curves_result_acc(cal_data, curves, "team_meeting", "team_meeting_index")
	# cal_data = cal_data.withColumn("team_meeting_index", cal_curves_result(cal_data.curve_name, cal_data.team_meeting))
	
	cal_data = cal_curves_result_prepare(cal_data, ["curve18"])
	cal_data = cal_curves_result_acc(cal_data, curves, "kol_management", "kol_management_index")
	# cal_data = cal_data.withColumn("kol_management_index", cal_curves_result(cal_data.curve_name, cal_data.kol_management))
	
	cal_data = cal_curves_result_prepare(cal_data, ["curve02", "curve03", "curve04"], "level", [1, 2, 3])
	cal_data = cal_curves_result_acc(cal_data, curves, "budget_prop", "budget_factor")
	# cal_data = cal_data.withColumn("budget_factor", cal_curves_result(cal_data.curve_name, cal_data.budget_prop))
	
	cal_data = cal_curves_result_prepare(cal_data, ["curve05", "curve06", "curve07"], "level", [1, 2, 3])
	cal_data = cal_curves_result_acc(cal_data, curves, "meeting_attendance", "meeting_attendance_factor")
	# cal_data = cal_data.withColumn("meeting_attendance_factor", cal_curves_result(cal_data.curve_name, cal_data.meeting_attendance))
	cal_data.persist()

	cal_data = cal_data.withColumn("behavior_efficiency", 
				cal_data.p_behavior_efficiency + 0.3 * (10 - cal_data.p_behavior_efficiency) * cal_data.behavior_efficiency_factor)
	cal_data = cal_data.withColumn("deployment_quality", 
				cal_data.business_strategy_planning_index * cal_data.business_strategy_planning_index_w + \
				cal_data.admin_work_index * cal_data.admin_work_index_w + \
				cal_data.employee_kpi_and_compliance_check_index * cal_data.employee_kpi_and_compliance_check_index_w + \
				cal_data.kol_management_index * cal_data.kol_management_index_w + \
				cal_data.team_meeting_index * cal_data.team_meeting_index_w)
	cal_data = cal_data.withColumn("customer_relationship", 
				(cal_data.budget_factor * cal_data.budget_factor_w + cal_data.meeting_attendance_factor * cal_data.meeting_attendance_factor_w) * 100)
				
	cal_data = cal_data.withColumn("general_ability", 
				10 * (cal_data.territory_management_ability * cal_data.territory_management_ability_w + \
					cal_data.sales_skills * cal_data.sales_skills_w + \
					cal_data.product_knowledge * cal_data.product_knowledge_w + \
					cal_data.behavior_efficiency * cal_data.behavior_efficiency_w + \
					cal_data.work_motivation * cal_data.work_motivation_w))

	cal_data = cal_data.withColumn("rep_ability_efficiency", 
				(cal_data.general_ability * cal_data.general_ability_w + \
				cal_data.call_time_index * cal_data.call_time_index_w + \
				cal_data.quota_restriction_index * cal_data.quota_restriction_index_w))
	
	cal_data = cal_data.withColumn("sales_performance", 
				cal_data.rep_ability_efficiency * cal_data.rep_ability_efficiency_w + \
				cal_data.field_work_index * cal_data.field_work_index_w + \
				cal_data.deployment_quality * cal_data.deployment_quality_w)
				
	cal_data = cal_data.withColumn("offer_attractiveness", 
				cal_data.sales_performance * cal_data.sales_performance_w + \
				cal_data.customer_relationship * cal_data.customer_relationship_w)
	
	cal_data = cal_curves_result_prepare(cal_data, ["curve28"])
	cal_data = cal_curves_result_acc(cal_data, curves, "offer_attractiveness", "share_delta_factor")
	# cal_data = cal_data.withColumn("share_delta_factor", cal_curves_result(cal_data.curve_name, cal_data.offer_attractiveness))
	
	cal_data = cal_data.withColumn("share", cal_data.p_share * (1.0 + cal_data.share_delta_factor))
	cal_data = cal_data.withColumn("sales", cal_data.potential / 4 * cal_data.share)
	cal_data.persist()
	# cal_data.show()
	
	cal_data_res = cal_data.groupBy("representative_id").agg(sum(cal_data.sales).alias("rep_sales"), sum(cal_data.quota).alias("rep_quota"))
	cal_data = cal_data.join(cal_data_res, on="representative_id", how="inner")

	cal_data = cal_data.withColumn("rep_quota_achv", cal_data.rep_sales / cal_data.rep_quota) \
						.withColumn("target", cal_data.p_target) \
						.withColumn("target_coverage", cal_data.p_target_coverage)

	cal_data = cal_data.withColumn("work_motivation", 
									when(((cal_data.rep_quota_achv >= 0.9) & (cal_data.rep_quota_achv <= 1.2)), 
											cal_data.work_motivation + 0.2 * (10 - cal_data.work_motivation))
											.otherwise(cal_data.work_motivation))
											
	cal_data = cal_data.withColumn("class1", when(((cal_data.behavior_efficiency >= 0) & (cal_data.behavior_efficiency < 3)), 1)
									.otherwise(when(((cal_data.behavior_efficiency <=3) & (cal_data.behavior_efficiency < 6)), 2)
									.otherwise(when(((cal_data.behavior_efficiency >= 6) & (cal_data.behavior_efficiency < 8)), 3)
									.otherwise(4))))
	cal_data = cal_data.withColumn("class2", when(((cal_data.behavior_efficiency >= 0) & (cal_data.behavior_efficiency < 3)), 1)
									.otherwise(when(((cal_data.behavior_efficiency <=3) & (cal_data.behavior_efficiency < 6)), 2)
									.otherwise(when(((cal_data.behavior_efficiency >= 6) & (cal_data.behavior_efficiency < 8)), 3)
									.otherwise(4))))

	cal_data = cal_data.withColumn("target_coverage", when(cal_data.class1 == 1, cal_data.target_coverage - rand() * 5 + 5)
																.otherwise(when(cal_data.class1 == 2, rand() * 5)
																.otherwise(when(cal_data.class1 == 3, rand() * 5)
																.otherwise(cal_data.target_coverage + rand() * 5))))

	cal_data = cal_data.withColumn("high_target_m", when(cal_data.class1 == 1, rand() + 13)
																.otherwise(when(cal_data.class1 == 2, rand() + 14)
																.otherwise(when(cal_data.class1 == 3, 2 * rand() + 16)
																.otherwise(rand() * 3 + 19))))

	cal_data = cal_data.withColumn("middle_target_m", when(cal_data.class1 == 1, rand() + 13)
																.otherwise(when(cal_data.class1 == 2, rand() + 13)
																.otherwise(when(cal_data.class1 == 3, rand() + 12)
																.otherwise(rand() + 12))))

	cal_data = cal_data.withColumn("low_target_m", when(cal_data.class1 == 1, rand() + 13)
																.otherwise(when(cal_data.class1 == 2, rand() + 13)
																.otherwise(when(cal_data.class1 == 3, rand() + 12)
																.otherwise(rand() + 11))))

	cal_data = cal_data.withColumn("high_target", when(cal_data.class2 == 1, cal_data.high_target_m - (rand() + 1))
																.otherwise(when(cal_data.class2 == 2, cal_data.high_target_m - rand())
																.otherwise(when(cal_data.class2 == 3, cal_data.high_target_m + rand())
																.otherwise(cal_data.high_target_m + 1))))

	cal_data = cal_data.withColumn("middle_target", when(cal_data.class2 == 1, cal_data.middle_target_m - 2)
																.otherwise(when(cal_data.class2 == 2, cal_data.middle_target_m - 1)
																.otherwise(when(cal_data.class2 == 3, cal_data.middle_target_m + rand())
																.otherwise(cal_data.middle_target_m + 1))))

	cal_data = cal_data.withColumn("low_target", when(cal_data.class2 == 1, cal_data.low_target_m - 2)
																.otherwise(when(cal_data.class2 == 2, cal_data.low_target_m - 1)
																.otherwise(when(cal_data.class2 == 3, cal_data.low_target_m + rand())
																.otherwise(cal_data.low_target_m + 1))))
	

	# cal_data.printSchema()
	cal_data = cal_data.select("hospital", "hospital_level", "budget", "meeting_attendance", "product", "quota", "call_time",
								"one_on_one_coaching", "field_work", "performance_review", "product_knowledge_training",
								"territory_management_training", "representative", "sales_skills_training", "career_development_guide",
								"employee_kpi_and_compliance_check", "admin_work", "kol_management", "business_strategy_planning",
								"team_meeting", "potential", "p_sales", "p_quota", "p_share", "life_cycle", "representative_time",
								"p_territory_management_ability", "p_sales_skills", "p_product_knowledge", "p_behavior_efficiency",
								"p_work_motivation", "total_potential", "total_p_sales", "total_quota", "total_place", "manager_time", 
								"work_motivation", "territory_management_ability", "sales_skills", 
								"product_knowledge", "behavior_efficiency", "general_ability", "target", "target_coverage", 
								"high_target", "middle_target", "low_target", "share", "sales")	
	
	cal_data.repartition(1).write.mode("overwrite").parquet(cal_result)

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
	



def cal_curves_result_prepare(df, curves, adjust_col="", condi = []):
	
	@pandas_udf(StringType(), PandasUDFType.SCALAR)
	def cal_curves_name(ad):
		frame = {
			"ad": ad
		}
		df = pd.DataFrame(frame)
		
		df["result"] = df["ad"].apply(lambda x: curves[condi.index(x)])
		return df["result"]

	if len(condi) == 0:
		df = df.withColumn("curve_name", lit(curves[0]))
	else:
		df = df.withColumn("curve_name", cal_curves_name(col(adjust_col)))

	# df = df.withColumn(df, "input_col", lit(input_col))
	# df = df.withColumn(df, "output_col", lit(output_col))
	return df


def cal_curves_result_acc(df, curves, input_col, output_col):
	
	@pandas_udf(DoubleType(), PandasUDFType.SCALAR)
	def cal_curves_result(cn, s):
		frame = {
			"cn": cn,
			"source": s
		}
		df = pd.DataFrame(frame)
	
		def curve_func(cn, input):
			curve_data = curves[curves['name'] == cn]
			curve_data["ti"] = curve_data["x"]
			curve_data = curve_data.set_index("ti")
			if input < curve_data.min()["x"]:
				return curve_data.min()["y"]
			
			if input > curve_data.max()["x"]:
				return curve_data.max()["y"]
		
			curve_data = curve_data.reset_index(drop=True)
			curve_data["in"] = abs(input - curve_data["x"])
			curve_data = curve_data.set_index("in")
			
			left = curve_data.min()
			tmp = curve_data[curve_data["x"] != left["x"]]
			right = tmp.min()
			
			if left.x <= right.x:
				return (1.0 - (input - left.x) / (right.x - left.x)) * left.y + (1.0 - (right.x - input) / (right.x - left.x)) * right.y
			else:
				return (1.0 - (input - right.x) / (left.x - right.x)) * right.y + (1.0 - (left.x - input) / (left.x - right.x)) * left.y
	
		df["result"] = df.apply(lambda x: curve_func(x["cn"], x["source"]), axis=1)
		return df["result"]
		
	return df.withColumn(output_col, cal_curves_result(df.curve_name, df[input_col]))
		
################--------------------- functions ---------------------################