# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

import uuid
import pandas as pd
from ph_logs.ph_logs import phs3logger
from pyspark.sql.types import *
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import udf, pandas_udf, PandasUDFType
from nltk.metrics.distance import jaro_winkler_similarity


def execute(**kwargs):
	"""
		please input your code below
		get spark session: spark = kwargs["spark"]()
	"""
	logger = phs3logger(kwargs["job_id"])
	spark = kwargs["spark"]()
	
	logger.info(kwargs)
	
	#input
	depends = get_depends_path(kwargs)
	df_result = spark.read.parquet(depends["input"])
	
	# output 	
	job_id = get_job_id(kwargs)
	run_id = get_run_id(kwargs)
	result_path_prefix = get_result_path(kwargs, run_id, job_id)
	result_path = result_path_prefix + kwargs["effective_result"]
	
	# 5. edit_distance is not very good for normalization probloms
	# we use jaro_winkler_similarity instead
	# if not good enough, change back to edit distance
	df_result = df_result.withColumn("EFFTIVENESS", \
					efftiveness_with_jaro_winkler_similarity( \
						df_result.MOLE_NAME, df_result.MOLE_NAME_STANDARD, \
						df_result.PRODUCT_NAME, df_result.PRODUCT_NAME_STANDARD, \
						df_result.DOSAGE, df_result.DOSAGE_STANDARD, \
						df_result.SPEC, df_result.SPEC_STANDARD, \
						df_result.PACK_QTY, df_result.PACK_QTY_STANDARD, \
						df_result.MANUFACTURER_NAME, df_result.MANUFACTURER_NAME_STANDARD, df_result.MANUFACTURER_NAME_EN_STANDARD, \
						df_result.SPEC_ORIGINAL
					))

	df_result = df_result.withColumn("EFFTIVENESS_MOLE_NAME", df_result.EFFTIVENESS[0]) \
					.withColumn("EFFTIVENESS_PRODUCT_NAME", df_result.EFFTIVENESS[1]) \
					.withColumn("EFFTIVENESS_DOSAGE", df_result.EFFTIVENESS[2]) \
					.withColumn("EFFTIVENESS_SPEC", df_result.EFFTIVENESS[3]) \
					.withColumn("EFFTIVENESS_PACK_QTY", df_result.EFFTIVENESS[4]) \
					.withColumn("EFFTIVENESS_MANUFACTURER", df_result.EFFTIVENESS[5]) \
					.drop("EFFTIVENESS")

	df_result.write.mode("overwrite").parquet(result_path)
	logger.info("第一轮完成，写入完成")
	
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
	
	
"""
	由于Edit Distance不是一个相似度算法，当你在计算出相似度之后还需要通过一定的辅助算法计算
	Normalization。但是由于各个地方的Normalization很有可能产生误差错误，
	需要一个统一的Similarity的计算方法，去消除由于Normalization来产生的误差
	优先使用1989年提出的  Jaro Winkler distance

	The Jaro Winkler distance is an extension of the Jaro similarity in:

			William E. Winkler. 1990. String Comparator Metrics and Enhanced
			Decision Rules in the Fellegi-Sunter Model of Record Linkage.
			Proceedings of the Section on Survey Research Methods.
			American Statistical Association: 354-359.
		such that:

			jaro_winkler_sim = jaro_sim + ( l * p * (1 - jaro_sim) )
"""
@pandas_udf(ArrayType(DoubleType()), PandasUDFType.SCALAR)
def efftiveness_with_jaro_winkler_similarity(mo, ms, po, ps, do, ds, so, ss, qo, qs, mf, mfc, mfe, spec):
	frame = {
		"MOLE_NAME": mo, "MOLE_NAME_STANDARD": ms,
		"PRODUCT_NAME": po, "PRODUCT_NAME_STANDARD": ps,
		"DOSAGE": do, "DOSAGE_STANDARD": ds,
		"SPEC": so, "SPEC_STANDARD": ss,
		"PACK_QTY": qo, "PACK_QTY_STANDARD": qs,
		"MANUFACTURER_NAME": mf, "MANUFACTURER_NAME_STANDARD": mfc, "MANUFACTURER_NAME_EN_STANDARD": mfe,
		"SPEC_ORIGINAL": spec
	}
	df = pd.DataFrame(frame)

	df["MOLE_JWS"] = df.apply(lambda x: jaro_winkler_similarity(x["MOLE_NAME"], x["MOLE_NAME_STANDARD"]), axis=1)
	df["PRODUCT_JWS"] = df.apply(lambda x: 1 if x["PRODUCT_NAME"] in x ["PRODUCT_NAME_STANDARD"] \
										else 1 if x["PRODUCT_NAME_STANDARD"] in x ["PRODUCT_NAME"] \
										else jaro_winkler_similarity(x["PRODUCT_NAME"], x["PRODUCT_NAME_STANDARD"]), axis=1)
	df["DOSAGE_JWS"] = df.apply(lambda x: 1 if x["DOSAGE"] in x ["DOSAGE_STANDARD"] \
										else 1 if x["DOSAGE_STANDARD"] in x ["DOSAGE"] \
										else jaro_winkler_similarity(x["DOSAGE"], x["DOSAGE_STANDARD"]), axis=1)
	df["SPEC_JWS"] = df.apply(lambda x: 1 if x["SPEC"].strip() ==  x["SPEC_STANDARD"].strip() \
										else 0 if ((x["SPEC"].strip() == "") or (x["SPEC_STANDARD"].strip() == "")) \
										else 1 if x["SPEC"].strip() in x["SPEC_STANDARD"].strip() \
										else 1 if x["SPEC_STANDARD"].strip() in x["SPEC"].strip() \
										else jaro_winkler_similarity(x["SPEC"].strip(), x["SPEC_STANDARD"].strip()), axis=1)
	df["PACK_QTY_JWS"] = df.apply(lambda x: 1 if (x["PACK_QTY"].replace(".0", "") == x["PACK_QTY_STANDARD"].replace(".0", "")) \
										| (("喷" in x["PACK_QTY"]) & (x["PACK_QTY"] in x["SPEC_ORIGINAL"])) \
										else 0, axis=1)
	df["MANUFACTURER_NAME_CH_JWS"] = df.apply(lambda x: 1 if x["MANUFACTURER_NAME"] in x ["MANUFACTURER_NAME_STANDARD"] \
										else 1 if x["MANUFACTURER_NAME_STANDARD"] in x ["MANUFACTURER_NAME"] \
										else jaro_winkler_similarity(x["MANUFACTURER_NAME"], x["MANUFACTURER_NAME_STANDARD"]), axis=1)
	df["MANUFACTURER_NAME_EN_JWS"] = df.apply(lambda x: 1 if x["MANUFACTURER_NAME"] in x ["MANUFACTURER_NAME_EN_STANDARD"] \
										else 1 if x["MANUFACTURER_NAME_EN_STANDARD"] in x ["MANUFACTURER_NAME"] \
										else jaro_winkler_similarity(x["MANUFACTURER_NAME"].upper(), x["MANUFACTURER_NAME_EN_STANDARD"].upper()), axis=1)
	df["MANUFACTURER_NAME_MINUS"] = df["MANUFACTURER_NAME_CH_JWS"] - df["MANUFACTURER_NAME_EN_JWS"]
	df.loc[df["MANUFACTURER_NAME_MINUS"] < 0.0, "MANUFACTURER_NAME_JWS"] = df["MANUFACTURER_NAME_EN_JWS"]
	df.loc[df["MANUFACTURER_NAME_MINUS"] >= 0.0, "MANUFACTURER_NAME_JWS"] = df["MANUFACTURER_NAME_CH_JWS"]

	df["RESULT"] = df.apply(lambda x: [x["MOLE_JWS"], \
										x["PRODUCT_JWS"], \
										x["DOSAGE_JWS"], \
										x["SPEC_JWS"], \
										x["PACK_QTY_JWS"], \
										x["MANUFACTURER_NAME_JWS"], \
										], axis=1)
	return df["RESULT"]
################-----------------------------------------------------################
	