# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

import uuid
import pandas as pd
from phcli.ph_logs.ph_logs import phs3logger
from pyspark.sql.types import *
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import udf, pandas_udf, PandasUDFType
from nltk.metrics import jaccard_distance as jd


def execute(**kwargs):
	"""
		please input your code below
		get spark session: spark = kwargs["spark"]()
	"""
	logger = phs3logger(kwargs["job_id"])
	spark = kwargs["spark"]()
	# spark = prepare()
	
	logger.info(kwargs)


###################-----------input---------------####################
	depends = get_depends_path(kwargs)
	g_cleaning_limit = int(kwargs["g_cleaning_limit"])
	if g_cleaning_limit > 0:
		df_cleanning = spark.read.parquet(depends["cleaning"]).limit(g_cleaning_limit)
	else:
		df_cleanning = spark.read.parquet(depends["cleaning"])
##################------------input----------------#######################


################----------------output----------------#########################
	job_id = get_job_id(kwargs)
	run_id = get_run_id(kwargs)
	result_path_prefix = get_result_path(kwargs, run_id, job_id)
	result_path = result_path_prefix + kwargs["cross_result"]
# 	drop_path = result_path_prefix + kwargs["cross_drop"]
###############----------------output------------------########################
	

    
###########--------------load file----------------------- ################

	df_standard = spark.read.parquet(depends["standard"])

###########--------------load file----------------------- ################



#########--------------main function--------------------#################   

	# 2. cross join

	df_cleanning = df_cleanning.repartition(int(kwargs["g_partitions_num"]))
	df_result = df_cleanning.crossJoin(broadcast(df_standard)).na.fill("")
    
	# 3. jaccard distance
	# 得到一个list，里面是mole_name 和 doasge 的 jd 数值
	df_result = df_result.withColumn("JACCARD_DISTANCE", \
				efftiveness_with_jaccard_distance( \
					df_result.MOLE_NAME, df_result.MOLE_NAME_STANDARD, \
					df_result.PACK_QTY, df_result.PACK_QTY_STANDARD \
					))
	df_result.persist()
    
	# 4. cutting for reduce the calculation
	g_mole_name_shared = float(kwargs["g_mole_name_shared"])
	g_pack_qty_shared = float(kwargs["g_pack_qty_shared"])
	g_repatition_shared = int(kwargs["g_repatition_shared"])
	# 需要换一个做法
	# df_drop_data = df_result.where((df_result.JACCARD_DISTANCE[0] >= g_mole_name_shared) & (df_result.JACCARD_DISTANCE[1] >= g_pack_qty_shared))
	# df_drop_data.repartition(g_repatition_shared).write.mode("overwrite").parquet(drop_path)	
	
	df_result = df_result.where((df_result.JACCARD_DISTANCE[0] < g_mole_name_shared) & (df_result.JACCARD_DISTANCE[1] < g_pack_qty_shared))  # 目前取了分子名和pack来判断
# 	df_result.repartition(g_repatition_shared).write.mode("overwrite").parquet(result_path)
    
#########--------------main function--------------------#################   

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
	由于高级的字符串匹配算法的时间复杂度过高，
	在大量的数据量的情况下需要通过简单的数据算法过滤掉不一样的数据
	这个是属于数据Cutting过程，所以这两个变量不是精确变量，不放在后期学习的过程中
"""
@pandas_udf(ArrayType(DoubleType()), PandasUDFType.SCALAR)
def efftiveness_with_jaccard_distance(mo, ms, po, ps):
	frame = {
		"MOLE_NAME": mo, "MOLE_NAME_STANDARD": ms,
		"PACK_QTY": po, "PACK_QTY_STANDARD": ps
	}
	df = pd.DataFrame(frame)

	df["MOLE_JD"] = df.apply(lambda x: jd(set(x["MOLE_NAME"]), set(x["MOLE_NAME_STANDARD"])), axis=1)
	df["PACK_JD"] = df.apply(lambda x: jd(set(x["PACK_QTY"].replace(".0", "")), set(x["PACK_QTY_STANDARD"].replace(".0", ""))), axis=1)
	df["RESULT"] = df.apply(lambda x: [x["MOLE_JD"], x["PACK_JD"]], axis=1)
	return df["RESULT"]
################-----------------------------------------------------################