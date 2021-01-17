# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from ph_logs.ph_logs import phs3logger
import uuid
import time
import pandas as pd
import numpy as np
from pyspark.sql.types import *
from pyspark.ml import PipelineModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import udf, pandas_udf, PandasUDFType
from pyspark.sql.functions import desc, col
from pyspark.sql.functions import rank, lit, when, row_number
from pyspark.sql import Window


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
	model = PipelineModel.load(depends["model"])
	df_result = spark.read.parquet(depends["data"])
	df_lost = spark.read.parquet(depends["lost"])
	
	# output
	job_id = get_job_id(kwargs)
	run_id = get_run_id(kwargs)
	result_path_prefix = get_result_path(kwargs, run_id, job_id)
	prediction_path = result_path_prefix + kwargs["prediction_result"]
	positive_result_path = result_path_prefix + kwargs["positive_result"]
	negative_result_path = result_path_prefix + kwargs["negative_result"]
	tm = time.strftime("%Y-%m-%d_%H-%M-%S", time.localtime())
	final_predictions = get_final_result_path(kwargs, run_id, kwargs["final_predictions"], tm)
	final_positive_path = get_final_result_path(kwargs, run_id, kwargs["final_positive"], tm)
	final_negative_path = get_final_result_path(kwargs, run_id, kwargs["final_negative"], tm)
	final_lost_path = get_final_result_path(kwargs, run_id, kwargs["final_lost"], tm)
	final_report_path = get_final_result_path(kwargs, run_id, kwargs["final_report"], tm)

	assembler = VectorAssembler( \
						inputCols=["EFFTIVENESS_MOLE_NAME", "EFFTIVENESS_PRODUCT_NAME", "EFFTIVENESS_DOSAGE", "EFFTIVENESS_SPEC", \
									"EFFTIVENESS_PACK_QTY", "EFFTIVENESS_MANUFACTURER"], \
						outputCol="features")
	df_result = assembler.transform(df_result)
	
	df_predictions = model.transform(df_result)
	print(df_predictions.count())
	print(df_predictions.select("id").distinct().count())
	print(df_predictions.where((df_predictions.label == 1)).count())
	print(df_predictions.where((df_predictions.prediction == 1)).count())
	print(df_predictions.where((df_predictions.prediction == 1) & (df_predictions.label == 1)).count())
	# df_predictions.write.mode("overwrite").parquet(prediction_path)

	# df_predictions = df_predictions.withColumn("JACCARD_DISTANCE_MOLE_NAME", df_predictions.JACCARD_DISTANCE[0]) \
	# 			.withColumn("JACCARD_DISTANCE_DOSAGE", df_predictions.JACCARD_DISTANCE[1]) \
	# 			.drop("JACCARD_DISTANCE", "indexedFeatures").drop("rawPrediction", "probability")
	df_predictions = df_predictions.drop("indexedFeatures", "rawPrediction", "probability")
	
	# 5. 生成文件
	join_udf = udf(lambda x: ",".join(map(str,x)))
	
	@pandas_udf(StringType(), PandasUDFType.SCALAR)
	def join_pandas_udf(array_col):
		frame = { "array_col": array_col }
		
		df = pd.DataFrame(frame)
		
		def array_to_str(arr):
			if type(arr) != np.ndarray:
				s = ""
			else:
				s = ",".join(map(str,arr))
			return s
		
		df["RESULT"] = df["array_col"].apply(array_to_str)
		return df["RESULT"]
	
	
	df_predictions = similarity(df_predictions)
	df_predictions = df_predictions.na.fill("").drop("features")
	df_predictions.persist()						
	df_predictions.write.mode("overwrite").parquet(prediction_path)
	df_predictions.repartition(1).write.mode("overwrite").option("header", "true").csv(final_predictions)
	
	df_positive = df_predictions.where((df_predictions.prediction == 1.0) & (df_predictions.RANK == 1))
	df_positive.write.mode("overwrite").parquet(positive_result_path)   # TODO 记得打开
	
	# df_positive = df_positive.withColumn("MASTER_DOSAGE", join_pandas_udf(col("MASTER_DOSAGE"))) \
	# 						.withColumn("MANUFACTURER_NAME_STANDARD_WORDS", join_pandas_udf(col("MANUFACTURER_NAME_STANDARD_WORDS"))) \
	# 						.withColumn("MANUFACTURER_NAME_CLEANNING_WORDS", join_pandas_udf(col("MANUFACTURER_NAME_CLEANNING_WORDS"))) \
	# 						.withColumn("MANUFACTURER_NAME_STANDARD_WORDS_SEG", join_pandas_udf(col("MANUFACTURER_NAME_STANDARD_WORDS_SEG"))) \
	# 						.withColumn("MANUFACTURER_NAME_CLEANNING_WORDS_SEG", join_pandas_udf(col("MANUFACTURER_NAME_CLEANNING_WORDS_SEG"))) \
	# 						.withColumn("features", join_udf(col("features")))
	df_positive.repartition(1).write.mode("overwrite").option("header", "true").csv(final_positive_path)
	logger.warn("机器判断positive的条目写入完成")
	
	df_negative = df_predictions.where((df_predictions.prediction == 0.0) | ((df_predictions.prediction == 1.0) & (df_predictions.RANK != 1)))
	df_negative.write.mode("overwrite").parquet(negative_result_path)
	# df_negative = df_negative.withColumn("MASTER_DOSAGE", join_pandas_udf(col("MASTER_DOSAGE"))) \
	# 						.withColumn("MANUFACTURER_NAME_STANDARD_WORDS", join_pandas_udf(col("MANUFACTURER_NAME_STANDARD_WORDS"))) \
	# 						.withColumn("MANUFACTURER_NAME_CLEANNING_WORDS", join_pandas_udf(col("MANUFACTURER_NAME_CLEANNING_WORDS"))) \
	# 						.withColumn("MANUFACTURER_NAME_STANDARD_WORDS_SEG", join_pandas_udf(col("MANUFACTURER_NAME_STANDARD_WORDS_SEG"))) \
	# 						.withColumn("MANUFACTURER_NAME_CLEANNING_WORDS_SEG", join_pandas_udf(col("MANUFACTURER_NAME_CLEANNING_WORDS_SEG"))) \
	# 						.withColumn("features", join_udf(col("features")))
	df_negative.repartition(1).write.mode("overwrite").option("header", "true").csv(final_negative_path)
	logger.warn("机器判断negative的条目写入完成")
	
	# df_lost.write.mode("overwrite").parquet(final_lost_path)
	# df_lost.drop("JACCARD_DISTANCE").repartition(1).write.mode("overwrite").option("header", "true").csv(final_lost_path)
	# logger.warn("匹配第一步丢失条目写入完成")
	
	# 6. 结果统计
	all_count = df_predictions.count() + df_lost.count() # 数据总数
	ph_total = df_result.groupBy("id").agg({"label": "first"}).count()
	positive_count = df_positive.count()  # 机器判断pre=1条目
	negative_count = all_count - positive_count - df_lost.count()  # 机器判断pre=0条目
	matching_ratio = positive_count / all_count  # 匹配率
	
	
	# 7. 最终结果报告以csv形式写入s3
	report = [("data_matching_report", ),]
	report_schema = StructType([StructField('title',StringType(),True),])
	df_report = spark.createDataFrame(report, schema=report_schema).na.fill("")
	df_report = df_report.withColumn("数据总数", lit(str(all_count)))
	df_report = df_report.withColumn("进入匹配流程条目", lit(str(ph_total)))
	df_report = df_report.withColumn("丢失条目", lit(str(df_lost.count())))
	df_report = df_report.withColumn("机器匹配条目", lit(str(positive_count)))
	df_report = df_report.withColumn("机器无法匹配条目", lit(str(negative_count)))
	df_report = df_report.withColumn("匹配率", lit(str(matching_ratio)))
	df_report.show()
	df_report.repartition(1).write.mode("overwrite").option("header", "true").csv(final_report_path)
	logger.warn("final report csv文件写入完成")

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


def get_final_result_path(kwargs, run_id, final_key, tm):
	path_prefix = kwargs["final_prefix"]
	return path_prefix + "/" + tm + "/" + final_key


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
	

def similarity(df):
	df = df.withColumn("SIMILARITY", \
					(df.EFFTIVENESS_MOLE_NAME + df.EFFTIVENESS_PRODUCT_NAME + df.EFFTIVENESS_DOSAGE \
						+ df.EFFTIVENESS_SPEC + df.EFFTIVENESS_PACK_QTY + df.EFFTIVENESS_MANUFACTURER))
	windowSpec = Window.partitionBy("id").orderBy(desc("SIMILARITY"), desc("EFFTIVENESS_MOLE_NAME"), desc("EFFTIVENESS_DOSAGE"), desc("PACK_ID_STANDARD"))
	df = df.withColumn("RANK", row_number().over(windowSpec))
	df = df.where((df.RANK <= 5) | (df.label == 1.0))
	return df
################-----------------------------------------------------################