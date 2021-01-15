# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from ph_logs.ph_logs import phs3logger
import uuid
import time
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import desc
from pyspark.sql.functions import rank
from pyspark.sql import Window
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler


def execute(**kwargs):
	"""
		please input your code below
		get spark session: spark = kwargs["spark"]()
	"""
	logger = phs3logger(kwargs["job_id"])
	spark = kwargs["spark"]()
	# spark = prepare()
	
	logger.info(kwargs)

	#input
	depends = get_depends_path(kwargs)
	training_data = spark.read.parquet(depends["input"])
	
	# output
	job_id = get_job_id(kwargs)
	run_id = get_run_id(kwargs)
	result_path_prefix = get_result_path(kwargs, run_id, job_id)
	model_path = result_path_prefix + kwargs["model_result"]
	validate_path = result_path_prefix + kwargs["model_validate"]
	
	# 0. load the cleanning data
	# features
	assembler = VectorAssembler( \
					inputCols=["EFFTIVENESS_MOLE_NAME", "EFFTIVENESS_PRODUCT_NAME", "EFFTIVENESS_DOSAGE", "EFFTIVENESS_SPEC", \
								"EFFTIVENESS_PACK_QTY", "EFFTIVENESS_MANUFACTURER"], \
					outputCol="features")
	training_data = assembler.transform(training_data)
	
	
	df_cleanning = training_data.select("id").distinct()
	# Split the data into training and test sets (30% held out for testing)
	(df_training, df_test) = df_cleanning.randomSplit([0.7, 0.3])

	# 1. load the training data
	# 准备训练集合
	df_result = training_data
	df_result = df_result.select("id", "label", "features")
	labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(df_result)
	featureIndexer = VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=6).fit(df_result)

	# 1.1 构建训练集合
	df_training = df_training.join(df_result, how="left", on="id")
	# df_training.show()

	# 1.2 构建测试集合
	df_test = df_test.join(df_result, how="left", on="id")
	# df_test.write.mode("overwrite").parquet(validate_path)
	# df_test.show()

	# Train a DecisionTree model.
	dt = DecisionTreeClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures")

	# Chain indexers and tree in a Pipeline
	pipeline = Pipeline(stages=[labelIndexer, featureIndexer, dt])

	# Train model.  This also runs the indexers.
	model = pipeline.fit(df_training)

	# model.write().overwrite().save("s3a://ph-max-auto/2020-08-11/BPBatchDAG/refactor/alfred/dt")
	model.write().overwrite().save(model_path)
	
	# validata the model
	# Make predictions.
	df_predictions = model.transform(df_test)

	# Select example rows to display.
	df_predictions.show(10)
	df_predictions.select("prediction", "indexedLabel", "features").show(5)

	# save predictions
	# df_predictions.write.mode("overwrite").parquet(model_validata)

	# Select (prediction, true label) and compute test error
	evaluator = MulticlassClassificationEvaluator(
		labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
	accuracy = evaluator.evaluate(df_predictions)
	logger.info("Test Error = %g " % (1.0 - accuracy))

	# treeModel = model.stages[2]
	# summary only
	# logger.warn(treeModel)

	# Create pandas data frame and convert it to a spark data frame 
	pandas_df = pd.DataFrame({"MODEL":["Decision Tree"], "ACCURACY": [accuracy]})
	spark_df = spark.createDataFrame(pandas_df)
	spark_df.repartition(1).write.mode("overwrite").parquet(validate_path)
	
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
################-----------------------------------------------------################