# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from ph_logs.ph_logs import phs3logger
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import desc
from pyspark.sql.functions import rank, lit, when, row_number
from pyspark.sql import Window
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.classification import MultilayerPerceptronClassificationModel
from pyspark.ml.classification import DecisionTreeClassificationModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import PipelineModel
from pyspark.ml.feature import VectorAssembler

def execute(**kwargs):
    """
        please input your code below
        get spark session: spark = kwargs["spark"]()
    """
    logger = phs3logger(kwargs["job_id"])
    spark = kwargs["spark"]()
    logger.info(kwargs)

    # input
    training_data_path = kwargs["training_data_path"]
    predictions_path = kwargs["predictions_path"]
    word_dict_encode_path = kwargs["word_dict_encode_path"]
    split_data_path = kwargs["split_data_path"]

    # output
    job_id = kwargs["job_id"]
    if not job_id:
        job_id = str(int(time.time()))
    result_path = kwargs["result_path"] + "/" + job_id

    # 1. load the data
    df_result = spark.read.parquet(training_data_path)  # 进入清洗流程的所有数据
    df_validate = df_result #.select("id", "label", "features").orderBy("id")
    df_encode = spark.read.parquet(word_dict_encode_path)
    df_all = spark.read.parquet(split_data_path)  # 带id的所有数据
    resultid = df_result.select("id").distinct()
    resultid_lst = resultid.toPandas()["id"].tolist()
    df_lost = df_all.where(~df_all.id.isin(resultid_lst))  # 第一步就丢失了的数据

    # 2. load predictions
    predictions = spark.read.parquet(predictions_path)

    # 3. compute accuracy on the test set
    evaluator = MulticlassClassificationEvaluator(labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    logger.warn("Test Error = %g " % (1.0 - accuracy))
    logger.warn("Test set accuracy = " + str(accuracy))

    # 4. Test with Pharbers defined methods
    result = predictions
    result_similarity = similarity(result)
    # result_similarity.write.mode("overwrite").parquet("s3a://ph-max-auto/2020-08-11/BPBatchDAG/refactor/zyyin/eia/0.0.2/for_analysis")
    # print("用于分析的的条目写入完成")
    result = result.withColumn("JACCARD_DISTANCE_MOLE_NAME", result.JACCARD_DISTANCE[0]) \
                .withColumn("JACCARD_DISTANCE_DOSAGE", result.JACCARD_DISTANCE[1]) \
                .drop("JACCARD_DISTANCE", "indexedFeatures").drop("rawPrediction", "probability")
    ph_total = result.groupBy("id").agg({"prediction": "first", "label": "first"}).count()
    logger.warn("数据总数： " + str(df_all.count()))
    logger.warn("进入匹配流程条目： " + str(ph_total))
    logger.warn("丢失条目： " + str(df_lost.count()))
    # result = result.where(result.PACK_ID_CHECK != "")
    # ph_total = result.groupBy("id").agg({"prediction": "first", "label": "first"}).count()
    # print("人工已匹配数据总数: " + str(ph_total))

    # 5. 尝试解决多高的问题
    df_true_positive = similarity(result.where(result.prediction == 1.0))
    df_true_positive = df_true_positive.where(df_true_positive.RANK == 1)
    machine_right_1 = df_true_positive
    df_true_positive.write.mode("overwrite").parquet(result_path)
    logger.warn("机器判断TP的条目写入完成")

    return {}

def similarity(df):
	df = df.withColumn("SIMILARITY", \
					(df.EFFTIVENESS_MOLE_NAME + df.EFFTIVENESS_PRODUCT_NAME + df.EFFTIVENESS_DOSAGE \
						+ df.EFFTIVENESS_SPEC + df.EFFTIVENESS_PACK_QTY + df.EFFTIVENESS_MANUFACTURER))
	windowSpec = Window.partitionBy("id").orderBy(desc("SIMILARITY"), desc("EFFTIVENESS_MOLE_NAME"), desc("EFFTIVENESS_DOSAGE"), desc("PACK_ID_STANDARD"))
	df = df.withColumn("RANK", row_number().over(windowSpec))
	df = df.where((df.RANK <= 5) | (df.label == 1.0))
	# df.repartition(1).write.format("parquet").mode("overwrite").save("s3a://ph-max-auto/2020-08-11/BPBatchDAG/refactor/zyyin/qilu/0.0.3/result_analyse/all_similarity_rank5")
	# print("写入完成")
	return df
