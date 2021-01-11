# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from ph_logs.ph_logs import phs3logger
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import desc
from pyspark.sql.functions import rank
from pyspark.sql import Window
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml import Pipeline
from pyspark.ml import PipelineModel
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

def execute(**kwargs):
    """
        please input your code below
        get spark session: spark = kwargs["spark"]()
    """
    logger = phs3logger(kwargs["job_id"])
    spark = kwargs["spark"]()
    logger.info(kwargs)

    # input
    model_path = kwargs["model_path"]
    validate_path = kwargs["validate_path"]

    # output
    job_id = kwargs["job_id"]
    if not job_id:
        job_id = str(int(time.time()))
    validate_output_path = kwargs["validate_output_path"] + "/" + job_id

    # 加载测试集
    df_test = spark.read.parquet(validate_path)

    # load model
    model = PipelineModel.load(model_path)

    # Make predictions.
    df_predictions = model.transform(df_test)

    # Select example rows to display.
    df_predictions.show(10)
    df_predictions.select("prediction", "indexedLabel", "features").show(5)

    # save predictions
    df_predictions.write.mode("overwrite").parquet(validate_output_path)

    # Select (prediction, true label) and compute test error
    evaluator = MulticlassClassificationEvaluator(
        labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(df_predictions)
    logger.warn("Test Error = %g " % (1.0 - accuracy))

    treeModel = model.stages[2]
    # summary only
    logger.warn(treeModel)

    return {}
