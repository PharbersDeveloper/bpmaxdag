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
    training_data_path = kwargs["training_data_path"]

    # output
    job_id = kwargs["job_id"]
    if not job_id:
        job_id = str(int(time.time()))
    validate_path = kwargs["validate_path"] + "/" + job_id
    model_path = kwargs["model_path"] + "/" + job_id

    training_data = spark.read.parquet(training_data_path)

    # 0. load the cleanning data
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
    df_training.show()

    # 1.2 构建测试集合
    df_test = df_test.join(df_result, how="left", on="id")
    df_test.write.mode("overwrite").parquet(validate_path)
    df_test.show()

    # Train a DecisionTree model.
    dt = DecisionTreeClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures")

    # Chain indexers and tree in a Pipeline
    pipeline = Pipeline(stages=[labelIndexer, featureIndexer, dt])

    # Train model.  This also runs the indexers.
    model = pipeline.fit(df_training)

    # model.write().overwrite().save("s3a://ph-max-auto/2020-08-11/BPBatchDAG/refactor/alfred/dt")
    model.write().overwrite().save(model_path)

    return {}
