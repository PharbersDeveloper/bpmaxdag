# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger
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
    model_path = kwargs["model_path"]

    # output
    job_id = kwargs["job_id"]
    if not job_id:
        job_id = str(int(time.time()))
    result_path = kwargs["result_path"] + "/" + job_id

    # 1. load the data
    df_result = spark.read.parquet(training_data_path)  # 进入清洗流程的所有数据
    df_validate = df_result #.select("id", "label", "features").orderBy("id")

    # 2. load model
    # model = PipelineModel.load("s3a://ph-max-auto/2020-08-11/BPBatchDAG/refactor/alfred/dt")
    model = PipelineModel.load(model_path)

    # 3. compute accuracy on the test set
    predictions = model.transform(df_validate)

    # 4. save predictions
    predictions.write.mode("overwrite").parquet(result_path)

    return {}
