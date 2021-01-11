# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is gen cube job

"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import lit
from pyspark.sql.functions import floor
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import udf
from pyspark.sql.functions import col
from pyspark.sql.streaming import *
import logging
import os
import string
from uuid import uuid4


def execute(**kwargs):

    logging.basicConfig(format='%(asctime)s %(filename)s %(funcName)s %(lineno)d %(message)s')
    logger = logging.getLogger('driver_logger')
    logger.setLevel(logging.INFO)
    logger.info("Origin kwargs = {}.".format(str(kwargs)))

    # input required
    max_result_path = kwargs['max_result_path']
    if max_result_path == u'default':
        raise Exception("Invalid max_result_path!", max_result_path)

    cleancube_result_path = kwargs['cleancube_result_path']
    if cleancube_result_path == u'default':
        jobName = "cleancube"
        version = kwargs['version']
        if not version:
            raise Exception("Invalid version!", version)
        runId = kwargs['run_id']
        if runId == u'default':
            runId = str(uuid4())
            logger.info("runId is " + runId)
        jobId = kwargs['job_id']
        if jobId == u'default':
            jobId = str(uuid4())
            logger.info("jobId is " + jobId)
        destPath = "s3a://ph-max-auto/" + version +"/jobs/runId_" + runId + "/" + jobName +"/jobId_" + jobId
        logger.info("DestPath is {}.".format(destPath))
        cleancube_result_path = destPath + "/content"
    logger.info("cleancube_result_path is {}.".format(cleancube_result_path))

    start = kwargs['start']
    end = kwargs['end']

    sd = int(start)
    ed = int(end)
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("data cube clean job") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.instance", "1") \
        .config("spark.executor.memory", "1g") \
        .config('spark.sql.codegen.wholeStage', False) \
        .getOrCreate()

    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    if access_key is not None:
        spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
        spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
        spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
        spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
        # spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
        spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.cn-northwest-1.amazonaws.com.cn")

    logger.info("preparing data from hive")

    """
        |-- PHA: string (nullable = true)
        |-- Province: string (nullable = true)
        |-- City: string (nullable = true)
        |-- Date: double (nullable = true)
        |-- Molecule: string (nullable = true)
        |-- Prod_Name: string (nullable = true)
        |-- BEDSIZE: double (nullable = true)
        |-- PANEL: double (nullable = true)
        |-- Seg: double (nullable = true)
        |-- Predict_Sales: double (nullable = true)
        |-- Predict_Unit: double (nullable = true)
        |-- version: string (nullable = true)
        |-- company: string (nullable = true)
    """

    readingSchema = \
        StructType([ \
            StructField("PHA", StringType()), \
            StructField("Province", StringType()), \
            StructField("City", StringType()), \
            StructField("Date", DoubleType()), \
            StructField("Molecule", StringType()), \
            StructField("Prod_Name", StringType()), \
            StructField("BEDSIZE", DoubleType()), \
            StructField("PANEL", DoubleType()), \
            StructField("Seg", DoubleType()), \
            StructField("Predict_Sales", DoubleType()), \
            StructField("Predict_Unit", DoubleType()), \
            StructField("version", StringType()), \
            StructField("company", StringType()) \
        ])

    jid = str(uuid4())
    reading = spark.read.schema(readingSchema).parquet(max_result_path)
    min2prod_udf = udf(lambda x: string.split(x, "|")[0], StringType())

    # 1. 数据清洗
    reading.filter((col("Date") < ed) & (col("Date") > sd)) \
        .withColumnRenamed("Province", "PROVINCE_NAME") \
        .withColumnRenamed("City", "CITY_NAME") \
        .withColumnRenamed("company", "COMPANY") \
        .withColumnRenamed("Prod_Name", "MIN") \
        .withColumnRenamed("Province", "PROVINCE_NAME") \
        .withColumnRenamed("City", "CITY_NAME") \
        .withColumnRenamed("Molecule", "MOLE_NAME") \
        .withColumnRenamed("company", "COMPANY") \
        .withColumnRenamed("Prod_Name", "MIN") \
        .withColumn("PRODUCT_NAME", min2prod_udf(col("MIN"))) \
        .withColumn("YEAR", floor(col("Date") / 100)) \
        .withColumn("MONTH", floor(col("Date") - col("YEAR") * 100)) \
        .withColumn("QUARTER", floor((col("MONTH")- 1) / 3 + 1)) \
        .withColumn("MKT", col("COMPANY")) \
        .fillna(0.0) \
        .withColumnRenamed("Predict_Sales", "SALES_VALUE") \
        .withColumnRenamed("Predict_Unit", "SALES_QTY") \
        .withColumn("COUNTRY_NAME", lit("CHINA")) \
        .select("YEAR", "QUARTER", "MONTH", "COUNTRY_NAME", "PROVINCE_NAME", "CITY_NAME", "COMPANY", "MKT", "MOLE_NAME", "PRODUCT_NAME", "SALES_QTY", "SALES_VALUE") \
        .withColumn("apex", lit("alfred")) \
        .withColumn("dimension.name", lit("*")) \
        .withColumn("dimension.value", lit("*")) \
        .write \
        .partitionBy("YEAR", "MONTH", "COMPANY") \
        .format("parquet") \
        .mode("overwrite") \
        .save(cleancube_result_path)
