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
import string
import os
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

    # output
    cleancube_result_path = kwargs['cleancube_result_path']
    if cleancube_result_path == u'default':
        jobName = "cleanextractdata"
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
        destPath = "s3a://ph-max-auto/" + version +"/cube/jobs/runId_" + runId + "/" + jobName +"/jobId_" + jobId
        logger.info("DestPath is {}.".format(destPath))
        cleancube_result_path = destPath + "/content"
    logger.info("cleancube_result_path is {}.".format(cleancube_result_path))

    cleancube_result_metadata_path = "metadata".join(cleancube_result_path.rsplit('content', 1))
    logger.info("cleancube_result_metadata_path is {}.".format(cleancube_result_metadata_path))

    start = kwargs['start']
    end = kwargs['end']

    sd = int(start)
    ed = int(end)
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("data cube cleanextractdata job") \
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

    logger.info("preparing data from extract_data")

    reading = spark.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(max_result_path)

    # 1. 数据清洗
    df = reading.filter((col("Date") <= ed) & (col("Date") >= sd)) \
        .withColumnRenamed("标准省份名称", "PROVINCE_NAME") \
        .withColumnRenamed("标准城市名称", "CITY_NAME") \
        .withColumnRenamed("标准通用名", "MOLE_NAME") \
        .withColumnRenamed("project", "COMPANY") \
        .withColumnRenamed("标准商品名", "PRODUCT_NAME") \
        .withColumn("YEAR", floor(col("Date") / 100)) \
        .withColumn("MONTH", floor(col("Date") - col("YEAR") * 100)) \
        .withColumn("QUARTER", floor((col("MONTH")- 1) / 3 + 1)) \
        .withColumn("MKT", col("COMPANY")) \
        .fillna(0.0) \
        .withColumnRenamed("Sales", "SALES_VALUE") \
        .withColumnRenamed("Units", "SALES_QTY") \
        .withColumn("COUNTRY_NAME", lit("CHINA")) \
        .select("YEAR", "QUARTER", "MONTH", "COUNTRY_NAME", "PROVINCE_NAME", "CITY_NAME", "COMPANY", "MKT", "MOLE_NAME", "PRODUCT_NAME", "SALES_QTY", "SALES_VALUE") \
        .withColumn("YEAR", col("YEAR").cast("integer")) \
        .withColumn("QUARTER", col("QUARTER").cast("long")) \
        .withColumn("MONTH", col("MONTH").cast("integer")) \
        .withColumn("SALES_QTY", col("SALES_QTY").cast("double")) \
        .withColumn("SALES_VALUE", col("SALES_VALUE").cast("double")) \
        .withColumn("apex", lit("alfred")) \
        .withColumn("dimension.name", lit("*")) \
        .withColumn("dimension.value", lit("*"))

    df.select("YEAR", "MONTH").distinct().repartition(1).write \
        .mode("overwrite").parquet(cleancube_result_metadata_path)	# metadata

    df.write.format("parquet") \
        .mode("overwrite") \
        .save(cleancube_result_path)

    logger.info("cleanextractdata done.")
    
