# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import col
from pyspark.sql.functions import sum
from pyspark.sql.functions import lit
from pyspark.sql.functions import udf
from pyspark.sql.functions import monotonically_increasing_id
import pandas as pd
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.functions import array
from pyspark.sql.functions import array_position
from uuid import uuid4
import logging
import os


def execute(**kwargs):

    logging.basicConfig(format='%(asctime)s %(filename)s %(funcName)s %(lineno)d %(message)s')
    logger = logging.getLogger('driver_logger')
    logger.setLevel(logging.INFO)
    logger.info("Origin kwargs = {}.".format(str(kwargs)))

    # input required
    hor_measures_content_path = kwargs['hor_measures_content_path']
    if hor_measures_content_path == u'default':
        raise Exception("Invalid hor_measures_content_path!", hor_measures_content_path)

    # output required
    postgres_uri = kwargs['postgres_uri']
    if postgres_uri == u'default':
        raise Exception("Invalid postgres_uri!", postgres_uri)
    postgres_user = kwargs['postgres_user']
    if postgres_user == u'default':
        raise Exception("Invalid postgres_user!", postgres_user)
    postgres_pass = kwargs['postgres_pass']
    if postgres_pass == u'default':
        raise Exception("Invalid postgres_pass!", postgres_pass)

    logger.info("hor_measures_content_path is {}.".format(hor_measures_content_path))
    hor_measures_metadata_path = "metadata".join(hor_measures_content_path.rsplit('content', 1))
    logger.info("hor_measures_metadata_path is {}.".format(hor_measures_metadata_path))

    sparkClassPath = os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.postgresql:postgresql:42.2.14 pyspark-shell'

    spark = SparkSession.builder \
        .master("yarn") \
        .appName("data cube cal hor measures") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.cores", "2") \
        .config("spark.executor.instance", "2") \
        .config("spark.executor.memory", "4g") \
        .config('spark.sql.codegen.wholeStage', False) \
        .config("spark.shuffle.memoryFraction", "0.4") \
        .config("spark.driver.extraClassPath", sparkClassPath) \
        .getOrCreate()

    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    if access_key is not None:
        spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
        spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
        spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
        spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
        spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.cn-northwest-1.amazonaws.com.cn")

    def write2postgres(s3path, postgres_table):
        logger.info("Start read s3path({}) write to postgres table({}).".format(s3path, postgres_table))
        spark.read.parquet(s3path) \
            .write.format("jdbc") \
            .option("url", postgres_uri) \
            .option("dbtable", postgres_table) \
            .option("user", postgres_user) \
            .option("password", postgres_pass) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()

    logger.info("Start write to PostgreSQL.")

    df_company = spark.read.parquet(hor_measures_metadata_path)
    if hor_measures_metadata_path == hor_measures_content_path:
        logger.info("start generate cpmpany distinct.")
        df_company = spark.read.parquet(hor_measures_metadata_path).select("COMPANY").distinct()
    pds_company = df_company.toPandas()
    for idx, row in pds_company.iterrows():
        logger.info("idx is {},\trow is {}.".format(idx, row))
        company = row["COMPANY"]
        logger.info("company is {}.".format(company))
        write2postgres(hor_measures_content_path + "/COMPANY=" + str(company), company)
