# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import *
from pyspark.sql.functions import monotonically_increasing_id
import pandas as pd
import urllib
from uuid import uuid4
import logging
import os


def execute(**kwargs):

    logging.basicConfig(format='%(asctime)s %(filename)s %(funcName)s %(lineno)d %(message)s')
    logger = logging.getLogger('driver_logger')
    logger.setLevel(logging.INFO)
    logger.info("Origin kwargs = {}.".format(str(kwargs)))

	# input required
    lattices_path = kwargs['lattices_path']
    if lattices_path == u'default':
        raise Exception("Invalid lattices_path!", lattices_path)
    dimensions_path = kwargs['dimensions_path']
    if dimensions_path == u'default':
        raise Exception("Invalid dimensions_path!", dimensions_path)
    lattices_bucket_content_path = kwargs['lattices_bucket_content_path']
    if lattices_bucket_content_path == u'default':
        raise Exception("Invalid lattices_bucket_content_path!", lattices_bucket_content_path)

	# output
    lattices_result_path = kwargs['lattices_result_path']
    if lattices_result_path == u'default':
        jobName = "lattices-result"
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
        lattices_result_path = destPath + "/content"
    logger.info("lattices_result_path is {}.".format(lattices_result_path))

    spark = SparkSession.builder \
        .master("yarn") \
        .appName("data cube lattices result job") \
        .config("spark.driver.memory", "5g") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.instance", "2") \
        .config("spark.executor.memory", "5g") \
        .config('spark.sql.codegen.wholeStage', False) \
        .config("spark.shuffle.memoryFraction", "0.4") \
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

    logger.info("group by lattices data")

    meta_lattices_df = spark.read.parquet(lattices_path).where(col("CUBOIDS_ID") == 3).toPandas()

    columns = ["YEAR", "MONTH", "QUARTER", "COUNTRY_NAME", "PROVINCE_NAME", "CITY_NAME", "MKT", "COMPANY", "MOLE_NAME", "PRODUCT_NAME", "CUBOIDS_ID", "CUBOIDS_NAME", "LATTLES", "apex", "dimension_name", "dimension_value"]
    sch_columns = ["YEAR", "MONTH", "QUARTER", "COUNTRY_NAME", "PROVINCE_NAME", "CITY_NAME", "MKT", "COMPANY", "MOLE_NAME", "PRODUCT_NAME", "CUBOIDS_ID", "CUBOIDS_NAME", "LATTLES", "apex", "dimension_name", "dimension_value", "SALES_QTY", "SALES_VALUE"]

    schema = \
        StructType([ \
            StructField("QUARTER", LongType()), \
            StructField("COUNTRY_NAME", StringType()), \
            StructField("PROVINCE_NAME", StringType()), \
            StructField("CITY_NAME", StringType()), \
            StructField("MKT", StringType()), \
            StructField("MOLE_NAME", StringType()), \
            StructField("PRODUCT_NAME", StringType()), \
            StructField("SALES_QTY", DoubleType()), \
            StructField("SALES_VALUE", DoubleType()), \
            StructField("apex", StringType()), \
            StructField("dimension.name", StringType()), \
            StructField("dimension.value", StringType()), \
            StructField("YEAR", IntegerType()), \
            StructField("MONTH", IntegerType()), \
            StructField("COMPANY", StringType()), \
            StructField("CUBOIDS_ID", LongType()), \
            StructField("CUBOIDS_NAME", StringType()), \
            StructField("LATTLES", ArrayType(StringType()))
        ])

    # years = [2018, 2019]
    # months = range(1, 13)
    years = eval(kwargs['years'])
    months = range(1, 13)

    dim = spark.read.parquet(dimensions_path) \
            .repartition(1).withColumn("LEVEL", monotonically_increasing_id())
    dim_group_level = dim.groupBy("DIMENSION").agg({"LEVEL":"min"}).withColumnRenamed("min(LEVEL)", "EDGE")
    dim = dim.join(dim_group_level, how="left", on="DIMENSION").toPandas()

    def leaf2Condi(df, lts):
        res = []
        for tmp in lts:
            level = df[df["HIERARCHY"] == tmp].iloc[0]["LEVEL"]
            edge = df[df["HIERARCHY"] == tmp].iloc[0]["EDGE"]
            res.extend(list(df[(df["LEVEL"] >= edge) & (df["LEVEL"] <= level)]["HIERARCHY"]))
        return res

    for year in years:
        for month in months:
            for index, row in meta_lattices_df.iterrows():
                cid = row["CUBOIDS_ID"]
            	lts = []
            	for tmp in row["LATTLES"]:
            		lts.append(str(tmp))
            	path = str(lts).replace("'", "").replace("[", "%5B").replace("]", "%5D")
            	logger.info(year)
            	logger.info(month)
            	logger.info(cid)
            	logger.info(path)
            	df = spark.read.parquet(lattices_bucket_content_path + "/YEAR=" + str(year) + "/MONTH=" + str(month) + "/CUBOIDS_ID=" + str(cid) + "/LATTLES=" + path)
            	condi = ["YEAR", "MONTH", "CUBOIDS_ID"]
            	condi.extend(leaf2Condi(dim, lts))
            	logger.info("Now condi is {}.".format(condi))
            	df = df.withColumn("YEAR", lit(year)) \
            	        .withColumn("MONTH", lit(month)) \
            	        .withColumn("CUBOIDS_ID", lit(cid)) \
            	        .groupBy(condi).agg({"SALES_VALUE": "sum", "SALES_QTY": "sum"}) \
            	        .withColumnRenamed("sum(SALES_VALUE)", "SALES_VALUE") \
            	        .withColumnRenamed("sum(SALES_QTY)", "SALES_QTY")
            	ad_col = list(set(columns).difference(set(condi)))
            	for tc in ad_col:
            	    if tc is "LATTLES":
            	        df = df.withColumn(tc, lit(path))
            	    elif tc is "dimension_name":
            	        df = df.withColumn(tc, lit(row["CUBOIDS_NAME"]))
            	    elif tc is "CUBOIDS_NAME":
            	        df = df.withColumn(tc, lit(row["CUBOIDS_NAME"]))
            	   # elif tc is "dimension_value":
            	       # df = df.withColumn(tc, map(lts))
            	    else:
            	        df = df.withColumn(tc, lit("*"))

            	df = df.select(sch_columns).orderBy(desc("SALES_VALUE")).repartition(1)
            	df.persist()
                # full lattices
            	# 这时数据量偏小的情况下，不需要在分桶了
            	df.write.mode("overwrite") \
            	    .partitionBy("YEAR", "MONTH", "CUBOIDS_ID", "LATTLES") \
        			.parquet(lattices_result_path)

                df.unpersist()
    
