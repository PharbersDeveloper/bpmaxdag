# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import udf
from pyspark.sql.functions import col
from pyspark.sql.functions import floor
from pyspark.sql.functions import lit
from pyspark.sql.functions import first
from pyspark.sql.functions import sum
from pyspark.sql.functions import udf
from pyspark.sql.functions import monotonically_increasing_id
import pandas as pd
from uuid import uuid4
import logging
import os



def execute(**kwargs):

    logging.basicConfig(format='%(asctime)s %(filename)s %(funcName)s %(lineno)d %(message)s')
    logger = logging.getLogger('driver_logger')
    logger.setLevel(logging.INFO)
    logger.info("Origin kwargs = {}.".format(str(kwargs)))

    # input required
    dimensions_path = kwargs['dimensions_path']
    if dimensions_path == u'default':
        raise Exception("Invalid dimensions_path!", dimensions_path)
    lattices_result_path = kwargs['lattices_result_path']
    if lattices_result_path == u'default':
        raise Exception("Invalid lattices_result_path!", lattices_result_path)

	# output
    final_result_path = kwargs['final_result_path']
    if final_result_path == u'default':
        jobName = "final-result"
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
        final_result_path = destPath + "/content"
    logger.info("final_result_path is {}.".format(final_result_path))

    spark = SparkSession.builder \
        .master("yarn") \
        .appName("data cube final result job") \
        .config("spark.driver.memory", "5g") \
        .config("spark.executor.cores", "2") \
        .config("spark.executor.instance", "5") \
        .config("spark.executor.memory", "5g") \
        .config('spark.sql.codegen.wholeStage', False) \
        .config("spark.sql.files.maxRecordsPerFile", 33554432) \
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

    logger.info("final cube data")

    """
        前面的计算会有一个bug, 因为分片计算, 在quarter上的聚合与年上的聚合是错的
        所以对结果在来一次
        因为小文件过多，很容易造成数据队列的oom

        同时还有类型不匹配的bug，以后修改
    """

    """
        root
         |-- QUARTER: string (nullable = true)
         |-- COUNTRY_NAME: string (nullable = true)
         |-- PROVINCE_NAME: string (nullable = true)
         |-- CITY_NAME: string (nullable = true)
         |-- MKT: string (nullable = true)
         |-- COMPANY: string (nullable = true)
         |-- MOLE_NAME: string (nullable = true)
         |-- PRODUCT_NAME: string (nullable = true)
         |-- CUBOIDS_NAME: string (nullable = true)
         |-- apex: string (nullable = true)
         |-- dimension_name: string (nullable = true)
         |-- dimension_value: string (nullable = true)
         |-- SALES_QTY: double (nullable = true)
         |-- SALES_VALUE: double (nullable = true)
         |-- YEAR: Integer
         |-- MONTH: Integer
         |-- CUBOIDS_ID: integer (nullable = true)
         |-- LATTLES: string (nullable = true)
    """

    schema = \
        StructType([ \
            StructField("QUARTER", StringType()), \
            StructField("COUNTRY_NAME", StringType()), \
            StructField("PROVINCE_NAME", StringType()), \
            StructField("CITY_NAME", StringType()), \
            StructField("MKT", StringType()), \
            StructField("MOLE_NAME", StringType()), \
            StructField("PRODUCT_NAME", StringType()), \
            StructField("SALES_QTY", DoubleType()), \
            StructField("SALES_VALUE", DoubleType()), \
            StructField("apex", StringType()), \
            StructField("dimension_name", StringType()), \
            StructField("dimension_value", StringType()), \
            StructField("YEAR", IntegerType()), \
            StructField("MONTH", IntegerType()), \
            StructField("COMPANY", StringType()), \
            StructField("CUBOIDS_ID", LongType()), \
            StructField("CUBOIDS_NAME", StringType()), \
            StructField("LATTLES", StringType())
        ])

    sch_columns = ["YEAR", "MONTH", "QUARTER", "COUNTRY_NAME", "PROVINCE_NAME", "CITY_NAME", "MKT", "COMPANY", "MOLE_NAME", "PRODUCT_NAME", "CUBOIDS_ID", "CUBOIDS_NAME", "LATTLES", "apex", "dimension_name", "dimension_value", "SALES_QTY", "SALES_VALUE"]

            # .where((col("YEAR") == 2019) & (col("CUBOIDS_ID") == 3)) \
    df = spark.read.schema(schema).parquet(lattices_result_path) \
            .where((col("CUBOIDS_ID") == 3)) \
            .drop("QUARTER")
    df = df.withColumn("QUARTER", floor((col("MONTH") - 1) / 3 + 1)).withColumn("QUARTER", col("QUARTER").cast(IntegerType()))
    df = df.withColumn("QUARTER", df.YEAR * 100 + df.QUARTER)
    df = df.withColumn("MONTH", df.QUARTER * 100 + df.MONTH)
    df = df.withColumn("CUBOIDS_NAME", col("dimension_name"))
    df.persist()

    dim = spark.read.parquet(dimensions_path) \
            .repartition(1).withColumn("LEVEL", monotonically_increasing_id())
    dim_group_level = dim.groupBy("DIMENSION").agg({"LEVEL":"min"}).withColumnRenamed("min(LEVEL)", "EDGE")
    dim = dim.join(dim_group_level, how="left", on="DIMENSION").toPandas()
    logger.info("Now dim is {}.".format(dim))

    def leaf2Condi(df, lts):
        res = []
        for tmp in lts:
            level = df[df["HIERARCHY"] == tmp].iloc[0]["LEVEL"]
            edge = df[df["HIERARCHY"] == tmp].iloc[0]["EDGE"]
            res.extend(list(df[(df["LEVEL"] >= edge) & (df["LEVEL"] <= level)]["HIERARCHY"]))
        return res

    df_lattices = df.select("LATTLES", "CUBOIDS_NAME").distinct().repartition(1).withColumn("LATTLES_ID", monotonically_increasing_id()).toPandas()
    logger.info("Now df_lattices is {}.".format(df_lattices))
    columns = ["YEAR", "MONTH", "QUARTER", "COUNTRY_NAME", "PROVINCE_NAME", "CITY_NAME", "MKT", "COMPANY", "MOLE_NAME", "PRODUCT_NAME", "CUBOIDS_ID", "CUBOIDS_NAME", "LATTLES", "apex", "dimension_name", "dimension_value"]
    sch_columns = ["YEAR", "MONTH", "QUARTER", "COUNTRY_NAME", "PROVINCE_NAME", "CITY_NAME", "MKT", "COMPANY", "MOLE_NAME", "PRODUCT_NAME", "CUBOIDS_ID", "CUBOIDS_NAME", "LATTLES", "apex", "dimension_name", "dimension_value", "SALES_QTY", "SALES_VALUE"]

    for idx, row in df_lattices.iterrows():
        df_c = df.where(col("LATTLES") == row["LATTLES"])
        lts = []
        cur_l = row["LATTLES"].replace("%5B", "").replace("%5D", "").replace(" ", "").split(",")
        for tmp in cur_l:
            lts.append(str(tmp))
        condi = ["CUBOIDS_ID"]
        condi.extend(leaf2Condi(dim, lts))
        logger.info("Now LATTLES is {}.".format(row["LATTLES"]))
        logger.info("Now condi is {}.".format(condi))

        ad_col = list(set(columns).difference(set(condi)))
        logger.info("Now ad_col is {}.".format(ad_col))

        if "MONTH" in row["LATTLES"]:
            # 0. MONTH 的做法
            logger.info("Start at MONTH with {}.".format(cur_l))
            df_c.select(sch_columns).repartition(1).write.mode("append").parquet(final_result_path)
        elif "QUARTER" in row["LATTLES"]:
            # 1. QUARTER 的做法
            logger.info("Start at QUARTER with {}.".format(cur_l))
            df_c = df_c.groupBy(condi).agg(
                    sum(df_c.SALES_VALUE).alias("SALES_VALUE"),
                    sum(df_c.SALES_QTY).alias("SALES_QTY")
                )

            for tc in ad_col:
                if tc is "LATTLES":
                    df_c = df_c.withColumn(tc, lit(row["LATTLES"]))
                elif tc is "dimension_name":
                    df_c = df_c.withColumn(tc, lit(row["CUBOIDS_NAME"]))
                elif tc is "CUBOIDS_NAME":
                    df_c = df_c.withColumn(tc, lit(row["CUBOIDS_NAME"]))
                elif tc is "MONTH":
                    df_c = df_c.withColumn(tc, lit(-1))
                else:
                    df_c = df_c.withColumn(tc, lit("*"))
            df_c.select(sch_columns).repartition(1).write.mode("append").parquet(final_result_path)
        elif "YEAR" in row["LATTLES"]:
            # 2. YEAR 的做
            logger.info("Start at YEAR with {}.".format(cur_l))
            df_c = df_c.groupBy(condi).agg(
                    sum(df_c.SALES_VALUE).alias("SALES_VALUE"),
                    sum(df_c.SALES_QTY).alias("SALES_QTY")
                )
            for tc in ad_col:
                if tc is "LATTLES":
                    df_c = df_c.withColumn(tc, lit(row["LATTLES"]))
                elif tc is "dimension_name":
                    df_c = df_c.withColumn(tc, lit(row["CUBOIDS_NAME"]))
                elif tc is "CUBOIDS_NAME":
                    df_c = df_c.withColumn(tc, lit(row["CUBOIDS_NAME"]))
                elif tc is "MONTH":
                    df_c = df_c.withColumn(tc, lit(-1))
                elif tc is "QUARTER":
                    df_c = df_c.withColumn(tc, lit(-1))
                else:
                    df_c = df_c.withColumn(tc, lit("*"))
            df_c.select(sch_columns).repartition(1).write.mode("append").parquet(final_result_path)
        else:
            pass
    
