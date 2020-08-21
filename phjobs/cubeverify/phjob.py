# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as func
from phlogs.phlogs import phlogger
from pyspark.sql.functions import explode
from pyspark.sql.functions import col
from pyspark.sql.functions import monotonically_increasing_id
import string


def execute(a, b):
	spark = SparkSession.builder \
        .master("yarn") \
        .appName("data cube create lattices data") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.instance", "1") \
        .config("spark.executor.memory", "1g") \
        .config('spark.sql.codegen.wholeStage', False) \
        .getOrCreate()

    # access_key = os.getenv("AWS_ACCESS_KEY_ID")
    # secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
	access_key = "AKIAWPBDTVEAJ6CCFVCP"
	secret_key = "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"
	if access_key is not None:
		spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
		spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
		spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
		spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
		# spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
		spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.cn-northwest-1.amazonaws.com.cn")

	phlogger.info("varify data lattices")
    
	# df = spark.read.parquet("s3a://ph-max-auto/2020-08-11/cube/dest/8cd67399-3eeb-4f47-aaf9-9d2cc4258d90/lattices-data/2-D-geo-prod")
	# df = spark.read.parquet("s3a://ph-max-auto/2020-08-11/cube/dest/8cd67399-3eeb-4f47-aaf9-9d2cc4258d90/meta/lattices")
	# df = spark.read.parquet("s3a://ph-max-auto/2020-08-11/cube/dest/8cd67399-3eeb-4f47-aaf9-9d2cc4258d90/lattices-result")
	# df = spark.read.parquet("s3a://ph-max-auto/2020-08-11/cube/dest/8cd67399-3eeb-4f47-aaf9-9d2cc4258d90/result2/lattices-result")
	# df = spark.read.parquet("s3a://ph-max-auto/2020-08-11/cube/dest/8cd67399-3eeb-4f47-aaf9-9d2cc4258d90/result2/final-result/part-00000-7aa9dfe8-1bf6-4fd5-8aad-75dbf502edce-c000.snappy.parquet")
	df = spark.read.parquet("s3a://ph-max-auto/2020-08-11/cube/dest/8cd67399-3eeb-4f47-aaf9-9d2cc4258d90/result2/final-result-ver-measures-2")
	# df = spark.read.parquet("s3a://ph-max-auto/2020-08-11/cube/dest/8cd67399-3eeb-4f47-aaf9-9d2cc4258d90/result2/final-result-2").where((col("LATTLES") == "%5BCITY_NAME, MOLE_NAME, QUARTER%5D") & (col("MOLE_NAME") == "丁苯酞") & (col("CITY_NAME") == "七台河市"))
	# df = spark.read.parquet("s3a://ph-max-auto/2020-08-11/cube/dest/8cd67399-3eeb-4f47-aaf9-9d2cc4258d90/meta/dimensions")
	# df = df.repartition(1).withColumn("LEVEL", monotonically_increasing_id())
	# df_group_level = df.groupBy("DIMENSION").agg({"LEVEL":"min"}).withColumnRenamed("min(LEVEL)", "EDGE")
	# df = df.join(df_group_level, how="left", on="DIMENSION")
	df.printSchema()
	# df = df.where((col("CUBOIDS_NAME").contains("time")) & (col("LATTLES").contains("QUARTER")))
	df.show(100)
	
	# df = spark.read.parquet("s3a://ph-max-auto/2020-08-11/cube/dest/8cd67399-3eeb-4f47-aaf9-9d2cc4258d90/meta/dimensions")
	# df = df.repartition(1).withColumn("LEVEL", monotonically_increasing_id())
	# df_group_level = df.groupBy("DIMENSION").agg({"LEVEL":"min"}).withColumnRenamed("min(LEVEL)", "EDGE")
	# df = df.join(df_group_level, how="left", on="DIMENSION").toPandas()
	# test = "CITY_NAME"
	# level = df[df["HIERARCHY"] == test].iloc[0]["LEVEL"]
	# edge = df[df["HIERARCHY"] == test].iloc[0]["EDGE"]
	# test = list(df[(df["LEVEL"] >= edge) & (df["LEVEL"] <= level)]["HIERARCHY"])
	# print test
