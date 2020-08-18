# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job create measures in the final result cubes
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as func
from phlogs.phlogs import phlogger
from pyspark.sql.functions import col
from pyspark.sql.functions import sum
from pyspark.sql.functions import lit
from pyspark.sql.functions import udf


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

	phlogger.info("create calculate measures base on time dimension")
	jid_udf = udf(lambda y,m,c,l: "|".join([str(y), str(m), str(c), str(l)]), StringType())
	cat_udf = udf(lambda x: "PRODUCT" if "PRODUCT_NAME" in x else "MKT" if "MKT" in x else "MOLE" if "MOLE_NAME" in x else "COMPANY", StringType())
	# product_on_mkt_share_udf= udf(lambda s,m,mm,c: s/m if c == "MKT" else 0.0, DoubleType())
	# product_on_mole_share_udf = udf(lambda s,m,mm,c: s/m if c == "MOLE" else s/mm if c == "MKT" else, DoubleType())
	# mole_on_mkt_share_udf = udf(lambda s,m,mm,c: s/m if c == "MKT" else 0.0, DoubleType())

	# assumption 1: every lattice always have all three dimensions
	# we use CUBOIDS_ID for the first time
	df = spark.read.parquet("s3a://ph-max-auto/2020-08-11/cube/dest/8cd67399-3eeb-4f47-aaf9-9d2cc4258d90/result2/final-result") \
			.where(col("CUBOIDS_ID") == 3) \
			.withColumn("jid", jid_udf(col("YEAR"), col("MONTH"), col("CUBOIDS_ID"), col("LATTLES"))) \
			.withColumn("CAT", cat_udf(col("LATTLES")))
	df.persist()
	df.show()

	# 市场规模 **
	df_mkt_size = df.where(col("CAT") == "MKT")
					
	df_mkt_size = df_mkt_size.groupBy("jid") \
				.agg(
					sum(df_mkt_size.SALES_VALUE).alias("MKT_VALUE"), 
					sum(df_mkt_size.SALES_QTY).alias("MKT_QTY")
				)
				
	df = df.join(df_mkt_size, on="jid", how="left")
	df = df.na.fill({"MKT_VALUE": 0.0, "MKT_QTY": 0.0})


	# 市场中的分子规模 **
	df_mkt_mole_size = df.where(col("CAT") == "MOLE")
						
	df_mkt_mole_size = df_mkt_mole_size.groupBy("jid", "MKT") \
						.agg(
							sum(df_mkt_mole_size.SALES_VALUE).alias("MOLE_MKT_VALUE"),
							sum(df_mkt_mole_size.SALES_QTY).alias("MOLE_MKT_QTY")
						)
	df = df.join(df_mkt_mole_size, on=["jid", "MKT"], how="left")
	df = df.na.fill({"MOLE_MKT_VALUE": 0.0, "MOLE_MKT_QTY": 0.0})

	# 分子规模 **
	df_mole_size = df.where(col("CAT") == "MOLE")
					
	df_mole_size = df_mole_size.groupBy("jid") \
					.agg(
						sum(df_mole_size.SALES_VALUE).alias("MOLE_VALUE"),
						sum(df_mole_size.SALES_QTY).alias("MOLE_QTY")
					)
					
	df = df.join(df_mole_size, on="jid", how="left")
	df = df.na.fill({"MOLE_VALUE": 0.0, "MOLE_QTY": 0.0})
	
	df.show()
	
	# 市场份额与产品占分子份额
	