# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as func
from phlogs.phlogs import phlogger
from pyspark.sql.functions import col
from pyspark.sql.functions import sum
from pyspark.sql.functions import lit
from pyspark.sql.functions import udf
from pyspark.sql.functions import monotonically_increasing_id


def execute(a, b):
	spark = SparkSession.builder \
        .master("yarn") \
        .appName("data cube cal measures") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.cores", "2") \
        .config("spark.executor.instance", "4") \
        .config("spark.executor.memory", "2g") \
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
	geo_level_udf = udf(lambda x: "CITY_NAME" if "CITY_NAME" in x else "PROVINCE_NAME" if "PROVINCE_NAME" in x else "COUNTRY_NAME", StringType())
	time_level_udf = udf(lambda x: "YEAR" if "YEAR" in x else "QUARTER" if "QUARTER" in x else "MONTH", StringType())
	time_parent_level_udf = udf(lambda x: "*" if "YEAR" in x else "YEAR" if "QUARTER" in x else "QUARTER", StringType())
	prod_level_udf = udf(lambda x: "PRODUCT_NAME" if "PRODUCT_NAME" in x else "MKT" if "MKT" in x else "MOLE_NAME" if "MOLE_NAME" in x else "COMPANY", StringType())
	prod_parent_level_udf = udf(lambda x: "MOLE" if "PRODUCT_NAME" in x else "MKT" if "MOLE_NAME" in x else "COMPANY" if "MKT" in x else "*", StringType())
	
	def ver_parent_condi(cur, dim):
		par = []
		for tmp in cur:
			par.append(str(tmp))

		time = ["YEAR", "QUARTER", "MONTH"]
		geo = ["COUNTRY_NAME", "PROVINCE_NAME", "CITY_NAME"]
		prod = ["COMPANY", "MKT", "MOLE_NAME", "PRODUCT_NAME"]
		tmp = list(set(cur).intersection(set(prod[1:])))
		if len(tmp) is 0:
			return []
		else:
			rp = prod[prod.index(tmp[0]) - 1]
			par = [rp if x == tmp[0] else x for x in par]
		return par
			
	"""
		# assumption 1: every lattice always have all three dimensions
		# we use CUBOIDS_ID for the first time
	"""
	
	schema = \
        StructType([ \
            StructField("YEAR", IntegerType()), \
            StructField("MONTH", IntegerType()), \
            StructField("QUARTER", LongType()), \
            StructField("COUNTRY_NAME", StringType()), \
            StructField("PROVINCE_NAME", StringType()), \
            StructField("CITY_NAME", StringType()), \
            StructField("MKT", StringType()), \
            StructField("COMPANY", StringType()), \
            StructField("MOLE_NAME", StringType()), \
            StructField("PRODUCT_NAME", StringType()), \
            StructField("CUBOIDS_ID", LongType()), \
            StructField("CUBOIDS_NAME", StringType()), \
            StructField("LATTLES", StringType()), \
            StructField("apex", StringType()), \
            StructField("dimension_name", StringType()), \
            StructField("dimension_value", StringType()), \
            StructField("SALES_QTY", DoubleType()), \
            StructField("SALES_VALUE", DoubleType())
        ])
	
	df = spark.read.parquet("s3a://ph-max-auto/2020-08-11/cube/dest/8cd67399-3eeb-4f47-aaf9-9d2cc4258d90/result2/final-result") \
			.where((col("CUBOIDS_ID") == 3) & (col("YEAR") == 2018))
	df = df.withColumn("PROD_CUR_LEVEL", prod_level_udf(col("LATTLES"))) \
			.withColumn("GEO_CUR_LEVEL", geo_level_udf(col("LATTLES"))) \
			.withColumn("TIME_CUR_LEVEL", time_level_udf(col("LATTLES"))) 
	df.persist()
		
	"""
		计算度量中很多是lattices之间的平移，也就是一个join的操作
	"""

	"""
		寻找层次间的平移, GEO 层级好像没有计算度量需要纵向的平移
		优先从产品层面平移吧
	"""
	dim = spark.read.parquet("s3a://ph-max-auto/2020-08-11/cube/dest/8cd67399-3eeb-4f47-aaf9-9d2cc4258d90/meta/dimensions") \
			.repartition(1).withColumn("LEVEL", monotonically_increasing_id())
	dim_group_level = dim.groupBy("DIMENSION").agg({"LEVEL":"min"}).withColumnRenamed("min(LEVEL)", "EDGE")
	dim = dim.join(dim_group_level, how="left", on="DIMENSION").toPandas()
	print dim
	
	def leaf2Condi(df, lts):
	    res = []
	    for tmp in lts:
	        level = df[df["HIERARCHY"] == tmp].iloc[0]["LEVEL"]
	        edge = df[df["HIERARCHY"] == tmp].iloc[0]["EDGE"]
	        res.extend(list(df[(df["LEVEL"] >= edge) & (df["LEVEL"] <= level)]["HIERARCHY"]))
	    return res

	# df_lattices = df.select("LATTLES", "CUBOIDS_NAME").distinct().repartition(1).withColumn("LATTLES_ID", monotonically_increasing_id()).toPandas()
	# columns = ["YEAR", "MONTH", "QUARTER", "COUNTRY_NAME", "PROVINCE_NAME", "CITY_NAME", "MKT", "COMPANY", "MOLE_NAME", "PRODUCT_NAME", "CUBOIDS_ID", "CUBOIDS_NAME", "LATTLES", "apex", "dimension_name", "dimension_value"]
	# sch_columns = ["YEAR", "MONTH", "QUARTER", "COUNTRY_NAME", "PROVINCE_NAME", "CITY_NAME", "MKT", "COMPANY", "MOLE_NAME", "PRODUCT_NAME", "CUBOIDS_ID", "CUBOIDS_NAME", "LATTLES", "apex", "dimension_name", "dimension_value", "SALES_QTY", "SALES_VALUE"]


	df_lattices = df.select("LATTLES").distinct().repartition(1).withColumn("LATTLES_ID", monotonically_increasing_id()).toPandas()
	print df_lattices

	# 产品维度层面	
	for idx, row in df_lattices.iterrows():
		df_c = df.where(col("LATTLES") == row["LATTLES"]) \
				.withColumn("dim", lit("prod")) 
		cur_l = row["LATTLES"].replace("%5B", "").replace("%5D", "").replace(" ", "").split(",")
		cur_p = ver_parent_condi(cur_l, "prod")
		if len(cur_p) is not 0:
			str_cur_p = str(cur_p).replace("[", "%5B").replace("]", "%5D").replace("'", "")
			df_p = df.where(col("LATTLES") == str_cur_p).select(cur_p + ["SALES_VALUE"]).withColumnRenamed("SALES_VALUE", "PROD_PARPENT_VALUE")
			df_p.show(100)
			
			df_r = df_c.join(df_p, on=cur_p, how="left")
			
			df_r.where(col("TIME_CUR_LEVEL") == "QUARTER").show()
		
		else:
			df_r = df_c.withColumn("PROD_PARPENT_VALUE", lit(0.0))