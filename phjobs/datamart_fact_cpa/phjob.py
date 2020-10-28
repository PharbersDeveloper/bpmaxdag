# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
import logging
import os

def execute(**kwargs):
	
	logging.basicConfig(format='%(asctime)s %(filename)s %(funcName)s %(lineno)d %(message)s')
	logger = logging.getLogger('driver_logger')
	logger.setLevel(logging.INFO)
	logger.info("Origin kwargs = {}.".format(str(kwargs)))

	# input required
	input_path = kwargs['input_path']
	if input_path == 'not set':
		raise Exception("Invalid input_path!", input_path)
		
	cpa_prod_path = kwargs['cpa_prod_path']
	if cpa_prod_path == 'not set':
		raise Exception("Invalid cpa_prod_path!", cpa_prod_path)
	
	cpa_hosp_path = kwargs['cpa_hosp_path']
	if cpa_hosp_path == 'not set':
		raise Exception("Invalid cpa_hosp_path!", cpa_hosp_path)
		
	cpa_date_path = kwargs['cpa_date_path']
	if cpa_date_path == 'not set':
		raise Exception("Invalid cpa_date_path!", cpa_date_path)
	
	cpa_etc_path = kwargs['cpa_etc_path']
	if cpa_etc_path == 'not set':
		raise Exception("Invalid cpa_etc_path!", cpa_etc_path)
		
	# output required
	output_path = kwargs['output_path']
	if output_path == 'not set':
		raise Exception("Invalid output_path!", output_path)

	spark = SparkSession.builder \
		.master("yarn") \
		.appName("datamart original fact_table cpa job") \
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
		spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.cn-northwest-1.amazonaws.com.cn")


	fact = spark.read.parquet('s3a://ph-platform/2020-08-10/datamart/original/fact_tables/cpa/v0.0.22_20201020_1')
	logger.info(fact.count())
	fact.printSchema()
	
	dim = spark.read.parquet('s3a://ph-platform/2020-08-10/datamart/original/dimensions/cpa_prods/v0.0.22_20201020_1')
	logger.info(dim.count())
	dim.printSchema()
	
	# logger.info("preparing data")
	
	# cpa_data_df = spark.read.parquet(input_path)

	# cpa_prod_data_df = spark.read.parquet(cpa_prod_path).withColumnRenamed("_ID", "CPA_PROD_ID")
	# join_prod_cond = [
	# 	cpa_data_df.PRODUCT_NAME == cpa_prod_data_df.PRODUCT_NAME,
	# 	cpa_data_df.ATC == cpa_prod_data_df.ATC,
	# 	cpa_data_df.MOLE_NAME == cpa_prod_data_df.MOLE_NAME,
	# 	cpa_data_df.SPEC == cpa_prod_data_df.SPEC,
	# 	cpa_data_df.DOSAGE == cpa_prod_data_df.DOSAGE,
	# 	cpa_data_df.PACK_QTY == cpa_prod_data_df.PACK_QTY,
	# 	cpa_data_df.MANUFACTURER_NAME == cpa_prod_data_df.MANUFACTURER_NAME,
	# 	cpa_data_df.DELIVERY_WAY == cpa_prod_data_df.DELIVERY_WAY,
	# ]
	# cpa_data_df = cpa_data_df.join(cpa_prod_data_df, join_prod_cond, "left_outer").drop("PRODUCT_NAME", "ATC", "MOLE_NAME", "SPEC", "DOSAGE", "PACK_QTY", "MANUFACTURER_NAME", "DELIVERY_WAY")

	# cpa_hosp_data_df = spark.read.parquet(cpa_hosp_path).withColumnRenamed("_ID", "CPA_HOSP_ID")
	# join_hosp_cond = [
	# 	cpa_data_df.HOSP_NAME == cpa_hosp_data_df.HOSP_NAME,
	# 	cpa_data_df.HOSP_CODE == cpa_hosp_data_df.HOSP_CODE,
	# 	cpa_data_df.HOSP_LEVEL == cpa_hosp_data_df.HOSP_LEVEL,
	# 	cpa_data_df.PROVINCE_NAME == cpa_hosp_data_df.PROVINCE_NAME,
	# 	cpa_data_df.CITY_NAME == cpa_hosp_data_df.CITY_NAME,
	# 	cpa_data_df.PREFECTURE_NAME == cpa_hosp_data_df.PREFECTURE_NAME,
	# ]
	# cpa_data_df = cpa_data_df.join(cpa_hosp_data_df, join_hosp_cond, "left_outer").drop("HOSP_NAME", "HOSP_CODE", "HOSP_LEVEL", "PROVINCE_NAME", "CITY_NAME", "PREFECTURE_NAME")

	# cpa_date_data_df = spark.read.parquet(cpa_date_path).withColumnRenamed("_ID", "CPA_DATE_ID")
	# join_date_cond = [
	# 	cpa_data_df.YEAR == cpa_date_data_df.YEAR,
	# 	cpa_data_df.QUARTER == cpa_date_data_df.QUARTER,
	# 	cpa_data_df.MONTH == cpa_date_data_df.MONTH,
	# ]
	# cpa_data_df = cpa_data_df.join(cpa_date_data_df, join_date_cond, "left_outer").drop("YEAR", "QUARTER", "MONTH")
	
	# cpa_etc_data_df = spark.read.parquet(cpa_etc_path).withColumnRenamed("_ID", "CPA_ETC_ID")
	# join_etc_cond = [
	# 	cpa_data_df.COMPANY == cpa_etc_data_df.COMPANY,
	# 	cpa_data_df.SOURCE == cpa_etc_data_df.SOURCE,
	# 	cpa_data_df.MKT == cpa_etc_data_df.MKT,
	# 	cpa_data_df.TAG == cpa_etc_data_df.TAG,
	# ]
	# cpa_data_df = cpa_data_df.join(cpa_etc_data_df, join_etc_cond, "left_outer").drop("COMPANY", "SOURCE", "MKT", "TAG")

	# cpa_data_df.write.format("parquet").mode("overwrite").save(output_path)
