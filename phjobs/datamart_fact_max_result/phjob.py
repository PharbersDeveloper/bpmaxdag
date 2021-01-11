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
	
	max_result_prod_path = kwargs['max_result_prod_path']
	if max_result_prod_path == 'not set':
		raise Exception("Invalid max_result_prod_path!", max_result_prod_path)
		
	max_result_hosp_path = kwargs['max_result_hosp_path']
	if max_result_hosp_path == 'not set':
		raise Exception("Invalid max_result_hosp_path!", max_result_hosp_path)
	
	max_result_etc_path = kwargs['max_result_etc_path']
	if max_result_etc_path == 'not set':
		raise Exception("Invalid max_result_etc_path!", max_result_etc_path)
		
	# output required
	output_path = kwargs['output_path']
	if output_path == 'not set':
		raise Exception("Invalid output_path!", output_path)
	
	
	spark = SparkSession.builder \
		.master("yarn") \
		.appName("datamart fact_table max result job") \
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

	logger.info("preparing data")
	
	max_result_data_df = spark.read.parquet(input_path)

	max_result_prod_data_df = spark.read.parquet(max_result_prod_path).withColumnRenamed("_ID", "MR_PROD_ID")
	join_prod_cond = [
		max_result_data_df.Molecule == max_result_prod_data_df.MOLE_NAME,
		max_result_data_df.Prod_Name == max_result_prod_data_df.PRODUCT_NAME,
	]
	max_result_data_df = max_result_data_df.join(max_result_prod_data_df, join_prod_cond, "left_outer") \
					.drop("Molecule", "Prod_Name", "MOLE_NAME", "PRODUCT_NAME")

	max_result_hosp_data_df = spark.read.parquet(max_result_hosp_path).withColumnRenamed("_ID", "MR_HOSP_ID")
	join_hosp_cond = [
		max_result_data_df.PHA == max_result_hosp_data_df.PHA,
		max_result_data_df.Province == max_result_hosp_data_df.PROVINCE,
		max_result_data_df.City == max_result_hosp_data_df.CITY,
		max_result_data_df.BEDSIZE == max_result_hosp_data_df.BEDSIZE,
	]
	max_result_data_df = max_result_data_df.join(max_result_hosp_data_df, join_hosp_cond, "left_outer") \
					.drop("PHA", "Province", "City", "BEDSIZE", "PROVINCE", "CITY")

	max_result_etc_data_df = spark.read.parquet(max_result_etc_path).withColumnRenamed("_ID", "MR_ETC_ID")
	join_etc_cond = [
		max_result_data_df.PANEL == max_result_etc_data_df.PANEL,
		max_result_data_df.Seg == max_result_etc_data_df.SEG,
		max_result_data_df.company == max_result_etc_data_df.COMPANY,
	]
	max_result_data_df = max_result_data_df.join(max_result_etc_data_df, join_etc_cond, "left_outer") \
					.drop("PANEL", "Seg", "company", "SEG", "COMPANY")

	max_result_data_df = max_result_data_df.withColumnRenamed('Date', "DATE") \
								.withColumnRenamed('Predict_Sales', "SALES") \
								.withColumnRenamed('Predict_Unit', "UNIT")
	max_result_data_df.write.format("parquet").mode("overwrite").save(output_path)