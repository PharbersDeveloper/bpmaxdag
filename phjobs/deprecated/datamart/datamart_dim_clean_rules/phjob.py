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
	human_replace_input_path = kwargs['human_replace_input_path']
	if human_replace_input_path == 'not set':
		raise Exception("Invalid human_replace_input_path!", human_replace_input_path)
		
	# input required
	mnfs_input_path = kwargs['mnfs_input_path']
	if mnfs_input_path == 'not set':
		raise Exception("Invalid mnfs_input_path!", mnfs_input_path)
		
	# input required
	prod_input_path = kwargs['prod_input_path']
	if prod_input_path == 'not set':
		raise Exception("Invalid prod_input_path!", prod_input_path)
		
	# output required
	clean_rules_output_path = kwargs['clean_rules_output_path']
	if clean_rules_output_path == 'not set':
		raise Exception("Invalid clean_rules_output_path!", clean_rules_output_path)

	spark = SparkSession.builder \
		.master("yarn") \
		.appName("datamart dimension clean_rules job") \
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
	
	human_replace_data_df = spark.read.parquet(human_replace_input_path)
	mnfs_data_df = spark.read.parquet(mnfs_input_path)
	product_data_df = spark.read.parquet(prod_input_path)
	
	mnfs_data_df = mnfs_data_df.withColumnRenamed('_ID', 'PH_MNF_ID')
	prod_dis = product_data_df.join(mnfs_data_df, 'PH_MNF_ID', "left_outer") \
								.withColumnRenamed('_ID', 'PH_PROD_ID')

	cond = [
		human_replace_data_df.MOLE_NAME == prod_dis.MOLE_NAME_CH,
		human_replace_data_df.PRODUCT_NAME == prod_dis.PROD_NAME_CH,
		human_replace_data_df.SPEC == prod_dis.SPEC,
		human_replace_data_df.DOSAGE == prod_dis.DOSAGE,
		human_replace_data_df.PACK_QTY == prod_dis.PACK,
		human_replace_data_df.MANUFACTURER_NAME == prod_dis.MNF_NAME_CH,
	]
	clean_rules_data_df = human_replace_data_df.join(prod_dis, cond, "left_outer") \
							.select('min', 'PH_PROD_ID', human_replace_data_df.version) \
							.withColumnRenamed('min', "MIN") \
							.repartition(1).withColumn("_ID", monotonically_increasing_id()).cache()

	clean_rules_data_df.write.format("parquet").mode("overwrite").save(clean_rules_output_path)
