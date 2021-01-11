# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""
import pandas as pd
import boto3
import io
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import logging
import os


ext_col = ['GDP总值(亿元)', '常住人口(万人)', '常住城镇人口(万人)',
		'常住乡村人口(万人)', '常住人口出生率(‰)', '新生儿数', '城镇居民人均可支配收入（元）',
		'农民人均可支配收入（元）', 'Hosp_level_new', 'Continuity']
		
	
def execute(**kwargs):
	"""
		please input your code below
	"""
	logging.basicConfig(format='%(asctime)s %(filename)s %(funcName)s %(lineno)d %(message)s')
	logger = logging.getLogger('driver_logger')
	logger.setLevel(logging.INFO)
	logger.info("Origin kwargs = {}.".format(str(kwargs)))
	
	# input required
	input_path = kwargs.get('input_path', '')
	if input_path == 'not set' or input_path == '':
		raise Exception("Invalid input_path!", input_path)
	if not input_path.endswith("/"):
		input_path += '/'
		
	standard = kwargs.get('standard', '')
	if standard == 'not set' or standard == '':
		raise Exception("Invalid standard!", standard)
		
	# output required
	output_path = kwargs.get('output_path', '')
	if output_path == 'not set' or output_path == '':
		raise Exception("Invalid output_path!", output_path)
	if not output_path.endswith("/"):
		output_path += '/'
	
	os.environ["PYSPARK_PYTHON"] = "python3"
	spark = SparkSession.builder \
		.master("yarn") \
		.appName("datamart dimension output pot hosp job") \
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
	
	common_df = spark.read.parquet(input_path+'STANDARD=COMMON')
	spec_df = spark.read.parquet(input_path+'STANDARD=%s'%(standard))
	
	spec_join_df = spec_df.select(col('_ID').alias('S_ID'))
	common_null_df = common_df.join(spec_join_df, common_df._ID == spec_join_df.S_ID, 'left') \
					.filter(spec_join_df.S_ID.isNull()) \
					.drop('S_ID')
				
	union_df = spec_df.unionByName(common_null_df)
	ext_schema = ArrayType(StructType([StructField(col, StringType()) for col in ext_col]))
	union_df = union_df.withColumn('EXT', from_json('EXT', ext_schema).getItem(0)).select("*", 'EXT.*').drop('EXT')

	union_df.repartition(1).write.format("csv").option("header", "true").mode("overwrite").save(output_path+standard)
		