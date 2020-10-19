# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def prod2mnfs(product_data_df):
	mnfs_data_df = product_data_df.select(
			"MNF_ID", 
			"MNF_TYPE", 
			"MNF_TYPE_NAME", 
			"MNF_TYPE_NAME_CH", 
			"MNF_NAME_EN", 
			"MNF_NAME_CH"
		).distinct().withColumn("_ID", F.monotonically_increasing_id()) \
		.withColumnRenamed("MNF_TYPE_NAME", "MNF_TYPE_NAME_EN") \
		.withColumn("TAG", F.lit("")) \
		.withColumn("PARENT_ID", F.lit(""))
	return mnfs_data_df
	

def trim_prod(product_data_df, mnfs_data_df):
	mnfs_data_df = mnfs_data_df.withColumnRenamed('_ID', 'PH_MNF_ID')
	cond = [
		product_data_df.MNF_ID == mnfs_data_df.MNF_ID,
		product_data_df.MNF_TYPE == mnfs_data_df.MNF_TYPE,
		product_data_df.MNF_TYPE_NAME == mnfs_data_df.MNF_TYPE_NAME_EN,
		product_data_df.MNF_TYPE_NAME_CH == mnfs_data_df.MNF_TYPE_NAME_CH,
		product_data_df.MNF_NAME_EN == mnfs_data_df.MNF_NAME_EN,
		product_data_df.MNF_NAME_CH == mnfs_data_df.MNF_NAME_CH,
	]
	product_data_df = product_data_df.join(mnfs_data_df, cond, "left_outer") \
						.drop('MNF_ID', 'MNF_TYPE', 'MNF_TYPE_NAME', 'MNF_TYPE_NAME_EN', 'MNF_TYPE_NAME_CH', 'MNF_NAME_EN', 'MNF_NAME_CH') \
						.drop('NFC1', 'NFC1_NAME', 'NFC1_NAME_CH', 'NFC12', 'NFC12_NAME', 'NFC12_NAME_CH') \
						.drop('ATC1', 'ATC1_CODE', 'ATC1_DESC', 'ATC2', 'ATC2_CODE', 'ATC2_DESC', "ATC3", "ATC3_CODE", "ATC3_DESC") \
						.drop('COMPANY', 'SOURCE', 'TAG', 'PARENT_ID') \
						.withColumn("_ID", F.monotonically_increasing_id())
	return product_data_df


def match_human_replace(human_replace_data_df, product_data_df, mnfs_data_df):
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
							.withColumn("_ID", F.monotonically_increasing_id())
	return clean_rules_data_df


def cpa2cpa_prod(product_data_df):
	mnfs_data_df = product_data_df.select(
			"MNF_ID", 
			"MNF_TYPE", 
			"MNF_TYPE_NAME", 
			"MNF_TYPE_NAME_CH", 
			"MNF_NAME_EN", 
			"MNF_NAME_CH"
		).distinct().withColumn("_ID", F.monotonically_increasing_id()) \
		.withColumnRenamed("MNF_TYPE_NAME", "MNF_TYPE_NAME_EN") \
		.withColumn("TAG", F.lit("")) \
		.withColumn("PARENT_ID", F.lit(""))
	return mnfs_data_df
	

def cpa2cpa_prod(product_data_df):
	mnfs_data_df = product_data_df.select(
			"MNF_ID", 
			"MNF_TYPE", 
			"MNF_TYPE_NAME", 
			"MNF_TYPE_NAME_CH", 
			"MNF_NAME_EN", 
			"MNF_NAME_CH"
		).distinct().withColumn("_ID", F.monotonically_increasing_id()) \
		.withColumnRenamed("MNF_TYPE_NAME", "MNF_TYPE_NAME_EN") \
		.withColumn("TAG", F.lit("")) \
		.withColumn("PARENT_ID", F.lit(""))
	return mnfs_data_df


def trim_cpa(cpa_data_df, product_data_df, mnfs_data_df):
	mnfs_data_df = mnfs_data_df.withColumnRenamed('_ID', 'PH_MNF_ID')
	prod_dis = product_data_df.join(mnfs_data_df, 'PH_MNF_ID', "left_outer") \
					.withColumnRenamed('_ID', 'PH_PROD_ID')

	cond = [
		cpa_data_df.MOLE_NAME == prod_dis.MOLE_NAME_CH,
		cpa_data_df.PRODUCT_NAME == prod_dis.PROD_NAME_CH,
		cpa_data_df.SPEC == prod_dis.SPEC,
		cpa_data_df.DOSAGE == prod_dis.DOSAGE,
		cpa_data_df.PACK_QTY == prod_dis.PACK,
		cpa_data_df.MANUFACTURER_NAME == prod_dis.MNF_NAME_CH,
	]
	cpa_data_df = cpa_data_df.join(prod_dis, cond, "left_outer")
							# .select('min', '_ID', human_replace_data_df.version) \
							# .withColumnRenamed('min', "MIN") \
							# .withColumnRenamed('_ID', 'PH_PROD_ID') \
							# .withColumn("_ID", F.monotonically_increasing_id())
	return cpa_data_df

def execute(a, b, target_path=''):
	"""
		please input your code below
	"""
	
	mnfs_target_path = 's3a://ph-platform/2020-08-10/datamart/standard/dimensions/mnfs/v20201001_20201015_1'
	prod_target_path = 's3a://ph-platform/2020-08-10/datamart/standard/dimensions/products/v20201001_20201015_1'
	clean_rules_target_path = 's3a://ph-platform/2020-08-10/datamart/standard/dimensions/clean_rules/v20201001_20201015_1'

	spark = SparkSession.builder \
		.master("yarn") \
		.appName("BPBatchDAG") \
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
		# spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
		spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.cn-northwest-1.amazonaws.com.cn")

	# product_data_df = spark.read.parquet("s3a://ph-stream/common/public/prod/0.0.15")
	# mnfs_data_df = prod2mnfs(product_data_df)
	# print(mnfs_data_df.count())
	# mnfs_data_df.show()
	# mnfs_data_df.repartition(2).write.format("parquet").mode("overwrite").save(mnfs_target_path)
	
	
	# product_data_df = spark.read.parquet("s3a://ph-stream/common/public/prod/0.0.15")
	# mnfs_data_df = spark.read.parquet(mnfs_target_path)
	# product_data_df = trim_prod(product_data_df, mnfs_data_df)
	# print(product_data_df.count())
	# product_data_df.show()
	# product_data_df.repartition(2).write.format("parquet").mode("overwrite").save(prod_target_path)

	
	# human_replace_data_df = spark.read.parquet("s3a://ph-stream/common/public/human_replace/0.0.14")
	# product_data_df = spark.read.parquet(prod_target_path)
	# mnfs_data_df = spark.read.parquet(mnfs_target_path)
	# clean_rules_data_df = match_human_replace(human_replace_data_df, product_data_df, mnfs_data_df)
	# print(clean_rules_data_df.count())
	# clean_rules_data_df.show()
	# clean_rules_data_df.repartition(2).write.format("parquet").mode("overwrite").save(clean_rules_target_path)
	
	
	
	
	
	# clean_rules_data_df = spark.read.parquet(clean_rules_target_path)
	# clean_rules_data_df.show()
	# print(clean_rules_data_df.count())
	# a = clean_rules_data_df.filter(clean_rules_data_df.PH_PROD_ID.isNull())
	# print(human_replace_data_df.count())
	# print(clean_rules_data_df.count())
	# human_replace_data_df.show()
	# clean_rules_data_df.show()
	# a = prod_dis.filter(prod_dis.MNF_NAME_CH.isNull())
	
	
	# cpa_data_df = spark.read.parquet("s3a://ph-stream/common/public/cpa/0.0.22")
	# product_data_df = spark.read.parquet(prod_target_path)
	# mnfs_data_df = spark.read.parquet(mnfs_target_path)
	# print(cpa_data_df.count())
	# cpa_data_df = trim_cpa(cpa_data_df, product_data_df, mnfs_data_df)
	# print(cpa_data_df.count())
	# a = cpa_data_df.filter(cpa_data_df.PH_PROD_ID.isNotNull())
	
	# print(a.count())
	# product_data_df.show()
	# spark.read.parquet("s3a://ph-stream/common/public/cpa/0.0.22").show()
	# cpa_data_df.select('YEAR').distinct().show()
	# cpa_data_df.select('COMPANY').distinct().show()
	# cpa_data_df.select('SOURCE').distinct().show()