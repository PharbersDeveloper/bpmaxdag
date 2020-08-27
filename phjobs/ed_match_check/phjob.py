# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as func


class DataStatistics(object):
	"""
	数据统计，将要比对的数据序列化，并重写 __eq__ 方法
	"""
	judge_list = []

	def set_count(self, count):
		"""
		设置总数
		"""
		self.count = count
		return self

	def set_category(self, category):
		self.category = category
		return self

	def __str__(self):
		return str(self.__dict__)

	def __eq__(self, other):
		for key, value in self.__dict__.items():
			other_value = getattr(other, key, None)
			if value == other_value:
				self.judge_list.append((key, True, str(value) + " == " + str(other_value)))
			else:
				self.judge_list.append((key, False, str(value) + " != " + str(other_value)))

		err_list = [judge for judge in self.judge_list if not judge[1]]

		for err in err_list:
			print(err)

		if err_list:
			return False
		else:
			return True


def execute(correct_data_path, test_data_path):
	"""
		please input your code below
	"""
	os.environ["PYSPARK_PYTHON"] = "python3"
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

	correct_data_df = spark.read.parquet(correct_data_path)
	test_data_df = spark.read.parquet(test_data_path)

	ds1 = DataStatistics().set_count(1).set_category({'浙江': 1000, '上海': 500})
	ds2 = DataStatistics().set_count(1)

	print(ds1 == ds2)


