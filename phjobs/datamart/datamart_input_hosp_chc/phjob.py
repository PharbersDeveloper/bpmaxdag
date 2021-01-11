# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""
import pandas as pd
import boto3
import io
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging
import os


def s3excel2df(spark, source_bucket, source_path):
	access_key = os.getenv("AWS_ACCESS_KEY_ID")
	secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
		
	s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
	object_file = s3_client.get_object(Bucket=source_bucket, Key=source_path)
	data = object_file['Body'].read()
	pd_df = pd.read_excel(io.BytesIO(data), sheet_name='PCHC', encoding='utf-8')
		
	return spark.createDataFrame(pd_df.astype(str))


def chc_universe_format_to_standard(df):
	return df.select(
			col('PCHC_Code').alias('PCHC_ID'),
			col('省').alias('PROVINCE'),
			col('地级市').alias('CITY'),
			col('区[县/县级市】').alias('DISTRICT'),
			col('乡（镇/街道)').alias('STREET'),
			col('单位名称').alias('PCHC_HOSP_NAME'),
			col('人口').alias('POPULATION'),
			col('2016年执业医师（助理）人数').alias('STAFF_DOCTOR_2016'),
			col('2016年总诊疗人次数').alias('TREATMENT_TN_2016'),
			col('2016年药品收入（千元）').alias('DRUGINCOME_KRMB_2016'),
			col('其中：西药药品收入（千元）').alias(''),
			col('新人次').alias('NEW_PERSON_TIME'),
			col('职工总数').alias('EMPLOYEES_TN'),
			col('实有床位').alias('ACTUAL_BEDS'),
			col('年总诊疗人次数').alias('TREATMENT_YTN'),
			col('年出院人数').alias('DISCHARGED_YN'),
			col("药品收入 '000RMB").alias('DRUGINCOME_KRMB'),
			col("药品支出 '000RMB").alias('EXPEND_KRMB'),
			col('其中：0-14岁人口数').alias('POPULATION_0_14'),
			col('15-64岁人口数').alias('POPULATION_15_64'),
			col('65岁及以上人口数').alias('POPULATION_65_'),
			col('ZS_Servier name'), 
			col('flag'),
			col('flag2'),
		)


ext_col = ['ZS_Servier name', 'flag', 'flag2']

drop_col = ['_ID', 'flag', 'flag2']


def align_schema(df, schemas):
	str_get_value = {
		'string': '',
		'long': 0,
		'double': 0.0,
		'boolean': False,
	}
	
	cur_names = df.schema.fieldNames()
	for schema in schemas:
		if schema.name not in cur_names:
			df = df.withColumn(schema.name, lit(str_get_value.get(schema.dataType.typeName(), '')))
			if 'long' == schema.dataType.typeName():
				df = df.withColumn(schema.name, df[schema.name].cast('string').cast('int'))
	return df
	
	
def execute(**kwargs):
	"""
		please input your code below
	"""
	logging.basicConfig(format='%(asctime)s %(filename)s %(funcName)s %(lineno)d %(message)s')
	logger = logging.getLogger('driver_logger')
	logger.setLevel(logging.INFO)
	logger.info("Origin kwargs = {}.".format(str(kwargs)))
	
	# input required
	standard_universe_path = kwargs.get('standard_universe_path', '')
	if standard_universe_path == 'not set' or standard_universe_path == '':
		raise Exception("Invalid standard_universe_path!", standard_universe_path)
	if not standard_universe_path.endswith("/"):
		standard_universe_path += '/'
		
	chc_universe_path = kwargs.get('chc_universe_path', '')
	if chc_universe_path == 'not set' or chc_universe_path == '':
		raise Exception("Invalid chc_universe_path!", chc_universe_path)
	
	os.environ["PYSPARK_PYTHON"] = "python3"
	spark = SparkSession.builder \
		.master("yarn") \
		.appName("datamart dimension input pot hosp job") \
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
	
	chc_universe_path_lst = chc_universe_path.split('/')
	source_bucket = chc_universe_path_lst[2]
	source_path = "/".join(chc_universe_path_lst[3:])
	tag = 'CHC' # chc_universe_path_lst[-1].split('.')[0]
	
	standard_df = spark.read.parquet(standard_universe_path+'STANDARD=COMMON').dropDuplicates(['PCHC_ID'])
	
	df = s3excel2df(spark, source_bucket=source_bucket, source_path=source_path)
	df = chc_universe_format_to_standard(df)
	df = df.withColumn("EXT", to_json(struct(ext_col))).drop(*ext_col)
	
	all_schema = df.schema.names
	for col in drop_col:
		if col in all_schema:
			all_schema.remove(col)

	df = align_schema(df, standard_df.schema).drop("_ID")
	df = df.withColumn('ALL', to_json(struct(all_schema)))

	standard_df = standard_df.withColumn('ALL', to_json(struct(all_schema))) \
							.select('PCHC_ID', "_ID", "ALL") \
							.withColumnRenamed('PCHC_ID', 'S_PCHC_ID') \
							.withColumnRenamed('_ID', 'S_ID') \
							.withColumnRenamed('ALL', 'S_ALL')
	
	join_df = df.join(standard_df, df.PCHC_ID == standard_df.S_PCHC_ID, 'left')
	unequal_df = join_df.filter(join_df.ALL != join_df.S_ALL).drop('ALL', 'S_PCHC_ID', 'S_ALL').withColumnRenamed('S_ID', '_ID')
	no_join_df = join_df.filter(join_df.S_ALL.isNull()).drop('ALL', 'S_PCHC_ID', 'S_ALL', 'S_ID').withColumn("_ID", lit(''))
	
	df = unequal_df.unionByName(no_join_df).withColumn('STANDARD', lit(tag))
	df.repartition("STANDARD").write.format("parquet").mode('append').partitionBy("STANDARD").save(standard_universe_path)

