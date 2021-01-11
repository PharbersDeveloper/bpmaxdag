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
	pd_df = pd.read_excel(io.BytesIO(data), encoding='utf-8')
		
	return spark.createDataFrame(pd_df.astype(str))


def pot_universe_format_to_standard(df):
	df = df.withColumnRenamed('新版ID', 'PHA_ID') \
		.withColumnRenamed('新版名称', 'PHA_HOSP_NAME') \
		.drop('county.HP') \
		.withColumnRenamed('Hosp_level', 'HOSP_LEVEL') \
		.withColumnRenamed('性质', 'HOSP_QUALITY') \
		.withColumnRenamed('Province', 'PROVINCE') \
		.withColumnRenamed('City', 'CITY') \
		.withColumnRenamed('City.Tier.2010', 'CITY_TIER') \
		.withColumnRenamed('District', 'DISTRICT') \
		.withColumnRenamed('Specialty_1_标准化', 'SPECIALTY_CATE') \
		.withColumnRenamed('Est_DrugIncome_RMB', 'DRUGINCOME_RMB') \
		.withColumnRenamed('医生数', 'DOCTORS_NUM') \
		.withColumnRenamed('床位数', 'BED_NUM') \
		.withColumnRenamed('全科床位数', 'GENERAL_BED_NUM') \
		.withColumnRenamed('内科床位数', 'INTERNAL_BED_NUM') \
		.withColumnRenamed('外科床位数', 'SURG_BED_NUM') \
		.withColumnRenamed('眼科床位数', 'OPHTH_BED_NUM') \
		.withColumnRenamed('年诊疗人次', 'ANNU_DIAG_TIME') \
		.withColumnRenamed('门诊诊次', 'OUTP_DIAG_TIME') \
		.withColumnRenamed('内科诊次', 'INTERNAL_DIAG_TIME') \
		.withColumnRenamed('外科诊次', 'SURG_DIAG_TIME') \
		.withColumnRenamed('入院人数', 'ADMIS_TIME') \
		.withColumnRenamed('住院病人手术人次数', 'SURG_TIME') \
		.withColumnRenamed('医疗收入', 'MED_INCOME') \
		.withColumnRenamed('门诊收入', 'OUTP_INCOME') \
		.withColumnRenamed('门诊治疗收入', 'OUTP_TREAT_INCOME') \
		.withColumnRenamed('门诊手术收入', 'OUTP_SURG_INCOME') \
		.withColumnRenamed('住院收入', 'IN_HOSP_INCOME') \
		.withColumnRenamed('住院床位收入', 'BED_INCOME') \
		.withColumnRenamed('住院治疗收入', 'IN_HOSP_TREAT_INCOME') \
		.withColumnRenamed('住院手术收入', 'IN_HOSP_SURG_INCOME') \
		.withColumnRenamed('药品收入', 'DRUG_INCOME') \
		.withColumnRenamed('门诊药品收入', 'OUTP_DRUG_INCOME') \
		.withColumnRenamed('门诊西药收入', 'OUTP_WST_DRUG_INCOME') \
		.withColumnRenamed('住院药品收入', 'IN_HOSP_DRUG_INCOME') \
		.withColumnRenamed('住院西药收入', 'IN_HOSP_WST_DRUG_INCOME')

	return df
	

ext_col = ['第一三共code', '第一三共医院名', 'GDP总值(亿元)', '常住人口(万人)', '常住城镇人口(万人)',
		'常住乡村人口(万人)', '常住人口出生率(‰)', '新生儿数', '城镇居民人均可支配收入（元）',
		'农民人均可支配收入（元）', 'Hosp_level_new', 'Continuity']

drop_col = ['_ID', 'COMPANY', 'SOURCE', 'TAG', 'UPDATE_LABEL']

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
		
	pot_universe_path = kwargs.get('pot_universe_path', '')
	if pot_universe_path == 'not set' or pot_universe_path == '':
		raise Exception("Invalid pot_universe_path!", pot_universe_path)
	
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
	
	pot_universe_path_lst = pot_universe_path.split('/')
	source_bucket = pot_universe_path_lst[2]
	source_path = "/".join(pot_universe_path_lst[3:])
	tag = pot_universe_path_lst[-1].split('.')[0]
	
	standard_df = spark.read.parquet(standard_universe_path+'STANDARD=COMMON').dropDuplicates(['PHA_ID'])
	
	df = s3excel2df(spark, source_bucket=source_bucket, source_path=source_path)
	df = pot_universe_format_to_standard(df)
	df = df.withColumn("EXT", to_json(struct(ext_col))).drop(*ext_col)
	
	all_schema = df.schema.names
	for col in drop_col:
		if col in all_schema:
			all_schema.remove(col)

	df = align_schema(df, standard_df.schema).drop("_ID")
	df = df.withColumn('ALL', to_json(struct(all_schema)))

	standard_df = standard_df.withColumn('ALL', to_json(struct(all_schema))) \
							.select('PHA_ID', "_ID", "ALL") \
							.withColumnRenamed('PHA_ID', 'S_PHA_ID') \
							.withColumnRenamed('_ID', 'S_ID') \
							.withColumnRenamed('ALL', 'S_ALL')
	
	join_df = df.join(standard_df, df.PHA_ID == standard_df.S_PHA_ID, 'left')
	unequal_df = join_df.filter(join_df.ALL != join_df.S_ALL).drop('ALL', 'S_PHA_ID', 'S_ALL').withColumnRenamed('S_ID', '_ID')
	no_join_df = join_df.filter(join_df.S_ALL.isNull()).drop('ALL', 'S_PHA_ID', 'S_ALL', 'S_ID').withColumn("_ID", lit(''))
	
	df = unequal_df.unionByName(no_join_df).withColumn('STANDARD', lit(tag))
	df.repartition("STANDARD").write.format("parquet").mode('append').partitionBy("STANDARD").save(standard_universe_path)

