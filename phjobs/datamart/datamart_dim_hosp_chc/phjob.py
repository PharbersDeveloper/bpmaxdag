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
	
	pd_df.columns = [str(index)+"#"+column for index, column in enumerate(pd_df)]
		
	return spark.createDataFrame(pd_df.astype(str))


def chc_format_to_standard(df):
	return df.withColumnRenamed('43#增长率（%）.1', 'CITY_PER_SPENDABLE_INCOME_GROWTH_RATE') \
			.withColumnRenamed('45#增长率（%）.2', 'RURAL_PER_SPENDABLE_INCOME_GROWTH_RATE') \
			.select(
			col('0#来源').alias('UPDATE_LABEL'),
			col('3#新版PCHC_Code').alias('PCHC_ID'),
			col('4#省').alias('PROVINCE'),
			col('5#地级市').alias('CITY'),
			col('6#区[县/县级市】').alias('DISTRICT'),
			col('7#乡（镇/街道)').alias('STREET'),
			col('8#PCHC_Name').alias('PCHC_HOSP_NAME'),
			col('9#地址').alias('ADDRESS'),
			col('10#邮编').alias('ZIP_CODE'),
			col('11#电话').alias('PHONE'),
			col('14#人口').alias('POPULATION'),
			col('15#2016年执业医师（助理）人数').alias('STAFF_DOCTOR_2016'),
			col('16#2016年总诊疗人次数').alias('TREATMENT_TN_2016'),
			col('17#2016年药品收入（千元）').alias('DRUGINCOME_KRMB_2016'),
			col('18#其中：西药药品收入（千元）').alias(''),
			col('19#新人次').alias('NEW_PERSON_TIME'),
			col('20#职工总数').alias('EMPLOYEES_TN'),
			col('21#实有床位').alias('ACTUAL_BEDS'),
			col('22#年总诊疗人次数').alias('TREATMENT_YTN'),
			col('23#年出院人数').alias('DISCHARGED_YN'),
			col("24#药品收入 '000RMB").alias('DRUGINCOME_KRMB'),
			col("25#药品支出 '000RMB").alias('EXPEND_KRMB'),
			col('26#其中：0-14岁人口数').alias('POPULATION_0_14'),
			col('27#15-64岁人口数').alias('POPULATION_15_64'),
			col('28#65岁及以上人口数').alias('POPULATION_65_'),
			col('29#2018年所属区县常住人口数（万人）').alias('RESIDENT_POPULATION_2018'),
			col('30#2018年所属区县社区卫生服务站数量').alias('SERVICE_STATION_NUM_2018'),
			col('31#执业医师人数').alias('STAFF_DOCTOR_2018'),
			col('32#2018年总诊疗人次数').alias('TREATMENT_TN_2018'),
			col('33#2018年全部药品收入（千元）').alias('DRUGINCOME_KRMB_2018'),
			col('34#行政区划代码').alias('GBT'),
			col('35#GDP总值(亿元)').alias('GDP_ERMB'),
			col('36#增长率（%）').alias('GDP_GROWTH_RATE'),
			col('37#常住人口(万人)').alias('RESIDENT_POPULATION'),
			col('38#常住城镇人口(万人)').alias('CITY_RESIDENT_POPULATION'),
			col('39#常住乡村人口(万人)').alias('RURAL_RESIDENT_POPULATION'),
			col('40#常住人口出生率(‰)').alias('RESIDENT_POPULATION_BRITH_RATE'),
			col('41#常住人口死亡率(‰)').alias('RESIDENT_POPULATION_DEATH_RATE'),
			col('42#城镇居民人均可支配收入（元）').alias('CITY_PER_SPENDABLE_INCOME'),
			col('CITY_PER_SPENDABLE_INCOME_GROWTH_RATE'),
			col('44#农民人均可支配收入（元）').alias('RURAL_PER_SPENDABLE_INCOME'),
			col('RURAL_PER_SPENDABLE_INCOME_GROWTH_RATE'),
			col('46#新生儿数').alias('NEWBORN_NUM'),
		)

	
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
		
	# output required
	output_path = kwargs.get('output_path', '')
	if output_path == 'not set' or output_path == '':
		raise Exception("Invalid output_path!", output_path)
	
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
	
	input_path_lst = input_path.split('/')
	source_bucket = input_path_lst[2]
	source_path = "/".join(input_path_lst[3:])
	
	df = s3excel2df(spark, source_bucket=source_bucket, source_path=source_path)
	df = chc_format_to_standard(df)
	
	df = df.withColumn('EXT', lit('{}')).withColumn('STANDARD', lit('COMMON')).repartition(1).withColumn("_ID", monotonically_increasing_id()).cache()
	df.repartition("STANDARD").write.format("parquet").mode('overwrite').partitionBy("STANDARD").save(output_path)
	