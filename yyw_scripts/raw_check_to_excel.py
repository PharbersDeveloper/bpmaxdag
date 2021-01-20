#from ph_logs.ph_logs import phlogger
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql import functions as func
import os
from pyspark.sql.functions import pandas_udf, PandasUDFType
import time
import pandas as pd

os.environ["PYSPARK_PYTHON"] = "python3"
spark = SparkSession.builder \
    .master("yarn") \
    .appName("data from s3") \
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

    
'''
合并s3上的raw_data_check/.csv 为excel
'''
project_name = '贝达'
outdir = '202011'
outdir_local = "/home/ywyuan/tmp_file"

max_path = 's3a://ph-max-auto/v0.0.1-2020-06-08/'
raw_data_check_path = max_path + '/' + project_name + '/' + outdir + '/raw_data_check/'
check_result_path = raw_data_check_path + '/check_result.csv'
check_1_path = raw_data_check_path + '/check_1_每个月产品个数.csv'
check_2_path = raw_data_check_path + '/check_2_各产品历史月份销量.csv'
check_3_path = raw_data_check_path + '/check_3_历史医院个数.csv'
check_5_path = raw_data_check_path + '/check_5_最近12期每家医院每个月的金额规模.csv'
check_8_path = raw_data_check_path + '/check_8_每个医院每个月产品个数.csv'
check_9_1_path = raw_data_check_path + '/check_9_1_所有产品每个月金额.csv'
check_9_2_path = raw_data_check_path + '/check_9_2_所有产品每个月份额.csv'
check_9_3_path = raw_data_check_path + '/check_9_3_所有产品每个月排名.csv'
check_10_path = raw_data_check_path + '/check_10_在售产品医院个数.csv'
check_11_path = raw_data_check_path + '/check_11_全部医院历史贡献率等级.csv'

check_result = spark.read.csv(check_result_path, header=True)
check_result = check_result.toPandas()
check_1 = spark.read.csv(check_1_path, header=True)
check_1 = check_1.toPandas()
check_1[check_1.columns[1:]]=check_1[check_1.columns[1:]].astype(float)

check_2 = spark.read.csv(check_2_path, header=True)
check_2 = check_2.toPandas()
check_2[check_2.columns[1:]]=check_2[check_2.columns[1:]].astype(float)

check_3 = spark.read.csv(check_3_path, header=True)
check_3 = check_3.toPandas()
check_3[check_3.columns[1:]]=check_3[check_3.columns[1:]].astype(float)

check_5 = spark.read.csv(check_5_path, header=True)
check_5 = check_5.toPandas()
check_5[check_5.columns[1:-1]]=check_5[check_5.columns[1:-1]].astype(float)

check_8 = spark.read.csv(check_8_path, header=True)
check_8 = check_8.toPandas()
check_8[check_8.columns[1:]]=check_8[check_8.columns[1:]].astype(float)

check_9_1 = spark.read.csv(check_9_1_path, header=True)
check_9_1 = check_9_1.toPandas()
check_9_1[check_9_1.columns[1:]]=check_9_1[check_9_1.columns[1:]].astype(float)

check_9_2 = spark.read.csv(check_9_2_path, header=True)
check_9_2 = check_9_2.toPandas()
check_9_2[check_9_2.columns[1:]]=check_9_2[check_9_2.columns[1:]].astype(float)

check_9_3 = spark.read.csv(check_9_3_path, header=True)
check_9_3 = check_9_3.toPandas()
check_9_3[check_9_3.columns[1:]]=check_9_3[check_9_3.columns[1:]].astype(float)

check_10 = spark.read.csv(check_10_path, header=True)
check_10 = check_10.toPandas()
check_10[check_10.columns[1:]]=check_10[check_10.columns[1:]].astype(float)

check_11 = spark.read.csv(check_11_path, header=True)
check_11 = check_11.toPandas()
check_11[check_11.columns[1:-1]]=check_11[check_11.columns[1:-1]].astype(float)


with pd.ExcelWriter(outdir_local + "/" + project_name + "_raw_data_check.xlsx") as xlsx:
    check_result.to_excel(xlsx, sheet_name="check_result", index=False)
    check_1.to_excel(xlsx, sheet_name="每个月产品个数", index=False)
    check_2.to_excel(xlsx, sheet_name="各产品历史月份销量", index=False)
    check_3.to_excel(xlsx, sheet_name="历史医院个数", index=False)
    check_5.to_excel(xlsx, sheet_name="最近12期每家医院每个月的金额规模", index=False)
    check_8.to_excel(xlsx, sheet_name="每个医院每个月产品个数", index=False)
    check_9_1.to_excel(xlsx, sheet_name="所有产品每个月金额", index=False)
    check_9_2.to_excel(xlsx, sheet_name="所有产品每个月份额", index=False)
    check_9_3.to_excel(xlsx, sheet_name="所有产品每个月排名", index=False)
    check_10.to_excel(xlsx, sheet_name="在售产品医院个数", index=False)
    check_11.to_excel(xlsx, sheet_name="全部医院历史贡献率等级", index=False)
    
    
