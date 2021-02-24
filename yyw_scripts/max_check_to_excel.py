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
    .appName("ywyuan") \
    .config("spark.driver.cores", "1") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.instances", "1") \
    .config('spark.sql.codegen.wholeStage', False) \
    .enableHiveSupport() \
    .getOrCreate()

access_key = os.getenv("AWS_ACCESS_KEY_ID", "AKIAWPBDTVEAEU44ZAGT")
secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "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")
if access_key:
    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
    spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
    spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.cn-northwest-1.amazonaws.com.cn")

    
'''
合并s3上的max_check/.csv 为excel
'''
project_name = '贝达'
outdir = '202012'
outdir_local = "/home/ywyuan/tmp_file"

max_path = 's3a://ph-max-auto/v0.0.1-2020-06-08/'
raw_data_check_path = max_path + '/' + project_name + '/' + outdir + '/max_check/'
check_1_path = raw_data_check_path + '/check_1_byProduct.csv'
check_2_path = raw_data_check_path + '/check_2_byCity.csv'
check_3_path = raw_data_check_path + '/check_3_补数比例.csv'
check_4_path = raw_data_check_path + '/check_4_放大比例.csv'
check_5_path = raw_data_check_path + '/check_5_产品个数.csv'
check_6_path = raw_data_check_path + '/check_6_样本医院个数.csv'


check_1 = spark.read.csv(check_1_path, header=True)
check_1 = check_1.toPandas()
check_1[check_1.columns[5:]]=check_1[check_1.columns[5:]].astype(float)

check_2 = spark.read.csv(check_2_path, header=True)
check_2 = check_2.toPandas()
check_2[check_2.columns[6:]]=check_2[check_2.columns[6:]].astype(float)

check_3 = spark.read.csv(check_3_path, header=True)
check_3 = check_3.toPandas()
check_3[check_3.columns[2:]]=check_3[check_3.columns[2:]].astype(float)

check_4 = spark.read.csv(check_4_path, header=True)
check_4 = check_4.toPandas()
check_4[check_4.columns[2:]]=check_4[check_4.columns[2:]].astype(float)

check_5 = spark.read.csv(check_5_path, header=True)
check_5 = check_5.toPandas()
check_5[check_5.columns]=check_5[check_5.columns].astype(float)

check_6 = spark.read.csv(check_6_path, header=True)
check_6 = check_6.toPandas()
check_6[check_6.columns]=check_6[check_6.columns].astype(float)


with pd.ExcelWriter(outdir_local + "/" + project_name + "_" + outdir + "_max_check.xlsx") as xlsx:
    check_1.to_excel(xlsx, sheet_name="check_byProduct", index=False)
    check_2.to_excel(xlsx, sheet_name="check_bycity", index=False)
    check_3.to_excel(xlsx, sheet_name="check补数比例", index=False)
    check_4.to_excel(xlsx, sheet_name="check放大比例", index=False)
    check_5.to_excel(xlsx, sheet_name="每个月产品个数", index=False)
    check_6.to_excel(xlsx, sheet_name="每个月样本医院个数", index=False)

    
    
