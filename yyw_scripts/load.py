from ph_logs.ph_logs import phlogger
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import StringType, IntegerType, DoubleType, BooleanType
from pyspark.sql import functions as func
import os
from pyspark.sql.functions import pandas_udf, PandasUDFType
os.environ["PYSPARK_PYTHON"] = "python3"
spark = SparkSession.builder \
        .master("yarn") \
        .appName("data from s3") \
        .config("spark.driver.memory", "2g") \
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
月更新数据上传
'''    

outdir = '202009'
product_name = '康哲'
 
# 产品匹配表

import pandas as pd
df = pd.read_excel(u"/workspace/YYW_max_project/kangzhe/kangzhe_prod_map.xlsx", dtype="object")
print(df.head(4))

for each in df.columns.values:
  df[each] = df[each].astype("str")
  
df_spark = spark.createDataFrame(df)

for each in df_spark.columns:
  df_spark = df_spark.withColumn(each, func.when(func.col(each) == 'nan', func.lit(None)).otherwise(func.col(each)))

df_spark = df_spark.distinct().repartition(1)
df_spark.write.format("parquet") \
    .mode("overwrite").save("s3a://ph-max-auto/v0.0.1-2020-06-08/" + product_name + "/" + outdir + "/prod_mapping")
    
#  =========  max_result_path_list 调整  =============
max_result_path_list = spark.read.csv("s3a://ph-max-auto/v0.0.1-2020-06-08/" + product_name + "/max_result_path_list.csv", header=True)
max_result_path_list.show()
list_dict = [
  {"project":"康哲","path":"s3a://ph-max-auto/v0.0.1-2020-06-08/康哲/202006/MAX_result/MAX_result_201801_202006_city_level", 
  "time_left":"201801", "time_right":"201912"},
  {"project":"康哲","path":"s3a://ph-max-auto/v0.0.1-2020-06-08/康哲/202009/MAX_result/MAX_result_202001_202009_city_level", 
  "time_left":"202001", "time_right":"202009"}
]

max_result_path_list_new = spark.createDataFrame(list_dict)
max_result_path_list_new = max_result_path_list_new.select("project", "path", "time_left", "time_right")
max_result_path_list_new.show()
max_result_path_list_new.toPandas()["path"].values
max_result_path_list_new = max_result_path_list_new.repartition(1)
max_result_path_list_new.write.format("csv").option("header", "true") \
    .mode("overwrite").save("s3a://ph-max-auto/v0.0.1-2020-06-08/" + product_name + "/max_result_path_list.csv")

   