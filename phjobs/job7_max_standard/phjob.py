# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""

from ph_logs.ph_logs import phlogger
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql import functions as func
import os
from pyspark.sql.functions import pandas_udf, PandasUDFType

#def execute(a, b):
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



max_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/"
out_path = "s3a://ph-stream/common/public/max_result/0.0.5/"
project_name = "Beite"
# "Astellas"

product_map_path = max_path  + "/Common_files/product_map_all"
MAX_city_normalize_path = max_path  + "/Common_files/MAX_city_normalize.csv"
packID_ACT_map_path = max_path  + "/Common_files/packID_ACT_map.csv"
# 月更的时候需要修改的max_list文件
max_result_path_list_path = max_path  + "/" + project_name + "/max_result_path_list.csv"
# Molecule_ACT_map_path = 


# 读取

product_map = spark.read.parquet(product_map_path)

MAX_city_normalize = spark.read.csv(MAX_city_normalize_path, header=True)

packID_ACT_map = spark.read.csv(packID_ACT_map_path, header=True)
# packID转为int，product_map里的pfc为int
packID_ACT_map = packID_ACT_map.withColumn("PACK_ID", packID_ACT_map.PACK_ID.cast(IntegerType()))

#Molecule_ACT_map

max_result_path_list = spark.read.csv(max_result_path_list_path, header=True)
max_result_path_list = max_result_path_list.toPandas()

# 储存时间
time_list= []
for i in range(len(max_result_path_list)):
    max_result_path = max_result_path_list.loc[i].path
    time_left = max_result_path_list.loc[i].time_left
    time_list.append(time_left)
    time_right = max_result_path_list.loc[i].time_right
    time_list.append(time_right)
    
    max_result = spark.read.parquet(max_result_path)
    max_result = max_result.withColumn("Date", max_result.Date.cast(IntegerType()))
    max_result = max_result.where((max_result.Date >= time_left) & (max_result.Date <= time_right))
    
    # product
    product_map = product_map.where(product_map.project == project_name) \
                    .withColumnRenamed("pfc", "PACK_ID")
                    
    max_standard = max_result.join(product_map, max_result["Prod_Name"] == product_map["min2"], how="left") \
                            .drop("min2")
    
    # city 标准化：先标准化省，再标准化市
    max_standard = max_standard.join(MAX_city_normalize.select("Province", "标准省份名称").distinct(), on=["Province"], how="left")
    max_standard = max_standard.join(MAX_city_normalize.select("City", "标准省份名称", "标准城市名称").distinct(),
                                on=["标准省份名称", "City"], how="left")
    
    # PACK_ID - ACT
    max_standard = max_standard.join(packID_ACT_map, on=["PACK_ID"], how="left")
    
        
    # 通用名 - ACT
    # max_standard = max_standard.join(Molecule_ACT_map, on=["PACK_ID"], how="left")
    
    # PACK_ID-ACT 匹配不上的都用 通用名-ACT
    # max_standard = max_standard.withColumn("ACT", func.when(max_standard["ATC4_CODE"].isNull(), max_standard["ATC4_molecule"]) \
    #                                            .otherwise(max_standard["ATC4_CODE"]))
    
    # 全量结果汇总
    if i == 0:
        max_standard_all = max_standard
    else:
        max_standard_all = max_standard_all.union(max_standard)

# 全量结果汇总
max_standard_all = max_standard_all.select("project", "Date", "Prod_Name", "Molecule", "PANEL", "DOI", "Predict_Sales", "Predict_Unit", 
                                            "标准商品名", "标准剂型", "标准规格", "标准包装数量", "标准生产企业", "标准省份名称", "标准城市名称", 
                                            "PACK_ID", "ATC4_CODE")
max_standard_all = max_standard_all.withColumn("Date_copy", max_standard_all.Date)
        
# 目录结果汇总
max_standard_brief = max_standard_all.select("project", "Date", "Molecule", "ATC4_CODE").distinct()
    

time_list = [int(x) for x in time_list]
time_range = str(min(time_list)) + '_' + str(max(time_list))

max_standard_path = out_path + "/" + project_name + "_" + time_range + "_max_standard"
max_standard_brief_path = out_path + "/" + project_name + "_" + time_range  + "_max_standard_brief"


# 根据日期分桶写出
max_standard_all = max_standard_all.repartition("Date_copy")
max_standard_all.write.format("parquet").partitionBy("Date_copy") \
    .mode("overwrite").save(max_standard_path)

max_standard_brief = max_standard_brief.repartition(2)
max_standard_brief.write.format("parquet") \
    .mode("overwrite").save(max_standard_brief_path)


