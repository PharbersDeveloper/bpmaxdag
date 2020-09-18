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

def execute(max_path, out_path, project_name, max_path_list):
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
    
    
    # max_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/"
    # out_path = "s3a://ph-stream/common/public/max_result/0.0.5/"
    # project_name = "Beite"
    # "Astellas","Pfizer","Beite"
    
    # 月更的时候需要修改的max_list文件
    if max_path_list == "Empty":
        max_result_path_list_path = max_path  + "/" + project_name + "/max_result_path_list.csv"
    else:
        max_result_path_list_path = max_path_list
    
    # 通用匹配文件
    product_map_path = max_path  + "/Common_files/product_map_all"
    molecule_ACT_path = max_path  + "/Common_files/product_map_all_ATC.csv"
    MAX_city_normalize_path = max_path  + "/Common_files/MAX_city_normalize.csv"
    packID_ACT_map_path = max_path  + "/Common_files/packID_ACT_map.csv"
    
    # mapping用文件：注意各种mapping的去重，唯一匹配
    
    # 城市标准化
    MAX_city_normalize = spark.read.csv(MAX_city_normalize_path, header=True)
                        
    # 产品信息
    # 有的min2结尾有空格与无空格的是两条不同的匹配
    product_map = spark.read.parquet(product_map_path)
    product_map = product_map.where(product_map.project == project_name)
    # 去重：保证每个min2只有一条信息, dropDuplicates会取first
    product_map = product_map.dropDuplicates(["min2"])
    product_map = product_map.withColumn("pfc", product_map.pfc.cast(IntegerType())) \
                        .withColumnRenamed("pfc", "PACK_ID") \
                        .withColumn("min2", func.regexp_replace("min2", "&amp;", "&")) \
                        .withColumn("min2", func.regexp_replace("min2", "&lt;", "<")) \
                        .withColumn("min2", func.regexp_replace("min2", "&gt;", ">"))
    
    
    # product_map_all_ATC: 有补充的新的 PACK_ID - 标准通用名 - ACT （0是缺失）
    molecule_ACT_map = spark.read.csv(molecule_ACT_path, header=True)
    molecule_ACT_map = molecule_ACT_map.where(molecule_ACT_map.project == project_name)
    molecule_ACT_map = molecule_ACT_map.select("通用名", "MOLE_NAME_CH", "ATC4_CODE") \
                        .withColumnRenamed("MOLE_NAME_CH", "MOLE_NAME_CH_2") \
                        .withColumnRenamed("ATC4_CODE", "ATC4_2") \
                        .dropDuplicates(["通用名"])
    molecule_ACT_map = molecule_ACT_map.withColumn("MOLE_NAME_CH_2", func.when(molecule_ACT_map.MOLE_NAME_CH_2 == "0", None).otherwise(molecule_ACT_map.MOLE_NAME_CH_2)) \
                            .withColumn("ATC4_2", func.when(molecule_ACT_map.ATC4_2 == "0", None).otherwise(molecule_ACT_map.ATC4_2))
    
    # packID_ACT_map：PACK_ID - 标准通用名 - ACT, 无缺失
    packID_ACT_map = spark.read.csv(packID_ACT_map_path, header=True)
    packID_ACT_map = packID_ACT_map.select("PACK_ID", "MOLE_NAME_CH", "ATC4_CODE").distinct() \
                        .withColumn("PACK_ID", packID_ACT_map.PACK_ID.cast(IntegerType())) \
                        .withColumnRenamed("MOLE_NAME_CH", "MOLE_NAME_CH_1") \
                        .withColumnRenamed("ATC4_CODE", "ATC4_1")
                        
    max_result_path_list = spark.read.csv(max_result_path_list_path, header=True)
    max_result_path_list = max_result_path_list.toPandas()
    
    # 汇总max_result_path结果
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
        
        # 杨森6月的max结果 衡水市- 湖北省 错误，先强制改为衡水市- 河北省
        if project_name == "Janssen":
            max_result = max_result.withColumn("Province", func.when(max_result.City == "衡水市", func.lit("河北省")) \
                                                            .otherwise(max_result.Province))
        
        # product 匹配 PACK_ID, 通用名, 标准商品名, 标准剂型, 标准规格, 标准包装数量, 标准生产企业
        max_result = max_result.withColumn("Prod_Name_tmp", max_result.Prod_Name)
        max_result = max_result.withColumn("Prod_Name_tmp", func.regexp_replace("Prod_Name_tmp", "&amp;", "&")) \
                                .withColumn("Prod_Name_tmp", func.regexp_replace("Prod_Name_tmp", "&lt;", "<")) \
                                .withColumn("Prod_Name_tmp", func.regexp_replace("Prod_Name_tmp", "&gt;", ">"))
        max_standard = max_result.join(product_map, max_result["Prod_Name_tmp"] == product_map["min2"], how="left") \
                                .drop("min2","Prod_Name_tmp")
        
        # PACK_ID - 标准通用名 - ACT
        max_standard = max_standard.join(packID_ACT_map, on=["PACK_ID"], how="left")
        
        # 通用名, PackID, MOLE_NAME_CH, ATC4_CODE
        max_standard = max_standard.join(molecule_ACT_map, on=["通用名"], how="left")
        
        # packID_ACT_map 匹配不上的用 molecule_ACT_map 
        max_standard = max_standard.withColumn("ATC", func.when(max_standard["ATC4_1"].isNull(), max_standard["ATC4_2"]) \
                                                    .otherwise(max_standard["ATC4_1"])) \
                                    .withColumn("标准通用名", func.when(max_standard["MOLE_NAME_CH_1"].isNull(), max_standard["MOLE_NAME_CH_2"]) \
                                                    .otherwise(max_standard["MOLE_NAME_CH_1"])) \
                                    .drop("ATC4_1", "ATC4_2", "MOLE_NAME_CH_1", "MOLE_NAME_CH_2")
        
        # 没有标准通用名的 用原始的通用名
        max_standard = max_standard.withColumn("标准通用名", func.when(max_standard['标准通用名'].isNull(), max_standard['通用名']) \
                                                                .otherwise(max_standard['标准通用名']))
        
        # city 标准化：
        '''
        先标准化省，再用(标准省份-City)标准化市
        '''
        max_standard = max_standard.join(MAX_city_normalize.select("Province", "标准省份名称").distinct(), on=["Province"], how="left")
        max_standard = max_standard.join(MAX_city_normalize.select("City", "标准省份名称", "标准城市名称").distinct(),
                                    on=["标准省份名称", "City"], how="left")
        
        
        # 全量结果汇总
        if i == 0:
            max_standard_all = max_standard
        else:
            max_standard_all = max_standard_all.union(max_standard)
    
    
    # 全量结果汇总
    max_standard_all = max_standard_all.select("project", "Date", "Prod_Name", "Molecule", "PANEL", "DOI", "Predict_Sales", "Predict_Unit", 
                                               "标准通用名", "标准商品名", "标准剂型", "标准规格", "标准包装数量", "标准生产企业", "标准省份名称", "标准城市名称", 
                                                "PACK_ID", "ATC")
    max_standard_all = max_standard_all.withColumn("Date_copy", max_standard_all.Date)
            
    # 目录结果汇总,
    max_standard_brief = max_standard_all.select("project", "Date", "标准通用名", "ATC", "DOI").distinct()
        
    # 获取时间范围
    time_list = [int(x) for x in time_list]
    time_range = str(min(time_list)) + '_' + str(max(time_list))
    
    # 输出文件名，时间区间
    max_standard_path = out_path + "/" + project_name + "_" + time_range + "_max_standard"
    max_standard_brief_path = out_path + "/" + project_name + "_" + time_range  + "_max_standard_brief"
    
    
    # 根据日期分桶写出
    max_standard_all = max_standard_all.repartition("Date_copy")
    max_standard_all.write.format("parquet").partitionBy("Date_copy") \
        .mode("overwrite").save(max_standard_path)
    # 输出brief结果
    max_standard_brief = max_standard_brief.repartition(1)
    max_standard_brief.write.format("parquet") \
        .mode("overwrite").save(max_standard_brief_path)



        
        