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

def execute(max_path, project_list):
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
        
    project_list_dict={}
    for each in project_list.replace(" ","").split(","):
        project_name = each.split(":")[0]
        month = each.split(":")[1]
        project_list_dict[project_name]=month
            
    # project_list="Astellas:202006,AZ:202006,Beite:202006, Gilead:202006, Janssen:202006, Mylan:202006, NHWA:202006, Pfizer:202006,Qilu:202006, Sankyo:202006, Sanofi:202006, Servier:202006, Tide:202006, XLT:202006, 京新:202006, 奥鸿:202005, 康哲:202006, 汇宇:202006, 海坤:202006, 贝达:202006"
    
    # max_path = 's3a://ph-max-auto/v0.0.1-2020-06-08/'
    
    
    # ========== 判断是否有缺失列 =========
    
    misscols_dict = {}
    for key, value in project_list_dict.items():
        prod_mapping_path = max_path + '/' + key + '/' + value + '/prod_mapping'
        product_map = spark.read.parquet(prod_mapping_path)
        
        if key == "Sanofi" or key == "AZ":
            product_map = product_map.withColumnRenamed(product_map.columns[21], "pfc")
        
        colnames_product_map = product_map.columns
        misscols_dict.setdefault(prod_mapping_path, [])
        if ("标准通用名" not in colnames_product_map) and ("通用名_标准"  not in colnames_product_map)  \
        and ("药品名称_标准"  not in colnames_product_map) and ("通用名"  not in colnames_product_map) \
        and ("S_Molecule_Name"  not in colnames_product_map):
            misscols_dict[prod_mapping_path].append("标准通用名")
        if ("min2" not in colnames_product_map) and ("min1_标准" not in colnames_product_map):
            misscols_dict[prod_mapping_path].append("min2")
        if ("pfc" not in colnames_product_map) and ("packcode" not in colnames_product_map) \
        and ("Pack_ID" not in colnames_product_map) and ("Pack_Id" not in colnames_product_map) \
        and ("PackID" not in colnames_product_map) and ("packid" not in colnames_product_map):
            misscols_dict[prod_mapping_path].append("pfc")
            
    # 判断输入文件是否有缺失列
    misscols_dict_final = {}
    for eachfile in misscols_dict.keys():
        if len(misscols_dict[eachfile]) != 0:
            misscols_dict_final[eachfile] = misscols_dict[eachfile]
    # 如果有缺失列，则报错，停止运行
    if misscols_dict_final:
        phlogger.error('miss columns: %s' % (misscols_dict_final))
        raise ValueError('miss columns: %s' % (misscols_dict_final))
        
    # ========== 合并文件，提取信息 =========
    index = 0
    for key, value in project_list_dict.items():
        print(key)
        
        prod_mapping_path = max_path + '/' + key + '/' + value + '/prod_mapping'
        product_map = spark.read.parquet(prod_mapping_path)
        
        # 列名清洗统一
        if key == "Sanofi" or key == "AZ":
            product_map = product_map.withColumnRenamed(product_map.columns[21], "pfc")
        
        for col in product_map.columns:
            if col in ["标准通用名", "通用名_标准", "药品名称_标准", "S_Molecule_Name"]:
                product_map = product_map.withColumnRenamed(col, "通用名")
            if col in ["min1_标准"]:
                product_map = product_map.withColumnRenamed(col, "min2")
            if col in ["packcode", "Pack_ID", "Pack_Id", "PackID", "packid"]:
                product_map = product_map.withColumnRenamed(col, "pfc")
            if col in ["商品名_标准", "S_Product_Name"]:
                product_map = product_map.withColumnRenamed(col, "标准商品名")
            if col in ["剂型_标准", "Form_std", "S_Dosage"]:
                product_map = product_map.withColumnRenamed(col, "标准剂型")
            if col in ["规格_标准", "Specifications_std", "药品规格_标准", "S_Pack"]:
                product_map = product_map.withColumnRenamed(col, "标准规格")
            if col in ["包装数量2", "包装数量_标准", "Pack_Number_std", "S_PackNumber", "最小包装数量"]:
                product_map = product_map.withColumnRenamed(col, "标准包装数量")
            if col in ["标准企业", "生产企业_标准", "Manufacturer_std", "S_CORPORATION", "标准生产厂家"]:
                product_map = product_map.withColumnRenamed(col, "标准生产企业")
        if key == "Janssen" or key == "NHWA":
            if "标准剂型" not in product_map.columns:
                product_map = product_map.withColumnRenamed("剂型", "标准剂型")
            if "标准规格" not in product_map.columns:
                product_map = product_map.withColumnRenamed("规格", "标准规格")
            if "标准生产企业" not in product_map.columns:
                product_map = product_map.withColumnRenamed("生产企业", "标准生产企业")
            if "标准包装数量" not in product_map.columns:
                product_map = product_map.withColumnRenamed("包装数量", "标准包装数量")
                
        # 去重：保证每个min2只有一条信息, 会取first
        product_map = product_map.dropDuplicates(["min2"])
        # 选取需要的列
        product_map = product_map \
                        .select("min2", "pfc", "通用名", "标准商品名", "标准剂型", "标准规格", "标准包装数量", "标准生产企业") \
                        .withColumn("pfc", product_map["pfc"].cast(IntegerType())) \
                        .withColumn("标准包装数量", product_map["标准包装数量"].cast(IntegerType())) \
                        .distinct()
        # pfc为0替换为null
        product_map = product_map.withColumn("pfc", func.when(product_map.pfc == 0, None).otherwise(product_map.pfc)).distinct()
        product_map = product_map.withColumn("project", func.lit(key)).distinct()
        
        if index == 0:
            product_map_all = product_map
        else:
            product_map_all = product_map_all.union(product_map)
        index += 1
    
       
    product_map_all.show(4)    
    
    product_map_all = product_map_all.repartition(1)
    product_map_all.write.format("parquet") \
        .mode("overwrite").save("s3a://ph-max-auto/v0.0.1-2020-06-08/Common_files/extract_data_files/product_map_all")
    # 保存为csv的时候会丢失尾部空格
    product_map_all.write.format("csv").option("header", "true") \
        .mode("overwrite").save("s3a://ph-max-auto/v0.0.1-2020-06-08/Common_files/extract_data_files/product_map_all.csv")
    
