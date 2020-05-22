# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
import click
import numpy as np
import logging

from pyspark.sql import SparkSession
import time
from pyspark.sql.types import *
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql import functions as func


@click.command()
@click.option('--max_path')
@click.option('--max_path_local')
@click.option('--project_name')
@click.option('--minimum_product_columns')
@click.option('--minimum_product_sep')
@click.option('--minimum_product_newname')
@click.option('--need_cleaning_cols')
@click.option('--test_out_path')

def execute(max_path, max_path_local, project_name, minimum_product_columns, minimum_product_sep,minimum_product_newname, need_cleaning_cols, test_out_path):
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("sparkOutlier") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.instance", "2") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()
        
    # logging配置
    logger = logging.getLogger("log")
    logger.setLevel(level=logging.INFO)
    file_handler = logging.FileHandler('job2_product_mapping.log','w')
    file_handler.setLevel(level=logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - [line:%(lineno)d] - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    logger.info('job2_product_mapping')
        
    # 输入输出
    product_map_path = max_path + "/" + project_name + "/prod_mapping"
    hospital_mapping_out_path = test_out_path + "/" + project_name + "/hospital_mapping_out"
    product_mapping_out_path = test_out_path + "/" + project_name + "/product_mapping_out"
    need_cleaning_path = max_path_local + "/" + project_name + "/need_cleaning.xlsx"
    
    # =========== 数据检查 =============
    logger.info('数据检查-start')
    
    # 存储文件的缺失列
    misscols_dict = {}

    # product_map file
    # product_map_path = "/common/projects/max/Sankyo/prod_mapping"
    product_map = spark.read.parquet(product_map_path)
    colnames_product_map = product_map.columns
    misscols_dict.setdefault("universe", [])
    if ("标准通用名" not in colnames_product_map) and ("通用名"  not in colnames_product_map):
        misscols_dict["product_map"].append("标准通用名")
    if "min1" not in colnames_product_map:
        misscols_dict["product_map"].append("min1")
    if "min2" not in colnames_product_map:
        misscols_dict["product_map"].append("min2")
    if "标准商品名" not in colnames_product_map:
        misscols_dict["product_map"].append("标准商品名") 
    
    # 判断输入文件是否有缺失列
    for eachfile in misscols_dict.keys():
        if len(misscols_dict[eachfile]) == 0:
            del misscols_dict[eachfile]
    # 如果有缺失列，则报错，停止运行
    if misscols_dict:
        logger.error('miss columns: %s' % (misscols_dict))
        raise ValueError('miss columns: %s' % (misscols_dict))
        
    logger.info('数据检查-Pass')
    
    # =========== 数据执行 =============
    logger.info('数据执行-start')
    
    # raw_data_job1_out_path = "/user/ywyuan/max/Sankyo/raw_data_job1_out"
    raw_data = spark.read.parquet(hospital_mapping_out_path)
    raw_data = raw_data.withColumn("Brand", func.when(func.isnull(raw_data.Brand), raw_data.Molecule).
                                   otherwise(raw_data.Brand))

    # concat_multi_cols
    # minimum_product_columns = ["Brand", "Form", "Specifications", "Pack_Number", "Manufacturer"]
    # minimum_product_sep = ""
    # minimum_product_newname = "min1"
    minimum_product_columns = minimum_product_columns.split(", ")
    for colname, coltype in raw_data.dtypes:
        if coltype == "logical":
            raw_data = raw_data.withColumn(colname, raw_data[colname].cast(StringType()))

    raw_data = raw_data.withColumn("tmp", func.when(func.isnull(raw_data[minimum_product_columns[0]]), func.lit("NA")).
                                   otherwise(raw_data[minimum_product_columns[0]]))

    for col in minimum_product_columns[1:]:
        raw_data = raw_data.withColumn("tmp", func.concat(
            raw_data["tmp"],
            func.lit(minimum_product_sep),
            func.when(func.isnull(raw_data[col]), func.lit("NA")).otherwise(raw_data[col])))

    raw_data = raw_data.withColumnRenamed("tmp", minimum_product_newname)

    # product_map
    product_map = product_map.withColumnRenamed("标准通用名", "通用名") \
        .withColumnRenamed("标准途径", "std_route")
    if "std_route" not in product_map.columns:
        product_map = product_map.withColumn("std_route", func.lit(''))

    product_map_for_needclean = product_map.select("min1").distinct()
    product_map_for_rawdata = product_map.select("min1", "min2", "通用名", "std_route", "标准商品名").distinct()
    
    # 输出待清洗
    # need_cleaning_cols = ["Molecule", "min1", "Route", "Corp"]
    need_cleaning_cols = need_cleaning_cols.split(", ")
    need_cleaning_cols[1:1] = minimum_product_columns
    need_cleaning = raw_data.join(product_map_for_needclean, on="min1", how="left_anti") \
        .select(need_cleaning_cols) \
        .distinct()
    logger.info('待清洗行数: ' + str(need_cleaning.count()))

    # need_cleaning_path = "/user/ywyuan/max/Sankyo/need_cleaning.xlsx"
    if need_cleaning.count() > 0:
        need_cleaning = need_cleaning.toPandas()
        need_cleaning.to_excel(need_cleaning_path)
        #print("已输出待清洗文件至:", need_cleaning_path)
        logger.info('已输出待清洗文件至:  ' + need_cleaning_path)

    raw_data = raw_data.join(product_map_for_rawdata, on="min1", how="left") \
        .drop("S_Molecule") \
        .withColumnRenamed("通用名", "S_Molecule")

    product_mapping_out = raw_data.repartition(2)
    product_mapping_out.write.format("parquet") \
        .mode("overwrite").save(product_mapping_out_path)

    raw_data.show(2)
    
    logger.info('数据执行-Finish')
    
    # =========== 数据验证 =============
    # 与原R流程运行的结果比较正确性
    if True:
        logger.info('数据验证-start')
        
        R_product_mapping_out_path = "/user/ywyuan/max/Sankyo/Rout/product_mapping_out"
        R_product_mapping_out = spark.read.parquet(R_product_mapping_out_path)
        
        # 检查内容：列的类型，列的值
        for colname, coltype in raw_data.dtypes:
            # 数据类型检查
            if R_product_mapping_out.select(colname).dtypes[0][1] != coltype:
                logger.warning ("different type columns: "  + colname + ", " + coltype + ", " + "right type: " + R_product_mapping_out.select(colname).dtypes[0][1])
    
            # 数值列的值检查
            if coltype == "double" or coltype == "int":
                # year_month, Pack_Number, Sales, Units, Units_Box, BI_hospital_code, Month, Year
                sum_raw_data = raw_data.groupBy().sum(colname).toPandas().iloc[0,0]
                sum_R = R_product_mapping_out.groupBy().sum(colname).toPandas().iloc[0,0]
                # print (colname, sum_raw_data, sum_R)
                if (sum_raw_data - sum_R) != 0:
                    logger.warning ("different value(sum) columns: " + colname + ", " + str(sum_raw_data) + ", " + "right value: " + str(sum_R))
                    
        logger.info('数据验证-Finish')
        
    # =========== return =============          
    return raw_data