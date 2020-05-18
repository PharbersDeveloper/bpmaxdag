# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
import click
import numpy as np

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

    product_map_path = max_path + "/" + project_name + "/prod_mapping"
    hospital_mapping_out_path = test_out_path + "/" + project_name + "/hospital_mapping_out"
    product_mapping_out_path = test_out_path + "/" + project_name + "/product_mapping_out"
    need_cleaning_path = max_path_local + "/" + project_name + "/need_cleaning.xlsx"

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
    # product_map_path = "/common/projects/max/Sankyo/prod_mapping"
    product_map = spark.read.parquet(product_map_path)
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
    print(need_cleaning.count())

    # need_cleaning_path = "/user/ywyuan/max/Sankyo/need_cleaning.xlsx"
    if need_cleaning.count() > 0:
        need_cleaning = need_cleaning.toPandas()
        need_cleaning.to_excel(need_cleaning_path)
        print("已输出待清洗文件至", need_cleaning_path)

    raw_data = raw_data.join(product_map_for_rawdata, on="min1", how="left") \
        .drop("S_Molecule") \
        .withColumnRenamed("通用名", "S_Molecule")

    product_mapping_out = raw_data.repartition(2)
    product_mapping_out.write.format("parquet") \
        .mode("overwrite").save(product_mapping_out_path)

    raw_data.show(2)

    return raw_data
