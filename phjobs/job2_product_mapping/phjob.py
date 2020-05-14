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
@click.option('--raw_data_job1_out_path')
@click.option('--min_content')
@click.option('--min1_sep')
@click.option('--new_col')
@click.option('--map_path')
@click.option('--need_cleaning_cols')
@click.option('--need_cleaning_path')
@click.option('--raw_data_job2_out_path')

def execute(raw_data_job1_out_path, min_content, min1_sep, new_col, map_path, need_cleaning_cols, need_cleaning_path,
            raw_data_job2_out_path):
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("sparkOutlier") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.instance", "2") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()

    # raw_data_job1_out_path = "/user/ywyuan/max/Sankyo/raw_data_job1_out"
    raw_data = spark.read.parquet(raw_data_job1_out_path)
    raw_data = raw_data.withColumn("Brand", func.when(func.isnull(raw_data.Brand), raw_data.Molecule).
                                   otherwise(raw_data.Brand))

    # concat_multi_cols
    # min_content = ["Brand", "Form", "Specifications", "Pack_Number", "Manufacturer"]
    # min1_sep = ""
    # new_col = "min1"
    min_content = min_content.split(", ")
    for colname, coltype in raw_data.dtypes:
        if coltype == "logical":
            raw_data = raw_data.withColumn(colname, raw_data[colname].cast(StringType()))

    raw_data = raw_data.withColumn("tmp", func.when(func.isnull(raw_data[min_content[0]]), func.lit("NA")).
                                   otherwise(raw_data[min_content[0]]))

    for col in min_content[1:]:
        raw_data = raw_data.withColumn("tmp", func.concat(
            raw_data["tmp"],
            func.lit(min1_sep),
            func.when(func.isnull(raw_data[col]), func.lit("NA")).otherwise(raw_data[col])))

    raw_data = raw_data.withColumnRenamed("tmp", new_col)

    # map
    # map_path = "/common/projects/max/Sankyo/prod_mapping"
    map = spark.read.parquet(map_path)
    map = map.withColumnRenamed("标准通用名", "通用名") \
        .withColumnRenamed("标准途径", "std_route")
    if "std_route" not in map.columns:
        map = map.withColumn("std_route", func.lit(''))

    map1 = map.select("min1").distinct()
    mp = map.select("min1", "min2", "通用名", "std_route", "标准商品名").distinct()

    # 输出待清洗
    # need_cleaning_cols = ["Molecule", "min1", "Route", "Corp"]
    need_cleaning_cols = need_cleaning_cols.split(", ")
    need_cleaning_cols[1:1] = min_content
    need_cleaning = raw_data.join(map1, on="min1", how="left_anti") \
        .select(need_cleaning_cols) \
        .distinct()
    print(need_cleaning.count())

    # need_cleaning_path = "need_cleaning.xlsx"
    if need_cleaning.count() > 0:
        need_cleaning = need_cleaning.toPandas()
        need_cleaning.to_excel(need_cleaning_path)
        print("已输出待清洗文件至", need_cleaning_path)

    raw_data = raw_data.join(mp, on="min1", how="left") \
        .drop("S_Molecule") \
        .withColumnRenamed("通用名", "S_Molecule")

    raw_data_job2_out = raw_data.repartition(2)
    raw_data_job2_out.write.format("parquet") \
        .mode("overwrite").save(raw_data_job2_out_path)

    raw_data.show(2)

    return raw_data
