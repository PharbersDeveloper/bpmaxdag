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
@click.option('--a')
@click.option('--b')
def execute(a, b):
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("sparkOutlier") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.instance", "2") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()
    
    raw_data_job1_out_path = "/user/yyw/max/Sankyo/raw_data_job1_out"
    raw_data = spark.read.parquet(raw_data_job1_out_path)
    raw_data = raw_data.withColumn("Brand", func.when(func.isnull(raw_data.Brand), raw_data.Molecule).
                                       otherwise(raw_data.Brand))
    # concat_multi_cols
    min_content = ["Brand", "Form", "Specifications", "Pack_Number", "Manufacturer"]
    min1_sep = ""
    new_col = "min1"

    for colname, type in raw_data.dtypes:
        if type == "logical":
            raw_data = raw_data.withColumn(colname, raw_data[colname].cast(StringType()))
                                   
    raw_data = raw_data.withColumn("tmp", func.when(func.isnull(raw_data[min_content[0]]), func.lit("NA")).
                                   otherwise(raw_data[min_content[0]]))

    for col in min_content[1:]:
        raw_data = raw_data.withColumn("tmp", func.concat(
            raw_data["tmp"],
            func.lit(min1_sep),
            func.when(func.isnull(raw_data[col]), func.lit("NA")).otherwise(raw_data[col])))

    raw_data = raw_data.withColumnRenamed("tmp", new_col)                                      
