# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

import random
import string
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from pyspark.sql.functions import lit, udf, col, array, explode
from functools import reduce
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession


def execute(**kwargs):
    """
        please input your code below
        get spark session: spark = kwargs["spark"]()
    """
    logger = phs3logger(kwargs["job_id"], LOG_DEBUG_LEVEL)
    logger.info("当前 owner 为 " + str(kwargs["owner"]))
    logger.info("当前 run_id 为 " + str(kwargs["run_id"]))
    logger.info("当前 job_id 为 " + str(kwargs["job_id"]))
    spark = kwargs["spark"]()
    
    
    _input_publish_paths = str(kwargs["input_publish_paths"]).replace(" ", "").split(",")
    _input_notarrived_paths = str(kwargs["input_notarrived_paths"]).replace(" ", "").split(",")
    _time = str(kwargs["time"])
    _output = str(kwargs["output"])
    _version = str(kwargs["version"])
    
    
    format_num_to_str = udf(lambda x: str(x).replace(".0", "").zfill(6), StringType())
    
    def get_type_name(path, key):
        name = path.split("/")[-1][:len(key)]
        return name.upper()
    
    
    def get_date(path, key):
        date = path.split("/")[-1][len(key):].replace(".csv", "")
        return date 
    
    
    def get_publish_df(path):
        _type = get_type_name(path, "Published")
        _date = get_date(path, "Published")
        d = list(map(lambda x: lit(_date + "%02d" % (x + 1)), range(12)))
        readding = spark.read.csv(path, header=True).selectExpr("ID", "Source AS VALUE") \
            .withColumn("ID", format_num_to_str(col("ID"))) \
            .withColumn("TYPE", lit(_type)) \
            .withColumn("TIME", lit(_time)) \
            .withColumn("COMPANY", lit("PHARBERS")) \
            .withColumn("DATES", array(d)) \
            .withColumn("DATE", explode(col("DATES"))) \
            .withColumn("VERSION", lit(_version)) \
            .drop("DATES")
        return readding
    
    
    def get_not_arrived_df(path):
        _type = get_type_name(path, "Not_arrived")
        # _date = get_date(path, "Not_arrived")
        readding = spark.read.csv(path, header=True).selectExpr("ID", "Date AS DATE") \
            .withColumn("ID", format_num_to_str(col("ID"))) \
            .withColumn("TYPE", lit(_type)) \
            .withColumn("TIME", lit(_time)) \
            .withColumn("COMPANY", lit("PHARBERS")) \
            .withColumn("VALUE", lit("")) \
            .withColumn("VERSION", lit(_version))
        return readding
        
        
    publish_df = reduce(lambda dfl, dfr: dfl.union(dfr), list(map(get_publish_df, _input_publish_paths))) \
        .selectExpr("ID", "TYPE", "VALUE", "DATE", "TIME", "COMPANY", "VERSION")
        
    not_arrived_df = reduce(lambda dfl, dfr: dfl.union(dfr), list(map(get_not_arrived_df, _input_notarrived_paths))) \
        .selectExpr("ID", "TYPE", "VALUE", "DATE", "TIME", "COMPANY", "VERSION")
    
    un_df = publish_df.union(not_arrived_df)
    un_df.write \
        .partitionBy("TIME", "COMPANY") \
        .mode("append") \
        .parquet(_output)
    return {}
