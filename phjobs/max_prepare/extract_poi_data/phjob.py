# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

import random
import string
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from pyspark.sql.functions import lit, udf, col, array_contains
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
    
    
    _time = str(kwargs["time"])
    _inputs = str(kwargs["input_poi_paths"]).replace(" ", "").split(",")
    _output = str(kwargs["output_poi_path"])
    _substr_tag = "v0.0.1-2020-06-08"
    _version = str(kwargs["version"])
    
    row = [{"POI": "NAN"}]
    cf = spark.createDataFrame(row)
    cf.collect()
    
    
    def general_id():
        charset = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ' + \
                  'abcdefghijklmnopqrstuvwxyz' + \
                  '0123456789-_'

        charsetLength = len(charset)

        keyLength = 3 * 5

        result = [""]
        for _ in range(keyLength):
            result.append(charset[random.randint(0, charsetLength - 1)])

        return "".join(result)


    gid = udf(general_id, StringType())
    
    
    def get_company_for_url(path):
        tmp = path[path.index(_substr_tag) + len(_substr_tag) + 1:]
        return tmp[:tmp.index("/")]
    
    
    def get_df(path):
        company = get_company_for_url(path)
        readding = spark.read.csv(path, header=True)
        cols = list(map(lambda col: col + " AS " + col.upper(), readding.columns))
        readding = readding.selectExpr(*cols)
        if readding.count() == 0:
            readding = readding.union(cf)
        
        df = readding \
            .withColumn("ID", gid()) \
            .withColumn("TIME", lit(_time)) \
            .withColumn("COMPANY", lit(company)) \
            .withColumn("VERSION", lit(_version))
        return df
        
        
    reduce(lambda dfl, dfr: dfl.union(dfr), list(map(get_df, _inputs))) \
        .write \
        .partitionBy("TIME", "COMPANY") \
        .mode("append") \
        .parquet(_output)
    
    return {}
