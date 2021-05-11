# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

import random
import string
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from pyspark.sql.functions import lit, udf, col, when
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

    _inputs = str(kwargs["input_factor_paths"]).replace(" ", "").split(",")
    _time = str(kwargs["time"])
    _version = str(kwargs["version"])
    _output = str(kwargs["output_factor_path"])
    _substr_company_tag = "v0.0.1-2020-06-08"
    
    _column_mapping = {
        "CITY": "CITY",
        "FACTOR": "FACTOR",
        "FACTOR_NEW": "FACTOR_NEW AS FACTOR"
    }
    
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
    
    
    def completion_columns(df, cols):
        if "PROVINCE" in cols:
            return df.PROVINCE
        else:
            return lit("null")
    
    
    def get_company_for_url(path):
        tmp = path[path.index(_substr_company_tag) + len(_substr_company_tag) + 1:]
        return tmp[:tmp.index("/")]
    
    
    def get_model_for_url(path):
        tmp = path[path.index(_substr_company_tag) + len(_substr_company_tag) + 1:]
        return tmp.split("/")[-1].split("_")[-1]
    
    
    def mapping_columns(col):
        if col in _column_mapping.keys():
            return _column_mapping[col]
        else:
            return col
    

    def get_df(path):
        company = get_company_for_url(path)
        model = get_model_for_url(path)
        reading = spark.read.parquet(path).drop(*["factor.x", "factor.y"])
        cols = list(map(mapping_columns, list(map(lambda col: col.upper(), reading.columns))))
        df = reading.selectExpr(*cols)
        return df.withColumn("PROVINCE_CM",completion_columns(df, df.columns)) \
            .withColumn("ID", gid()) \
            .withColumn("TIME", lit(_time)) \
            .withColumn("COMPANY", lit(company)) \
            .withColumn("MODEL", lit(model)) \
            .withColumn("VERSION", lit(_version)) \
            .selectExpr("ID", "PROVINCE_CM AS PROVINCE", "CITY", "FACTOR", "TIME", "COMPANY", "MODEL", "VERSION")


    reduce(lambda dfl, dfr: dfl.union(dfr), list(map(get_df, _inputs))) \
        .write \
        .partitionBy("TIME", "COMPANY", "MODEL") \
        .mode("append") \
        .parquet(_output)

    return {}
