# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

import random
import string
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from pyspark.sql.functions import lit, udf, col
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
    
    _inputs = str(kwargs["input_outler_paths"]).replace(" ", "").split(",")
    _time = str(kwargs["time"])
    _version = str(kwargs["version"])
    _output = str(kwargs["output_outler_path"])
    _drop_matchers = str(kwargs["drop_matchers"]).split(",")
    _hospital_univers_input = str(kwargs["hospital_univers"])
    _substr_company_tag = "v0.0.1-2020-06-08"
    
    hosp_mapping = spark.read.parquet(_hospital_univers_input)
    hosp_mapping.persist()

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
        tmp = path[path.index(_substr_company_tag) + len(_substr_company_tag) + 1:]
        return tmp[:tmp.index("/")]
    
    
    def get_model_for_url(path):
        tmp = path[path.index(_substr_company_tag) + len(_substr_company_tag) + 1:]
        return tmp.split("/")[-1].split("_")[-1]
    
    
    def convert_upper_columns(column):
        return column.upper()


    def drop_other_columns(df):
        columns = [item for item in df.columns if any(xitem in item for xitem in _drop_matchers)]
        return df.drop(*columns)
    
    
    def get_df(path):
        company = get_company_for_url(path)
        model = get_model_for_url(path)
        reading = spark.read.parquet(path)
        old_columns = reading.schema.names
        new_columns = list(map(convert_upper_columns, old_columns))
        df = drop_other_columns(reduce(lambda reading, idx: reading.withColumnRenamed(old_columns[idx], new_columns[idx]), range(len(old_columns)), reading))
        select_str = "PANEL_ID,PANEL,SEG".split(",")
        
        outler_df = df.selectExpr(*select_str) \
            .join(hosp_mapping, [col("PANEL_ID") == col("PHA_ID")], "left_outer") \
            .selectExpr("PHA_ID", "HOSP_NAME", 
                        "CAST(DRUGINCOME_RMB AS double) AS DRUGINCOME_RMB", 
                        "CAST(BED_NUM AS double) AS BED_NUM", 
                        "CAST(PANEL AS double) AS PANEL", 
                        "CAST(SEG AS double) AS SEG") \
            .withColumn("ID", gid()) \
            .withColumn("TIME", lit(_time)) \
            .withColumn("COMPANY", lit(company)) \
            .withColumn("MODEL", lit(model)) \
            .withColumn("VERSION", lit(_version))
        return outler_df
    
    
    reduce(lambda dfl, dfr: dfl.union(dfr), list(map(get_df, _inputs))) \
        .write \
        .partitionBy("TIME", "COMPANY", "MODEL") \
        .mode("append") \
        .parquet(_output)
    
    
    return {}
