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
    
    _substr_tag = "v0.0.1-2020-06-08"
    _inputs = str(kwargs["raw_inputs"]).replace(" ", "").split(",")
    _time = str(kwargs["time"])
    _output = str(kwargs["clean_output"]) + _time
    
    
    def col_to_upper(col):
        return col.upper()
    
    
    def get_company_for_url(path):
        tmp = path[path.index(_substr_tag) + len(_substr_tag) + 1:]
        return tmp[:tmp.index("/")]
    
    
    def clean_cols(cols):
        if ("S_MOLECULE" not in cols):
            return list(map(lambda col: col + " AS S_MOLECULE" if col == "MOLECULE" else col, cols))
        else:
            return cols
    
    
    format_num_to_str = udf(lambda x: str(x).replace(".0", "").zfill(6), StringType())
    
    
    def get_df(path):
        company = get_company_for_url(path)
        original_raw_df = spark.read.parquet(path)
        cols = list(map(col_to_upper, original_raw_df.columns))
        df = original_raw_df \
            .selectExpr(*clean_cols(cols)) \
            .withColumn("ID", col("ID")) \
            .withColumn("TIME", lit(_time)) \
            .withColumn("COMPANY", lit(company)) \
            .drop("PATH").drop("SHEET") \
            .drop("ORG_MEASURE").drop("UNITS_BOX")
        
        if "RAW_HOSP_NAME" not in df.columns:
            df = df.withColumn("RAW_HOSP_NAME", lit("0").cast(StringType()))
        
        return df.selectExpr("ID", "DATE", "RAW_HOSP_NAME", "S_MOLECULE AS MOLECULE", "BRAND", "FORM", 
            "SPECIFICATIONS", "PACK_NUMBER", "MANUFACTURER", "SALES", "UNITS", 
            "SOURCE", "TIME", "COMPANY")



    # list(map(get_df, _inputs))
    df = reduce(lambda x, y: x.union(y), list(map(get_df, _inputs)))
    df.write.mode("overwrite").parquet(_output)
    
    
    
    return {}
