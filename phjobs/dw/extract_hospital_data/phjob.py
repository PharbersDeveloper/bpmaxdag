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
    _input = kwargs["input"]
    _output = kwargs["output"].replace("${run_id}", str(kwargs["run_id"]))

    # TODO 提出参数化
    custom_columns = {
        "CITYGROUP": "CITYGROUP",
        "CITY_TIER": "CITY_TIER as CITYGROUP"
    }
    drop_matchers = "__,_GYC,LEVEL,综合专科,专科综合,CPA,是否在2020出版名单,SPECIALTY_1_标准".split(",")

    def convert_upper_columns(column):
        # if column in custom_columns.keys():
        #     return custom_columns[column]
        # else:
        return column.upper()

    def drop_other_columns(df):
        columns = [item for item in df.columns if any(xitem in item for xitem in drop_matchers)]
        return df.drop(*columns)
    
    def completion_column(columns):
        is_contains = list(filter(lambda x: x == "CITYGROUP" or x == "CITY_TIER", columns))
        if len(is_contains) > 1:
            columns = list(filter(lambda x: x != "CITY_TIER", columns))
        
        def fl(c):
            if c in custom_columns.keys():
                return custom_columns[c]
        return list(filter(lambda x: x is not None, list(map(fl, columns))))

    def check_hospital_name(df):
        if "HOSP_NAME" not in df.columns:
            # un = spark.read.parquet("s3a://ph-max-auto/2020-08-11/data_matching/refactor/data/DIMENSION/HOSPITAL_DIMENSION/") \
            #     .withColumnRenamed("HOSP_LEVEL", "DM_HOSP_LEVEL") \
            #     .withColumnRenamed("REGION", "DM_REGION") \
            #     .withColumnRenamed("LOCATION", "DM_LOCATION") \
            #     .withColumnRenamed("PROVINCE", "DM_PROVINCE") \
            #     .withColumnRenamed("CITY", "DM_CITY") \
            #     .withColumnRenamed("CITY_TIER", "DM_CITY_TIER") \
            #     .withColumnRenamed("CATEGORY", "DM_CATEGORY")
            # complete_df = df.join(un, [col("PANEL_ID") == col("PHA_ID")], "left_outer")
            
            un = spark.read.parquet("s3a://ph-max-auto/2020-08-11/data_matching/refactor/data/DIMENSION/MAPPING/MAX/UNIVERS/")
            
            new_pha = df.join(un.filter(un.CATEGORY == "NEW"), [col("PANEL_ID") == col("PHA_ID")], "left_outer")
            old_pha = new_pha.filter("HOSP_NAME is null").drop(*["PHA_ID", "CATEGORY","HOSP_NAME"]) \
                .join(un.filter(un.CATEGORY == "OLD"), [un.PHA_ID == col("PANEL_ID")], "left_outer")
            
            print(old_pha.count() + new_pha.filter("HOSP_NAME is not null").count())
            
            old_pha.show()
            new_pha.filter("HOSP_NAME is not null").show()
            
            complete_df = new_pha.filter("HOSP_NAME is not null").union(old_pha)
            complete_df.filter("HOSP_NAME is null").show(truncate=False)

            return complete_df
        else:
            return df
    
    def get_df(path):
        reading = spark.read.parquet(path)
        old_columns = reading.schema.names
        new_columns = list(map(convert_upper_columns, old_columns))
        df = drop_other_columns(reduce(lambda reading, idx: reading.withColumnRenamed(old_columns[idx], new_columns[idx]), range(len(old_columns)), reading))
        df = check_hospital_name(df)
        select_str = "PANEL_ID,HOSP_NAME,PROVINCE,CITY,REGION".split(",")
        select_str.extend(completion_column(df.schema.names))
        cond = reduce(lambda x,y:x if y in x else x + [y], [[], ] + select_str)
        df = df.selectExpr(*cond)
        df.show()
        return df
    inputs = ["s3a://ph-max-auto/v0.0.1-2020-06-08/Servier/universe_base/"]
    
    li = list(map(get_df, inputs))
    print(len(li))
    # un_all = reduce(lambda x, y: x.union(y), li).distinct()
    # un_all.show()
    # print(un_all.count())
    
    return {}
