# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

import random
import string
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from pyspark.sql.functions import lit, udf, col
from pyspark import StorageLevel
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
    _company = str(kwargs["company"])
    _input = str(kwargs["inpit_dim_path"]) + "TIME=" + _time + "/COMPANY=" + _company
    _other_seg_paths = str(kwargs["other_seg_paths"]).replace(" ", "").split(",")
    _version = str(kwargs["version"])
    
    _output = str(kwargs["output_fact_path"])
    _output_mapping_seg_path = str(kwargs["output_mapping_seg_path"])
    
    
    fact_mapping = [
        {
            "CATEGORY": "OTHER",
            "TAG": "universe_onc".upper(),
            "COLUMN": "SEG",
        },
        {
            "CATEGORY": "OTHER",
            "TAG": "universe_onc".upper(),
            "COLUMN": "PANEL",
        },
    ]
    
    def general_id():
        charset = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ' + \
                  'abcdefghijklmnopqrstuvwxyz' + \
                  '0123456789-_'

        charsetLength = len(charset)

        keyLength = 3 * 5

        result = ["H"]
        for _ in range(keyLength):
            result.append(charset[random.randint(0, charsetLength - 1)])

        return "".join(result)
    
    
    gid = udf(general_id, StringType())
    
    
    dim_df = spark.read.parquet(_input)
    dim_df.persist()
    
    
    def get_seg_mapping(path):
        tag = path.split("/")[-1]
        return spark.read.parquet(path) \
            .selectExpr("Panel_ID AS PHA_ID", "Seg AS SEG", "PANEL") \
            .withColumn("MARKET", lit(tag.upper())) \
            .withColumn("SOURCE", lit("OTHER"))
    
    
    mapping_seg_dfs = reduce(lambda dfl, dfr: dfl.union(dfr), list(map(get_seg_mapping, _other_seg_paths)))

    
    def fact_table(item):
        return dim_df.selectExpr("ID as HOSPITAL_ID", "PANEL_ID") \
            .join(mapping_seg_dfs.filter(col("MARKET") == item["TAG"]), [col("PANEL_ID") == col("PHA_ID")]) \
            .selectExpr("HOSPITAL_ID", "PANEL_ID", "PHA_ID", item["COLUMN"], "MARKET AS MARKET_TEMP") \
            .withColumn("ID", gid()) \
            .withColumn("CATEGORY", lit(item["CATEGORY"])) \
            .withColumn("TAG", lit(item["COLUMN"])) \
            .withColumn("VALUE", col(item["COLUMN"]).cast(StringType())) \
            .withColumn("COMPANY", lit(_company)) \
            .withColumn("TIME", lit(_time)) \
            .withColumn("VERSION", lit(_version)) \
            .drop(item["COLUMN"])
    
    
    
    fact_df = reduce(lambda dfl, dfr: dfl.union(dfr), list(map(fact_table, fact_mapping)))
    fact_df.persist()
    logger.info("Fact Table Count ===> " + str(fact_df.count()))
    
    
    hospital_other_seg_mapping_df = fact_df.selectExpr("ID AS HOSPITAL_FACT_ID", "PHA_ID", "MARKET_TEMP AS TAG", "CATEGORY", "TIME", "COMPANY", "VERSION")
    logger.info("Hospital Other Seg Mapping DF Count ===> " + str(hospital_other_seg_mapping_df.count()))
    
    
    fact_df.drop("MARKET_TEMP") \
        .selectExpr("ID", "HOSPITAL_ID", "PANEL_ID", "PHA_ID", "CATEGORY", "TAG", "VALUE", "COMPANY", "TIME", "VERSION") \
        .write \
        .partitionBy("TIME", "COMPANY") \
        .parquet(_output, "append")
    
    hospital_other_seg_mapping_df \
        .write \
        .partitionBy("TIME", "COMPANY") \
        .mode("append") \
        .parquet(_output_mapping_seg_path)
    
    return {}
