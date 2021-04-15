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
    _input = str(kwargs["input_dim_path"]) + "TIME=" + _time + "/COMPANY=" + _company
    _hospital_seg_panel_input = str(kwargs["seg_panel_paths"]).replace(" ", "").split(",")
    _output = str(kwargs["output_fact_path"])
    _version = str(kwargs["version"])
    
    fact_mapping = [
        {
            "CATEGORY": "CPA",
            "TAG": "SEG",
            "COLUMN": "SEG",
        },
        {
            "CATEGORY": "CPA",
            "TAG": "PANEL",
            "COLUMN": "PANEL",
        },
        {
            "CATEGORY": "GYC",
            "TAG": "SEG",
            "COLUMN": "SEG",
        },
        {
            "CATEGORY": "GYC",
            "TAG": "PANEL",
            "COLUMN": "PANEL",
        },
        {
            "CATEGORY": "CPAGY",
            "TAG": "SEG",
            "COLUMN": "SEG",
        },
        {
            "CATEGORY": "CPAGY",
            "TAG": "PANEL",
            "COLUMN": "PANEL",
        }
    ]
    
    
    val_mapping = {
        "肿瘤": "ONC",
        "传染": "INF",
        "普通": "BASE",
        "精神": "NER"
    }
    
    
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
    
    
    def convert_col_value(val):
        if val in val_mapping.keys():
            return val_mapping[val]
        else:
            return val
            
    
    gid = udf(general_id, StringType())
    convert_col = udf(convert_col_value, StringType())
    
    # dim_df = spark.read.parquet(_input)
    dim_df = spark.read.parquet("s3a://ph-max-auto/2020-08-11/data_matching/refactor/data/MAX/DIMENSION/HOSPITAL_DIMENSION/TIME=2021-04-06/COMPANY=贝达/")
    dim_df.persist()
    
    def seg_panel_mapping(path):
        key = path.split("/")[-1].replace(".csv", "").replace("universe_", "")
        source = key.split("_")[0]
        market = val_mapping[key.split("_")[-1]]
        return spark.read.csv(path, header=True) \
            .selectExpr("Panel_ID AS PHA_ID", "PANEL", "Market AS MARKET", "Seg AS SEG") \
            .withColumn("MARKET", convert_col(col("MARKET"))) \
            .withColumn("SOURCE", lit(source))
            
            
    mapping_seg_panel_df = reduce(lambda dfl, dfr: dfl.union(dfr), list(map(seg_panel_mapping, _hospital_seg_panel_input)))
    
    
    def fact_table(item):
        return dim_df.selectExpr("ID as HOSPITAL_ID", "PANEL_ID") \
            .join(mapping_seg_panel_df.filter(col("SOURCE") == item["CATEGORY"]), [col("PANEL_ID") == col("PHA_ID")]) \
            .selectExpr("HOSPITAL_ID", "PANEL_ID", "PHA_ID", item["COLUMN"], "MARKET AS MARKET_TEMP") \
            .withColumn("ID", gid()) \
            .withColumn("CATEGORY", lit(item["CATEGORY"])) \
            .withColumn("TAG", lit(item["TAG"])) \
            .withColumn("VALUE", col(item["COLUMN"])) \
            .withColumn("COMPANY", lit(_company)) \
            .withColumn("TIME", lit(_time)) \
            .withColumn("VERSION", lit(_version)) \
            .drop(item["COLUMN"])
    
     
    fact_df = reduce(lambda x, y: x.union(y), list(map(fact_table, fact_mapping)))
    # fact_df.persist()
    # fact_df.filter("TAG == 'SEG' and CATEGORY == 'CPA' and PHA_ID == 'PHA0000037'").show()
    # print(fact_df.count())
    
    
    hospital_market_mapping_df = fact_df.selectExpr("ID AS HOSPITAL_FACT_ID", "PHA_ID", "MARKET_TEMP AS TAG", "CATEGORY")
    
    
    # fact_df.drop("COMPANY").drop("TIME").drop("MARKET_TEMP").createOrReplaceTempView("hospital_fact")
    # dim_df.createOrReplaceTempView("hospital_dimesion")
    # hospital_market_mapping_df.createOrReplaceTempView("hospital_market_mapping")
    
    
    # spark.sql("""
    #     SELECT  PHA, HOSPITAL_ID, HOSP_NAME, 
    #             PROVINCE, CITY, CITYGROUP AS CITY_TIER, 
    #             REGION, PANEL, SEG
    #     FROM (
    #         SELECT 
    #             hfct.PHA_ID AS PHA, hfct.HOSPITAL_ID, hdim.HOSP_NAME, 
    #             hdim.PROVINCE, hdim.CITY, hdim.CITYGROUP, 
    #             hdim.REGION, hfct.TAG, hfct.VALUE 
    #         FROM hospital_dimesion AS hdim 
    #             INNER JOIN hospital_fact AS hfct ON hdim.ID == hfct.HOSPITAL_ID
    #             INNER JOIN hospital_market_mapping AS hmm ON hfct.ID == hmm.HOSPITAL_FACT_ID
    #         WHERE (hmm.CATEGORY = 'CPA' AND hmm.TAG = 'INF')
    #             AND (hfct.CATEGORY = 'CPA' AND hfct.TAG = 'SEG') 
    #             OR (hfct.CATEGORY = 'CPA' AND hfct.TAG = 'PANEL')
    #     )
    #     PIVOT (
    #         SUM(VALUE)
    #         FOR TAG in ('SEG', 'PANEL')
    #     )
    # """).filter("PHA == 'PHA0000037'").show()
    
    return {}
