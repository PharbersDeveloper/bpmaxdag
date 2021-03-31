# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

import random
import string
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from pyspark.sql.functions import lit, udf, col, collect_list, array_distinct, when, array, reverse
from pyspark.sql import Window
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
    
    _input = str(kwargs["input"])
    _output = str(kwargs["output"])
    
    reading = spark.read.csv(_input, header=True)
    
    df = reading \
        .withColumn("HOSP_NAME", when(col("HOSP_NAME") == "#N/A", col("ORIGINAL_HOSP_NAME")).when(col("HOSP_NAME") == "0", col("ORIGINAL_HOSP_NAME")).otherwise(col("HOSP_NAME"))) \
        .withColumn("PROVINCE", when(col("PROVINCE") == "#N/A", col("ORIGINAL_PROVINCE")).when(col("PROVINCE") == "0", col("ORIGINAL_PROVINCE")).otherwise(col("PROVINCE"))) \
        .withColumn("CITY", when(col("CITY") == "#N/A", col("ORIGINAL_CITY")).when(col("CITY") == "0", col("ORIGINAL_CITY")).otherwise(col("CITY"))) \
        .withColumn("DISTRICT", when(col("DISTRICT") == "#N/A", col("ORIGINAL_DISTRICT")).when(col("DISTRICT") == "0", col("ORIGINAL_DISTRICT")).otherwise(col("DISTRICT"))) \
        .withColumn("HOSP_LEVEL", when(col("HOSP_LEVEL") == "#N/A", col("ORIGINAL_HOSP_LEVEL")).when(col("HOSP_LEVEL") == "0", col("ORIGINAL_HOSP_LEVEL")).otherwise(col("HOSP_LEVEL"))) \
        .withColumn("SPECIALTY_CATE", when(col("SPECIALTY_CATE") == "#N/A", col("ORIGINAL_SPECIALTY_CATE")).when(col("SPECIALTY_CATE") == "0", col("ORIGINAL_SPECIALTY_CATE")).otherwise(col("SPECIALTY_CATE"))) \
        .withColumn("SPECIALTY_ADMIN", when(col("SPECIALTY_ADMIN") == "#N/A", col("ORIGINAL_SPECIALTY_ADMIN")).when(col("SPECIALTY_ADMIN") == "0", col("ORIGINAL_SPECIALTY_ADMIN")).otherwise(col("SPECIALTY_ADMIN"))) \
        .withColumn("HOSP_QUALITY", when(col("HOSP_QUALITY") == "#N/A", col("ORIGINAL_HOSP_QUALITY")).when(col("HOSP_QUALITY") == "0", col("ORIGINAL_HOSP_QUALITY")).otherwise(col("HOSP_QUALITY"))) \
        .withColumn("EMPLOYEES_NUM", when(col("EMPLOYEES_NUM") == "#N/A", lit("0")).otherwise(col("EMPLOYEES_NUM"))) \
        .withColumn("DOCTORS_NUM", when(col("DOCTORS_NUM") == "#N/A", col("ORIGINAL_DOCTORS_NUM")).when(col("DOCTORS_NUM") == "0", col("ORIGINAL_DOCTORS_NUM")).otherwise(col("DOCTORS_NUM"))) \
        .withColumn("AUTH_BED_NUM", when(col("AUTH_BED_NUM") == "#N/A", lit("0")).otherwise(col("AUTH_BED_NUM"))) \
        .withColumn("OPEN_BED_NUM", when(col("OPEN_BED_NUM") == "#N/A", lit("0")).otherwise(col("OPEN_BED_NUM"))) \
        .withColumn("AREA_MU", when(col("AREA_MU") == "#N/A", lit("0")).otherwise(col("AREA_MU"))) \
        .withColumn("AREA_SQ_M", when(col("AREA_SQ_M") == "#N/A", lit("0")).otherwise(col("AREA_SQ_M"))) \
        .withColumn("AREA_STRUCT", when(col("AREA_STRUCT") == "#N/A", lit("0")).otherwise(col("AREA_STRUCT"))) \
        .withColumn("ANNU_DIAG_TIME", when(col("ANNU_DIAG_TIME") == "#N/A", col("ORIGINAL_ANNU_DIAG_TIME")).when(col("ANNU_DIAG_TIME") == "0", col("ORIGINAL_ANNU_DIAG_TIME")).otherwise(col("ANNU_DIAG_TIME"))) \
        .withColumn("OUTP_DIAG_TIME", when(col("OUTP_DIAG_TIME") == "#N/A", col("ORIGINAL_OUTP_DIAG_TIME")).when(col("OUTP_DIAG_TIME") == "0", col("ORIGINAL_OUTP_DIAG_TIME")).otherwise(col("OUTP_DIAG_TIME"))) \
        .withColumn("REPRODUCT", when(col("REPRODUCT") == "#N/A", lit("NAN")).otherwise(col("REPRODUCT"))) \
        .groupBy("PHA_ID") \
        .agg(
                reverse(array_distinct(collect_list("MILITARY"))).alias("MILITARYS"),
                reverse(array_distinct(collect_list("DOCT_BANK"))).alias("DOCT_BANKS"),
                reverse(array_distinct(collect_list("ORIGINAL_REGION"))).alias("ORIGINAL_REGIONS"),
                reverse(array_distinct(collect_list("LOCATION"))).alias("LOCATIONS"),
                reverse(array_distinct(collect_list("AREA_LEVEL"))).alias("AREA_LEVELS"),
                reverse(array_distinct(collect_list("BID_SAMPLE"))).alias("BID_SAMPLES"),
                reverse(array_distinct(collect_list("CITY_TIER"))).alias("CITY_TIERS"),
                reverse(array_distinct(collect_list("RE_SPECIALTY"))).alias("RE_SPECIALTYS"),
                reverse(array_distinct(collect_list("SPECIALTY_DETAIL"))).alias("SPECIALTY_DETAILS"),
                reverse(array_distinct(collect_list("Est_DrugIncome_RMB"))).alias("EST_DRUGINCOME_RMBS"),
                reverse(array_distinct(collect_list("BED_NUM"))).alias("BED_NUMS"),
                reverse(array_distinct(collect_list("GENERAL_BED_NUM"))).alias("GENERAL_BED_NUMS"),
                reverse(array_distinct(collect_list("INTERNAL_BED_NUM"))).alias("INTERNAL_BED_NUMS"),
                reverse(array_distinct(collect_list("SURG_BED_NUM"))).alias("SURG_BED_NUMS"),
                reverse(array_distinct(collect_list("OPHTH_BED_NUM"))).alias("OPHTH_BED_NUMS"),
                reverse(array_distinct(collect_list("SURG_DIAG_TIME"))).alias("SURG_DIAG_TIMES"),
                reverse(array_distinct(collect_list("ADMIS_TIME"))).alias("ADMIS_TIMES"),
                reverse(array_distinct(collect_list("SURG_TIME"))).alias("SURG_TIMES"),
                reverse(array_distinct(collect_list("MED_INCOME"))).alias("MED_INCOMES"),
                reverse(array_distinct(collect_list("OUTP_INCOME"))).alias("OUTP_INCOMES"),
                reverse(array_distinct(collect_list("OUTP_TREAT_INCOME"))).alias("OUTP_TREAT_INCOMES"),
                reverse(array_distinct(collect_list("OUTP_SURG_INCOME"))).alias("OUTP_SURG_INCOMES"),
                reverse(array_distinct(collect_list("IN_HOSP_INCOME"))).alias("IN_HOSP_INCOMES"),
                reverse(array_distinct(collect_list("BED_INCOME"))).alias("BED_INCOMES"),
                reverse(array_distinct(collect_list("IN_HOSP_TREAT_INCOME"))).alias("IN_HOSP_TREAT_INCOMES"),
                reverse(array_distinct(collect_list("IN_HOSP_SURG_INCOME"))).alias("IN_HOSP_SURG_INCOMES"),
                reverse(array_distinct(collect_list("DRUG_INCOME"))).alias("DRUG_INCOMES"),
                reverse(array_distinct(collect_list("OUTP_DRUG_INCOME"))).alias("OUTP_DRUG_INCOMES"),
                reverse(array_distinct(collect_list("OUTP_WST_DRUG_INCOME"))).alias("OUTP_WST_DRUG_INCOMES"),
                reverse(array_distinct(collect_list("IN_HOSP_DRUG_INCOME"))).alias("IN_HOSP_DRUG_INCOMES"),
                reverse(array_distinct(collect_list("IN_HOSP_WST_DRUG_INCOME"))).alias("IN_HOSP_WST_DRUG_INCOMES"),
                reverse(array_distinct(collect_list("COUNTY_HOSP_WST_DRUG_INCOME"))).alias("COUNTY_HOSP_WST_DRUG_INCOMES"),
                reverse(array_distinct(collect_list("HOSP_NAME"))).alias("HOSP_NAMES"),
                reverse(array_distinct(collect_list("PROVINCE"))).alias("PROVINCES"),
                reverse(array_distinct(collect_list("CITY"))).alias("CITYS"),
                reverse(array_distinct(collect_list("DISTRICT"))).alias("DISTRICTS"),
                reverse(array_distinct(collect_list("HOSP_LEVEL"))).alias("HOSP_LEVELS"),
                reverse(array_distinct(collect_list("SPECIALTY_CATE"))).alias("SPECIALTY_CATES"),
                reverse(array_distinct(collect_list("SPECIALTY_ADMIN"))).alias("SPECIALTY_ADMINS"),
                reverse(array_distinct(collect_list("HOSP_QUALITY"))).alias("HOSP_QUALITYS"),
                reverse(array_distinct(collect_list("EMPLOYEES_NUM"))).alias("EMPLOYEES_NUMS"),
                reverse(array_distinct(collect_list("DOCTORS_NUM"))).alias("DOCTORS_NUMS"),
                reverse(array_distinct(collect_list("AUTH_BED_NUM"))).alias("AUTH_BED_NUMS"),
                reverse(array_distinct(collect_list("OPEN_BED_NUM"))).alias("OPEN_BED_NUMS"),
                reverse(array_distinct(collect_list("AREA_MU"))).alias("AREA_MUS"),
                reverse(array_distinct(collect_list("AREA_SQ_M"))).alias("AREA_SQ_MS"),
                reverse(array_distinct(collect_list("AREA_STRUCT"))).alias("AREA_STRUCTS"),
                reverse(array_distinct(collect_list("ANNU_DIAG_TIME"))).alias("ANNU_DIAG_TIMES"),
                reverse(array_distinct(collect_list("OUTP_DIAG_TIME"))).alias("OUTP_DIAG_TIMES"),
                reverse(array_distinct(collect_list("REPRODUCT"))).alias("REPRODUCTS"),
            )
    df.printSchema()
    
    
    
    get = udf(lambda x: x[0] if len(x) > 0 else "", StringType())
    
    # new_gb_df = reading.groupBy(col("PHA_ID")) \
    #     .agg(
    #         reverse(array_distinct(collect_list("HOSP_NAME"))).alias("HOSP_NAMES"),
    #         reverse(array_distinct(collect_list("HOSP_LEVEL"))).alias("HOSP_LEVELS"),
    #         reverse(array_distinct(collect_list("REGION"))).alias("REGIONS"),
    #         reverse(array_distinct(collect_list("LOCATION"))).alias("LOCATIONS"),
    #         reverse(array_distinct(collect_list("PROVINCE"))).alias("PROVINCES"),
    #         reverse(array_distinct(collect_list("CITY"))).alias("CITYS"),
    #         reverse(array_distinct(collect_list("DISTRICT"))).alias("DISTRICTS"),
    #         reverse(array_distinct(collect_list("CITY_TIER"))).alias("CITY_TIERS")
    #         ) \
    #     .withColumn("HOSP_NAME", get(col("HOSP_NAMES"))) \
    #     .withColumn("HOSP_LEVEL", get(col("HOSP_LEVELS"))) \
    #     .withColumn("REGION", get(col("REGIONS"))) \
    #     .withColumn("LOCATION", split(get(col("LOCATIONS")), ",")) \
    #     .withColumn("PROVINCE", get(col("PROVINCES"))) \
    #     .withColumn("CITY", get(col("CITYS"))) \
    #     .withColumn("DISTRICT", get(col("DISTRICTS"))) \
    #     .withColumn("CITY_TIER", get(col("CITY_TIERS"))) \
    #     .withColumn("CATEGORY", lit("NEW"))
    
    # df = new_gb_df.selectExpr("PHA_ID","HOSP_NAME", "HOSP_LEVEL", "REGION", "LOCATION", "PROVINCE", "CITY", "DISTRICT", "CITY_TIER", "CATEGORY")
    # df.repartition(1).write.mode("overwrite").parquet("s3a://ph-max-auto/2020-08-11/data_matching/refactor/data/DIMENSION/HOSPITAL_DIMENSION/")
    
    
    return {}
