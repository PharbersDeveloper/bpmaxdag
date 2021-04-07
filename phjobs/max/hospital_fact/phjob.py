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

    _time = str(kwargs["time"])
    _company = str(kwargs["company"])
    _input = str(kwargs["input_dim_path"]) + "TIME=" + _time + "/COMPANY=" + _company
    _hospital_univers_input = str(kwargs["hospital_univers"])
    _output = str(kwargs["output_fact_path"])
    _version = str(kwargs["version"])

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

    spark = kwargs["spark"]()
    dim = spark.read.parquet(_input)
    hosp_mapping = spark.read.parquet(_hospital_univers_input)
    fact_mapping = [
        {
            "CATEGORY": "EMPLOYEES",
            "TAG": "TOTAL",
            "COLUMN": "EMPLOYEES_NUM"
        },
        {
            "CATEGORY": "EMPLOYEES",
            "TAG": "DOCTOR",
            "COLUMN": "DOCTORS_NUM"
        },
        {
            "CATEGORY": "PATIENT",
            "TAG": "IN_RESIDENCE",
            "COLUMN": "ADMIS_TIME"
        },
        {
            "CATEGORY": "PATIENT",
            "TAG": "ANNUALLY",
            "COLUMN": "ANNU_DIAG_TIME"
        },
        {
            "CATEGORY": "PATIENT",
            "TAG": "PHYSICIAN",
            "COLUMN": "INTERNAL_DIAG_TIME"
        },
        {
            "CATEGORY": "PATIENT",
            "TAG": "OPD",
            "COLUMN": "OUTP_DIAG_TIME"
        },
        {
            "CATEGORY": "PATIENT",
            "TAG": "SURGERY",
            "COLUMN": "SURG_DIAG_TIME"
        },
        {
            "CATEGORY": "PATIENT",
            "TAG": "SURGERY_IN_RESIDENCE",
            "COLUMN": "SURG_TIME"
        },
        {
            "CATEGORY": "AREASIZE",
            "TAG": "GROSS",
            "COLUMN": "AREA_SQ_M"
        },
        {
            "CATEGORY": "AREASIZE",
            "TAG": "GROSS_CH",
            "COLUMN": "AREA_MU"
        },
        {
            "CATEGORY": "AREASIZE",
            "TAG": "COVER",
            "COLUMN": "AREA_STRUCT"
        },
        {
            "CATEGORY": "BEDCAPACITY",
            "TAG": "TOTAL",
            "COLUMN": "BED_NUM"
        },
        {
            "CATEGORY": "BEDCAPACITY",
            "TAG": "COMPILING",
            "COLUMN": "AUTH_BED_NUM"
        },
        {
            "CATEGORY": "BEDCAPACITY",
            "TAG": "GENERAL",
            "COLUMN": "GENERAL_BED_NUM"
        },
        {
            "CATEGORY": "BEDCAPACITY",
            "TAG": "PHYSICIAN",
            "COLUMN": "INTERNAL_BED_NUM"
        },
        {
            "CATEGORY": "BEDCAPACITY",
            "TAG": "OPENNESS",
            "COLUMN": "OPEN_BED_NUM"
        },
        {
            "CATEGORY": "BEDCAPACITY",
            "TAG": "OPHTHOALMIC",
            "COLUMN": "OPHTH_BED_NUM"
        },
        {
            "CATEGORY": "REVENUE",
            "TAG": "BED_IN_RESIDENCE",
            "COLUMN": "BED_INCOME"
        },
        {
            "CATEGORY": "REVENUE",
            "TAG": "MEDICINE_RMB",
            "COLUMN": "DRUGINCOME_RMB"
        },
        {
            "CATEGORY": "REVENUE",
            "TAG": "MEDICINE",
            "COLUMN": "DRUG_INCOME"
        },
        {
            "CATEGORY": "REVENUE",
            "TAG": "MEDICINE_IN_RESIDENCE",
            "COLUMN": "IN_HOSP_DRUG_INCOME"
        },
        {
            "CATEGORY": "REVENUE",
            "TAG": "IN_RESIDENCE",
            "COLUMN": "IN_HOSP_INCOME"
        },
        {
            "CATEGORY": "REVENUE",
            "TAG": "SURGERY_IN_RESIDENCE",
            "COLUMN": "IN_HOSP_SURG_INCOME"
        },
        {
            "CATEGORY": "REVENUE",
            "TAG": "TREATMENT_IN_RESIDENCE",
            "COLUMN": "IN_HOSP_TREAT_INCOME"
        },
        {
            "CATEGORY": "REVENUE",
            "TAG": "WST_MEDICINE_IN_RESIDENCE",
            "COLUMN": "IN_HOSP_WST_DRUG_INCOME"
        },
        {
            "CATEGORY": "REVENUE",
            "TAG": "MEDICINE_OPT",
            "COLUMN": "OUTP_DRUG_INCOME"
        },
        {
            "CATEGORY": "REVENUE",
            "TAG": "OPT",
            "COLUMN": "OUTP_INCOME"
        },
        {
            "CATEGORY": "REVENUE",
            "TAG": "SURGERY_OPT",
            "COLUMN": "OUTP_SURG_INCOME"
        },
        {
            "CATEGORY": "REVENUE",
            "TAG": "TREATMENT_OPT",
            "COLUMN": "OUTP_TREAT_INCOME"
        },
        {
            "CATEGORY": "REVENUE",
            "TAG": "WST_MEDICINE_OPT",
            "COLUMN": "OUTP_WST_DRUG_INCOME"
        },
        {
            "CATEGORY": "REVENUE",
            "TAG": "TREATMENT",
            "COLUMN": "MED_INCOME"
        },
        {
            "CATEGORY": "REVENUE",
            "TAG": "MEDICINE_DISTRICT",
            "COLUMN": "COUNTY_HOSP_WST_DRUG_INCOME"
        },
        {
            "CATEGORY": "IS",
            "TAG": "MILITARY",
            "COLUMN": "MILITARY"
        },
        {
            "CATEGORY": "IS",
            "TAG": "EPILEPSY",
            "COLUMN": "SPECIALTY_ADMIN"
        },
        {
            "CATEGORY": "IS",
            "TAG": "DOCTOR_LIB",
            "COLUMN": "DOCT_BANK"
        },
        {
            "CATEGORY": "IS",
            "TAG": "REPRODUCT",
            "COLUMN": "REPRODUCT"
        },
        {
            "CATEGORY": "IS",
            "TAG": "BID_SAMPLE",
            "COLUMN": "BID_SAMPLE"
        },
        {
            "CATEGORY": "TYPE",
            "TAG": "AREA_LEVEL",
            "COLUMN": "AREA_LEVEL"
        },
        {
            "CATEGORY": "TYPE",
            "TAG": "LEVEL",
            "COLUMN": "HOSP_LEVEL"
        },
        {
            "CATEGORY": "TYPE",
            "TAG": "NATURE",
            "COLUMN": "HOSP_QUALITY"
        },
        {
            "CATEGORY": "TYPE",
            "TAG": "SPECIALTY_CATE",
            "COLUMN": "SPECIALTY_CATE"
        },
        {
            "CATEGORY": "TYPE",
            "TAG": "SPECIALTY_ADMIN",
            "COLUMN": "SPECIALTY_ADMIN"
        },
        {
            "CATEGORY": "TYPE",
            "TAG": "RE_SPECIALTY",
            "COLUMN": "RE_SPECIALTY"
        },
        {
            "CATEGORY": "TYPE",
            "TAG": "SPECIALTY_DETAIL",
            "COLUMN": "SPECIALTY_DETAIL"
        },
    ]

    def fact_table(item):
        fact = dim.selectExpr("ID as HOSPITAL_ID", "PANEL_ID") \
            .join(hosp_mapping, [col("PANEL_ID") == col("PHA_ID")], "left_outer") \
            .selectExpr("HOSPITAL_ID", "PANEL_ID", "PHA_ID", item["COLUMN"]) \
            .withColumn("ID", gid()) \
            .withColumn("CATEGORY", lit(item["CATEGORY"])) \
            .withColumn("TAG", lit(item["TAG"])) \
            .withColumn("VALUE", col(item["COLUMN"])) \
            .withColumn("COMPANY", lit(_company)) \
            .withColumn("TIME", lit(_time)) \
            .withColumn("VERSION", lit(_version)) \
            .drop(item["COLUMN"])
        return fact

    fact_un_all = reduce(lambda x, y: x.union(y), list(map(fact_table, fact_mapping)))
    fact_un_all \
        .write \
        .partitionBy("TIME", "COMPANY") \
        .mode("append") \
        .parquet(_output)
    return {}
