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
        
    """
        please input your code below
        get spark session: spark = kwargs["spark"]()
    """
    logger = phs3logger(kwargs["job_id"], LOG_DEBUG_LEVEL)
    logger.info("当前 owner 为 " + str(kwargs["owner"]))
    logger.info("当前 run_id 为 " + str(kwargs["run_id"]))
    logger.info("当前 job_id 为 " + str(kwargs["job_id"]))
    spark = kwargs["spark"]()
    
    company = "奥鸿"
    date = "2021-04-01"
    # tp = "dim"
    
    _input = kwargs["input"]
    _output = kwargs["output"]

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
    # inputs = ["s3a://ph-max-auto/v0.0.1-2020-06-08/Servier/universe_base/"]
    
    # compony = "Servier"
    
    
    # li = list(map(get_df, inputs))
    # print(len(li))
    
    
    # un_all = reduce(lambda x, y: x.union(y), li).distinct()
    # un_all.show()
    # print(un_all.count())
    
    gid = udf(general_id, StringType())
    
    reading = spark.read.parquet(_input)
    old_columns = reading.schema.names
    new_columns = list(map(convert_upper_columns, old_columns))
    df = drop_other_columns(reduce(lambda reading, idx: reading.withColumnRenamed(old_columns[idx], new_columns[idx]), range(len(old_columns)), reading))
    df = check_hospital_name(df)
    select_str = "PANEL_ID,HOSP_NAME,PROVINCE,CITY,REGION".split(",")
    select_str.extend(completion_column(df.schema.names))
    cond = reduce(lambda x,y:x if y in x else x + [y], [[], ] + select_str)
    df = df.selectExpr(*cond) \
        .withColumn("ID", gid()) \
        .withColumn("COMPANY", lit(company)) \
        .withColumn("TIME", lit(date)) \
        .withColumn("VERSION", lit("0.0.1"))
    cond.insert(0, "ID")
    cond.append("COMPANY")
    cond.append("TIME")
    cond.append("VERSION")
    df = df.selectExpr(*cond)
    df.repartition(3) \
        .write \
        .partitionBy("TIME", "COMPANY") \
        .mode("overwrite") \
        .parquet(_output)
    
    
    
    # 生成Fact Table
    dim = spark.read.parquet(_output + "/" + "TIME=" + date + "/" + "COMPANY=" + company)
    hosp_mapping = spark.read.parquet("s3a://ph-max-auto/2020-08-11/data_matching/refactor/data/DIMENSION/MAPPING/MAX/HOSPITAL_UNIVERS/")
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
        }
    ]
    
    def fact_table(item):
        fact = dim.selectExpr("ID as HOSPITAL_ID", "PANEL_ID") \
            .join(hosp_mapping, [col("PANEL_ID") == col("PHA_ID")], "left_outer") \
            .selectExpr("HOSPITAL_ID", "PANEL_ID", "PHA_ID", item["COLUMN"]) \
            .withColumn("CATEGORY", lit(item["CATEGORY"])) \
            .withColumn("TAG", lit(item["TAG"])) \
            .withColumn("VALUE", col(item["COLUMN"])) \
            .withColumn("COMPANY", lit(company)) \
            .withColumn("TIME", lit(date)) \
            .withColumn("VERSION", lit("0.0.1")) \
            .drop(item["COLUMN"])
        return fact
        
    
    fact_un_all = reduce(lambda x, y: x.union(y), list(map(fact_table, fact_mapping)))
    fact_un_all.show()
    print(fact_un_all.count())
    
    fact_un_all.repartition(3) \
        .write \
        .partitionBy("TIME", "COMPANY") \
        .mode("overwrite") \
        .parquet("s3a://ph-max-auto/2020-08-11/data_matching/refactor/data/FACT/HOSPITAL_FACT")
    
    
    return {}
