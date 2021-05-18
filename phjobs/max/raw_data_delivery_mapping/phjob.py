# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

import random
import string
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from pyspark.sql.functions import lit, udf, col, concat_ws, collect_list, size
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
    
    def general_id():
        charset = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ' + \
                  'abcdefghijklmnopqrstuvwxyz' + \
                  '0123456789-_'

        charsetLength = len(charset)

        keyLength = 3 * 5

        result = ["R"]
        for _ in range(keyLength):
            result.append(charset[random.randint(0, charsetLength - 1)])

        return "".join(result)
    
    _company = str(kwargs["company"])
    _time = str(kwargs["time"])
    _input = str(kwargs["clean_input"]) + _time
    _output = str(kwargs["raw_data_output"])
    
    _id = udf(general_id, StringType())
    
    def get_dim_df(company, time, model,
        base_path = "s3a://ph-max-auto/2020-08-11/data_matching/refactor/data/MAX"):
            definite_path = "{base_path}/{model}/TIME={time}/COMPANY={company}"
            dim_path = definite_path.format(
                base_path = base_path,
                model = model,
                time = time,
                company = company
            )
            return spark.read.parquet(dim_path)

    
    match_select = ["ID AS RAW_CODE", "RAW_HOSP_NAME AS RAWDATA_HOSP_NAME", 
        "MOLECULE AS RAWDATA_MOLE_NAME", "BRAND AS RAWDATA_PRODUCT_NAME", "FORM AS RAWDATA_DOSAGE", 
        "SPECIFICATIONS AS RAWDATA_SPEC", "PACK_NUMBER AS RAWDATA_PACK", "MANUFACTURER AS RAWDATA_MANUFACTURER",
        "RAW_MAPPING_MIN", "DATE", "SALES", "UNITS", "SOURCE", "TIME", "COMPANY"]
    
    
    base_select = ["ID", "RAW_CODE", 
        "RAWDATA_HOSP_NAME", "RAWDATA_MOLE_NAME", "RAWDATA_PRODUCT_NAME", "RAWDATA_DOSAGE", "RAWDATA_SPEC", "RAWDATA_PACK", "RAWDATA_MANUFACTURER",
        "RAW_MOLE_NAME", "RAW_PRODUCT_NAME", "RAW_DOSAGE", "RAW_SPEC", "RAW_PACK", "RAW_MANUFACTURER", 
        "DATE", "SALES", "UNITS", "SOURCE", "PRODUCT_ID", "HOSPITAL_ID", "TIME", "COMPANY"]
    
    
    clean_df = spark.read.parquet(_input).filter(col("COMPANY") == _company) \
        .withColumn("RAW_MAPPING_MIN", concat_ws("|", col("BRAND"), col("FORM"), col("SPECIFICATIONS"), col("PACK_NUMBER"), col("MANUFACTURER") )) \
        .selectExpr(*match_select)
    product_rel_df = get_dim_df(_company, _time, "DIMENSION/PRODUCT_RELATIONSHIP_DIMENSION").filter("CATEGORY == 'IMS PACKID'").selectExpr("ID", "VALUE")
    product_dim_df = get_dim_df(_company, _time, "DIMENSION/PRODUCT_DIMENSION") \
        .withColumnRenamed("ID", "PRODUCT_ID") \
        .join(product_rel_df, [col("PACK_ID") == col("ID")], "left_outer").drop("ID")
    pha_cpa_gyc_mapping_df = get_dim_df(_company, _time, "DIMENSION/MAPPING/CPA_GYC_MAPPING/STANDARD").withColumnRenamed("ID", "MAPPING_ID")
    map_all_atc_df = get_dim_df("PHARBERS", _time, "DIMENSION/MAPPING/PRODUCT_MAPPING_ALL_ATC").filter(col("PROJECT") == _company).selectExpr("MIN", "MAPPING_MAMOLE_NAME", "PACK_ID", "PRODUCT_NAME", "DOSAGE", "SPEC", "PACK", "MNF_NAME", "MOLE_NAME")
    product_mapping_df = get_dim_df(_company, _time, "DIMENSION/MAPPING/PRODUCT_MAPPING").withColumnRenamed("PACK_ID", "MAPPING_PACK_ID")
    
    mapping_std_info_df = clean_df.join(product_mapping_df, [col("RAW_MAPPING_MIN") == col("MIN")], "left_outer") \
        .withColumn("RAW_MOLE_NAME", col("MOLE_NAME")) \
        .withColumn("RAW_PRODUCT_NAME", col("PRODUCT_NAME")) \
        .withColumn("RAW_DOSAGE", col("DOSAGE")) \
        .withColumn("RAW_SPEC", col("SPEC")) \
        .withColumn("RAW_PACK", col("PACK_NUMBER")) \
        .withColumn("RAW_MANUFACTURER", col("MNF_NAME")) \
        .selectExpr("MAPPING_PACK_ID AS RAW_PACK_ID", "RAW_CODE", "RAW_MOLE_NAME", "RAW_PRODUCT_NAME", 
            "RAW_DOSAGE", "RAW_SPEC", "RAW_PACK", "RAW_MANUFACTURER", 
            "RAWDATA_HOSP_NAME", "RAWDATA_MOLE_NAME", "RAWDATA_PRODUCT_NAME", "RAWDATA_DOSAGE", "RAWDATA_SPEC", "RAWDATA_PACK", "RAWDATA_MANUFACTURER",
            "DATE", "SALES", "UNITS", 
            "SOURCE", "TIME", "COMPANY") \
        .withColumn("RAW_MAPPING_MIN", concat_ws("|", col("RAW_PRODUCT_NAME"), col("RAW_DOSAGE"), col("RAW_SPEC"), col("RAW_PACK"), col("RAW_MANUFACTURER") ))
    
            
    
    mapping_packid_not_null_df =  mapping_std_info_df.filter("RAW_PACK_ID is not null")
    mapping_packid_null_df = mapping_std_info_df.filter("RAW_PACK_ID is null")
    mapping_packid_null_df = mapping_packid_null_df.join(map_all_atc_df, [col("RAW_MAPPING_MIN") == col("MIN")], "left_outer") \
        .selectExpr("PACK_ID AS RAW_PACK_ID", "RAW_CODE", "RAW_MOLE_NAME", "RAW_PRODUCT_NAME", 
            "RAW_DOSAGE", "RAW_SPEC", "RAW_PACK", "RAW_MANUFACTURER", 
            "RAWDATA_HOSP_NAME", "RAWDATA_MOLE_NAME", "RAWDATA_PRODUCT_NAME", "RAWDATA_DOSAGE", "RAWDATA_SPEC", "RAWDATA_PACK", "RAWDATA_MANUFACTURER",
            "DATE", "SALES", "UNITS", 
            "SOURCE", "TIME", "COMPANY", "RAW_MAPPING_MIN")
    mapping_std_info_df = mapping_packid_not_null_df.union(mapping_packid_null_df)
    mapping_std_info_df.persist()
    
    df = mapping_std_info_df.join(product_dim_df, [col("RAW_PACK_ID") == col("VALUE")], "left_outer").drop("VALUE") \
        .join(pha_cpa_gyc_mapping_df, [col("RAW_CODE") == col("VALUE"), col("TAG") == col("SOURCE")], "left_outer") \
        .withColumn("ID", _id()) \
        .selectExpr(*base_select)
        
    df.show()
    print(df.count())
    a = df.filter("PRODUCT_ID is null")
    a.show()
    print(a.count())
    df.repartition(2).write.partitionBy("TIME", "COMPANY").mode("append").parquet(_output)
    
    return {}
