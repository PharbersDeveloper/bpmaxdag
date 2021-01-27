# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
import time
from pyspark.sql.functions import regexp_extract , regexp_replace, upper ,concat_ws

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
  

    df_chc = spark.read.parquet(kwargs['path_chc'])
    df_standard = spark.read.parquet(kwargs['path_standard'])
    df_chc.show()
    spec_valid_regex = r'(\d{0,}[.]{0,1}\d+[MU]{0,1}G|\d{0,}[.]{0,1}\d+[ITM]U[G]{0,1}|\d{0,}[.]{0,1}\d+(AXAIU)|\d{0,}[.]{0,1}\d+(AXAU)|\d{0,}[.]{0,1}\d+(TIU)|\d{0,}[.]{0,1}\d+[Y])'
    spec_gross_regex =  r'(\d{0,}[.]{0,1}\d+[M]{0,1}L|\d{0,}[.]{0,1}\d+[ITM]U[G]{0,1}|\d{0,}[.]{0,1}\d+[CM]M)'
    spec_third_regex = r'([0-9]\d*\.?\d*\s*[A-Za-z]*/?\s*[A-Za-z]+)[ /:∶+\s]([0-9]\d*\.?\d*\s*[A-Za-z]*/?\s*[A-Za-z]+)[ /:∶+\s]([0-9]\d*\.?\d*\s*[A-Za-z]*/?\s*[A-Za-z]+)'
#     spec_valid_regex = r'([0-9]\d*\.?\d*\s*[A-Za-z]*/?\s*[A-Za-z]+)'
#     spec_gross_regex = r'([0-9]\d*\.?\d*\s*[A-Za-z]*/?\s*[A-Za-z]+)[ ,/:∶+\s][\u4e00-\u9fa5]*([0-9]\d*\.?\d*\s*[A-Za-z]*/?\s*[A-Za-z]+)'
    digit_regex_spec = r'(\d{0,}[.]{0,1}\d+)'
    df_chc_spec = df_chc.select("SPEC").distinct()
    print(df_chc_spec.count())
    df_chc_spec = df_chc_spec.withColumn("SPEC", upper(df_chc_spec.SPEC)) \
                            .withColumn("SPEC", regexp_replace("SPEC", r"(万)", "T")) \
                            .withColumn("SPEC", regexp_replace("SPEC", r"(μ)", "U")) \
                            .withColumn("SPEC", regexp_replace("SPEC", r"(ΜG)", "MG")) \
                            .replace(" ", "")\
                            .withColumn("SPEC_percent", regexp_extract('SPEC', r'(\d{1,3}[.]{0,1}\d+%)', 1))\
                            .withColumn("SPEC_valid", regexp_extract('SPEC', spec_valid_regex, 1))\
                            .withColumn("SPEC_valid_digit", regexp_extract('SPEC_valid', digit_regex_spec, 1))\
                            .withColumn("SPEC_valid_unit", regexp_replace('SPEC_valid', digit_regex_spec, ""))\
                            .withColumn("SPEC_gross", regexp_extract('SPEC', spec_gross_regex,1))\
                            .withColumn("SPEC_gross_digit", regexp_extract('SPEC_gross', digit_regex_spec, 1))\
                            .withColumn("SPEC_gross_unit", regexp_replace('SPEC_gross', digit_regex_spec, ""))\
                            .withColumn("SPEC_third", regexp_extract('SPEC', spec_third_regex, 3))
                        
    df_chc_spec.show(100)
    df_standard.show()
    print(df_standard.columns)
    
    return {}
