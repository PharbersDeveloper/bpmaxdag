# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job

Job流程
master_packid_by_common -> product_category_dimension
master_packid_by_common -> product_lexicon_dimension
master_packid_by_common -> product_manufacturer_dimension
master_packid_by_common -> product_drug_map_dimension
master_packid_by_common -> product_drug_dimension（必须是最后一个，最后一个基于上几个job的结果）
"""

import random
import string
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from pyspark.sql.functions import lit, udf, col
from functools import reduce
from pyspark.sql.types import StringType


def execute(**kwargs):
    logger = phs3logger(kwargs["job_id"], LOG_DEBUG_LEVEL)
    spark = kwargs["spark"]()
    result_path_prefix = kwargs["result_path_prefix"]
    depends_path = kwargs["depends_path"]
    
    _master_packid_input = kwargs["master_packid_input"]
    _version = kwargs["master_packid_input"]
    _company = kwargs["company"]
    _label = kwargs["label"]
    _master_packid_output = kwargs["master_packid_output"]
    
    df = spark.read.csv(_master_packid_input, header=True) \
        .selectExpr("PACK_ID", "MOLE_NAME_EN", "MOLE_NAME_CH", "PROD_NAME_CH", 
            "CORP_NAME_EN", "CORP_NAME_CH", "MNF_NAME_EN", "MNF_NAME_CH",
            "MNF_TYPE_NAME", "MNF_TYPE_NAME_CH", "PCK_DESC",
            "PACK", "DOSAGE", "SPEC", "CORP_ID", "MNF_ID", 
            "NFC123", "NFC123_NAME", "ATC4_CODE", "ATC4_DESC")
    
    df.write \
        .parquet(_master_packid_output, "overwrite")
    
    return {}
