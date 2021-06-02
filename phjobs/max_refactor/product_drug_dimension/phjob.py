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


def execute(**kwargs):
    logger = phs3logger(kwargs["job_id"], LOG_DEBUG_LEVEL)
    spark = kwargs["spark"]()
    result_path_prefix = kwargs["result_path_prefix"]
    depends_path = kwargs["depends_path"]
    
    
    _extract_product_input = kwargs["extract_product_input"]
    _version = kwargs["version"]
    _company = kwargs["company"]
    _label = kwargs["label"]
    _product_drug_output = kwargs["product_drug_output"]
    
    def general_id():
        charset = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ' + \
                  'abcdefghijklmnopqrstuvwxyz' + \
                  '0123456789-_'

        charsetLength = len(charset)

        keyLength = 3 * 5

        result = ["P"]
        for _ in range(keyLength):
            result.append(charset[random.randint(0, charsetLength - 1)])

        return "".join(result)
    
    
    gid = udf(general_id, StringType())
    
    
    extract_df = spark.read.parquet(_extract_product_input)
    
    
    def dim_path_format(categroy):
        return "/".join(_product_drug_output.replace("//", "$").split("/")[:-1]).replace("$", "//") + "/" + categroy + "/COMPANY=" + _company + "/VERSION=" + _version
    
    
    categroy_df = spark.read.parquet(dim_path_format("CATEGORY"))
    categroy_df.persist()
    
    lexicon_df = spark.read.parquet(dim_path_format("LEXICON"))
    lexicon_df.persist()
    
    manufacturer_df = spark.read.parquet(dim_path_format("MANUFACTURER"))
    manufacturer_df.persist()
    
    drug_map_df = spark.read.parquet(dim_path_format("DRUGMAP"))
    drug_map_df.persist()
    
    categroy_atc_df = categroy_df.filter("CATEGORY == 'ATC'")
    categroy_nfc_df = categroy_df.filter("CATEGORY == 'NFC'")
    
    
    
    return {}
