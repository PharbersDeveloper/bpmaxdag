# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job

Job 流程

extract_base_universe -> hospital_base_fact （只包含SEG与PANEL）

"""

import random
import string
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from pyspark.sql.functions import lit, udf, col, array
from functools import reduce
from pyspark.sql.types import StringType


def execute(**kwargs):
    logger = phs3logger(kwargs["job_id"], LOG_DEBUG_LEVEL)
    logger.info("Start")
    spark = kwargs["spark"]()
    logger.info("Get Spark Ins")
    result_path_prefix = kwargs["result_path_prefix"]
    depends_path = kwargs["depends_path"]
    
    _hosp_dim_input = kwargs["hosp_dim_input"]
    _original_clean_input = kwargs["original_clean_input"]
    _company = kwargs["company"]
    _version = kwargs["version"]
    _hosp_base_fact_output = kwargs["hosp_base_fact_output"]
    
    
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
    
    return {}
