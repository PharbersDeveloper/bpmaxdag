# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job

Job 流程
extract_base_universe -> hospital_dimension
extract_base_universe -> hospital_base_fact

"""

import random
import string
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from pyspark.sql.functions import lit, udf, col
from functools import reduce
from pyspark.sql.types import StringType


def execute(**kwargs):
    logger = phs3logger(kwargs["job_id"], LOG_DEBUG_LEVEL)
    logger.info("Start")
    spark = kwargs["spark"]()
    logger.info("Get Spark Ins")
    result_path_prefix = kwargs["result_path_prefix"]
    depends_path = kwargs["depends_path"]
    
    _base_input = kwargs["base_input"].replace(" ", "").split(",")
    _company = kwargs["company"]
    _version = kwargs["version"]
    _base_output = kwargs["base_output"] #.replace("${runid}", _run_id)
    
    # 最终选择输出字段
    _select_col = ["PANEL_ID", "HOSP_NAME", "PROVINCE", "CITY", "REGION", "EST_DRUGINCOME_RMB", "SEG", "PANEL"]
    
    def general_id():
        charset = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ' + \
                  'abcdefghijklmnopqrstuvwxyz' + \
                  '0123456789-_'

        charsetLength = len(charset)

        keyLength = 3 * 5

        result = ["RUN"]
        for _ in range(keyLength):
            result.append(charset[random.randint(0, charsetLength - 1)])

        return "".join(result)
    
    _run_id = general_id()
    
    #将奇异的城市等级单独处理，由于不能写个性化的code，暂时这样写，正确：变成策略或展示前端自行选择
    def convert_city_group_col(cols):
        if "CITYGROUP" in cols:
            _select_col.append("CITYGROUP")
        else:
            _select_col.append("CITY_TIER AS CITYGROUP")
    
    def _exec_df(path):
        reading = spark.read.parquet(path)
        original_columns = reading.columns
        upper_columns = list(map(lambda x: x.upper(), original_columns))
        convert_city_group_col(upper_columns)
        # Columns重命名转Upper()
        df = reduce(lambda reading, idx: reading.withColumnRenamed(original_columns[idx], upper_columns[idx]), range(len(original_columns)), reading)
        df.selectExpr(*_select_col) \
            .withColumn("VERSION", lit(_version)) \
            .withColumn("COMPANY", lit(_company)) \
            .write \
            .partitionBy("COMPANY", "VERSION") \
            .parquet(_base_output, "append")
            
        return True
        
        
    result = list(map(_exec_df, _base_input))
    
    return {}
