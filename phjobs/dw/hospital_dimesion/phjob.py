# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

import random
import string
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from pyspark.sql.functions import lit, udf
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
    _input = str(kwargs["input_clean_path"]) + _time + "/" + _company
    _output = str(kwargs["output_dim_path"])
    _version = str(kwargs["version"])

    spark = kwargs["spark"]()
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
        
    reading = spark.read.parquet(_input)
    df = reading \
        .withColumn("ID", gid()) \
        .withColumn("COMPANY", lit(_company)) \
        .withColumn("TIME", lit(_time)) \
        .withColumn("VERSION", lit(_version))
    df \
        .write \
        .partitionBy("TIME", "COMPANY") \
        .mode("append") \
        .parquet(_output)
    
    # old_columns = reading.schema.names
    # new_columns = list(map(convert_upper_columns, old_columns))
    # df = drop_other_columns(reduce(lambda reading, idx: reading.withColumnRenamed(old_columns[idx], new_columns[idx]), range(len(old_columns)), reading))
    # df = check_hospital_name(df)
    # select_str = "PANEL_ID,HOSP_NAME,PROVINCE,CITY,REGION".split(",")
    # select_str.extend(completion_column(df.schema.names))
    # cond = reduce(lambda x,y:x if y in x else x + [y], [[], ] + select_str)
    # df = df.selectExpr(*cond) \
    #     .withColumn("ID", gid()) \
    #     .withColumn("COMPANY", lit(_company)) \
    #     .withColumn("TIME", lit(_time)) \
    #     .withColumn("VERSION", lit(_versioin))
    # cond.insert(0, "ID")
    # cond.append("COMPANY")
    # cond.append("TIME")
    # cond.append("VERSION")
    # df = df.selectExpr(*cond)
    # df.show()
    # df \
    #     .write \
    #     .partitionBy("TIME", "COMPANY") \
    #     .mode("overwrite") \
    #     .parquet(_output)

    return {}

