# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

import base64
import random
import string
import json
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from pyspark.sql.functions import lit, udf
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession


def execute(**kwargs):
    def camel_to_underline(format_str):
        underline_format = ""
        if isinstance(format_str, str):
            for _s_ in format_str:
                underline_format += _s_ if _s_.islower() else "_" + _s_.lower()
        return format_str + " as " + underline_format.upper()


    """
        please input your code below
        get spark session: spark = kwargs["spark"]()
    """
    logger = phs3logger(kwargs["job_id"], LOG_DEBUG_LEVEL)
    logger.info("当前 owner 为 " + str(kwargs["owner"]))
    logger.info("当前 run_id 为 " + str(kwargs["run_id"]))
    logger.info("当前 job_id 为 " + str(kwargs["job_id"]))
    spark = kwargs["spark"]()

    # spark = SparkSession.builder.getOrCreate()

    _postgres_uri = kwargs["db_uri"]
    _postgres_properties = {'user': kwargs["db_user"], 'password': kwargs["db_password"]}
    _version = kwargs["version"]

    tables = json.loads(str(base64.b64decode(kwargs["db_tables"]), "utf-8"))
    for item in tables:
        reading = spark.read.jdbc(url=_postgres_uri, table=item["table_name"], properties=_postgres_properties)
        columns = reading.drop("category").drop("product").columns
        reading \
            .selectExpr(*list(map(camel_to_underline, columns))) \
            .withColumn("VERSION", lit(_version)) \
            .repartition(1) \
            .write.mode("overwrite") \
            .parquet(item["dw_path"] + _version)

    return {}

