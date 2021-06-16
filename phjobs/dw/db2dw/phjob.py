# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

import base64
import random
import string
import json
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from pyspark.sql.functions import col, create_map, lit, udf
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

    def replace_null(value):
        if value is None:
            return "nan"
        else:
            return value

    rn = udf(replace_null, StringType())

    tables = json.loads(str(base64.b64decode(kwargs["db_tables"]), "utf-8"))
    for item in tables:
        if item["table_name"] == "product":
            product = spark.read.jdbc(url=_postgres_uri, table=item["table_name"], properties=_postgres_properties)
            molespec = spark.read.jdbc(url=_postgres_uri, table="molespec", properties=_postgres_properties)

            mole_df = molespec.filter(col("type") == "MOLE").selectExpr("id as mole_id", "type as  mole_type",
                                                                        "moleName as mn")
            spec_df = molespec.filter(col("type") == "SPEC").selectExpr("id as spec_id", "type as spec_type",
                                                                        "quantity", "unit")

            df = product.join(mole_df, [col("contains")[0] == col("mole_id")], "left_outer") \
                .join(spec_df, [col("contains")[1] == col("spec_id")], "left_outer") \
                .select("id", "moleName", "prodDesc", "prodNameCh", "pack", "pckDesc", "dosage",
                        create_map(lit('MOLE_ID'), col("mole_id"),
                                   lit('MOLE_NAME'), col("mn"),
                                   lit("SPEC_ID"), rn(col("spec_id")),
                                   lit("QUANTITY"), rn(col("quantity")),
                                   lit("UNIT"), rn(col("unit"))).alias("contains"), "spec", "mnfId", "packId", "atcId",
                        "nfcId", "events", "version")

            df.selectExpr(*list(map(camel_to_underline, df.columns))) \
                .withColumn("VERSION", lit(_version)) \
                .repartition(1) \
                .write.mode("overwrite") \
                .parquet(item["dw_path"] + _version)
        else:
            reading = spark.read.jdbc(url=_postgres_uri, table=item["table_name"], properties=_postgres_properties)
            df = reading.drop("products")
            if item["table_name"] == "manufacturer":
                df = df.withColumnRenamed("ID", "mnfId")
                logger.info(df.columns)

            df \
                .selectExpr(*list(map(camel_to_underline, df.columns))) \
                .withColumn("VERSION", lit(_version)) \
                .repartition(1) \
                .write.mode("overwrite") \
                .parquet(item["dw_path"] + _version)

    return {}
