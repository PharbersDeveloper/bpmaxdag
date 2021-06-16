# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

import base64
import random
import string
import json
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from pyspark.sql.functions import array, col, collect_list, lit, udf
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
    # spark = SparkSession.builder.getOrCreate()
    _version = kwargs["version"]
    _tables = json.loads(str(base64.b64decode(kwargs["tables"]), "utf-8"))
    _postgres_uri = kwargs["db_uri"]
    _postgres_user = kwargs["db_user"]
    _postgres_pass = kwargs["db_password"]

    def hump_str(str):
        rstr = str.lower()
        if "_" in rstr:
            rstr = "".join(map(lambda x: x.capitalize(), rstr.split("_")))
        return str + " as " + rstr[0].lower() + rstr[1:]

    def find_by_key(item, key):
        if item["table_name"] == key:
            return item

    def filter_by_key(li, key):
        return list(filter(lambda x: find_by_key(x, key), li))

    def lexicon():
        item = filter_by_key(_tables, "lexicon")
        if item:
            reading = spark.read.parquet(item[0]["dw_path"] + _version)
            reading.selectExpr(*list(map(hump_str, reading.columns))) \
                .write.format("jdbc") \
                .option("url", _postgres_uri) \
                .option("dbtable", item[0]["table_name"]) \
                .option("user", _postgres_user) \
                .option("password", _postgres_pass) \
                .option("driver", "org.postgresql.Driver") \
                .mode("overwrite") \
                .save()

    def product_dim():
        item = filter_by_key(_tables, "product")
        if item:
            reading = spark.read.parquet(item[0]["dw_path"] + _version) \
                .withColumn("CONTAINS", array(col("CONTAINS.MOLE_ID"), col("CONTAINS.SPEC_ID"))) \
                .withColumn("CATEGORY", array(col("ATC_ID"), col("NFC_ID")))
            reading.selectExpr(*list(map(hump_str, reading.columns))) \
                .write.format("jdbc") \
                .option("url", _postgres_uri) \
                .option("dbtable", item[0]["table_name"]) \
                .option("user", _postgres_user) \
                .option("password", _postgres_pass) \
                .option("driver", "org.postgresql.Driver") \
                .mode("overwrite") \
                .save()

    def product_relationship_dim():
        product_rel = filter_by_key(_tables, "productrelationship")
        product = filter_by_key(_tables, "product")
        if product_rel and product:
            product_df = spark.read.parquet(product[0]["dw_path"] + _version) \
                .selectExpr("ID as PROD_ID", "PACK_ID as PRODUCT_PACK_ID")
            product_rel_df = spark.read.parquet(product_rel[0]["dw_path"] + _version)

            df = product_df.groupBy("PRODUCT_PACK_ID").agg(collect_list("PROD_ID").alias("PRODUCTS")) \
                .join(product_rel_df, [col("ID") == col("PRODUCT_PACK_ID")]) \
                .selectExpr("ID", "PRODUCTS", "CATEGORY", "TYPE", "VALUE", "VERSION")
            df.selectExpr(*list(map(hump_str, df.columns))) \
                .write.format("jdbc") \
                .option("url", _postgres_uri) \
                .option("dbtable", product_rel[0]["table_name"]) \
                .option("user", _postgres_user) \
                .option("password", _postgres_pass) \
                .option("driver", "org.postgresql.Driver") \
                .mode("overwrite") \
                .save()

    def product_category_dim():
        product_cgy = filter_by_key(_tables, "productcategory")
        product = filter_by_key(_tables, "product")
        if product_cgy and product:
            product_df = spark.read.parquet(product[0]["dw_path"] + _version) \
                .selectExpr("ID as PROD_ID", "ATC_ID", "NFC_ID")
            product_cgy_df = spark.read.parquet(product_cgy[0]["dw_path"] + _version)

            atc_ids = product_df.groupBy("ATC_ID") \
                .agg(collect_list("PROD_ID").alias("PRODUCTS")) \
                .withColumnRenamed("ATC_ID", "RE_ID")

            nfc_ids = product_df.groupBy("NFC_ID") \
                .agg(collect_list("PROD_ID").alias("PRODUCTS")) \
                .withColumnRenamed("NFC_ID", "RE_ID")

            ids = atc_ids.unionAll(nfc_ids)

            df = product_cgy_df.join(ids, [col("ID") == col("RE_ID")], "left_outer") \
                .select("ID", "PRODUCTS", "CATEGORY", "TYPE", "LEVEL", "VALUE", "DESCRIPTION", "VERSION")

            df.selectExpr(*list(map(hump_str, df.columns))) \
                .write.format("jdbc") \
                .option("url", _postgres_uri) \
                .option("dbtable", product_cgy[0]["table_name"]) \
                .option("user", _postgres_user) \
                .option("password", _postgres_pass) \
                .option("driver", "org.postgresql.Driver") \
                .mode("overwrite") \
                .save()

    def mole_spec_dim():
        mole_spec = filter_by_key(_tables, "molespec")
        product = filter_by_key(_tables, "product")
        if mole_spec and product:
            product_df = spark.read.parquet(product[0]["dw_path"] + _version) \
                .withColumnRenamed("ID", "PROD_ID").withColumnRenamed("MOLE_NAME", "PROD_MOLE_NAME")
            mole_spec_df = spark.read.parquet(mole_spec[0]["dw_path"] + _version)

            mole_df = product_df.join(mole_spec_df.filter(col("TYPE") == "MOLE"),
                                      [col("ID") == col("CONTAINS.MOLE_ID")], "right_outer") \
                .groupBy("ID").agg(collect_list("PROD_ID").alias("PRODUCTS"))
            spec_df = product_df.join(mole_spec_df.filter(col("TYPE") == "SPEC"),
                                      [col("ID") == col("CONTAINS.SPEC_ID")], "right_outer") \
                .groupBy("ID").agg(collect_list("PROD_ID").alias("PRODUCTS"))

            spec_nan_row = product_df.filter(col("SPEC") == "nannan") \
                .groupBy("SPEC").agg(collect_list("PROD_ID").alias("PRODUCTS")) \
                .withColumn("ID", lit("nan")) \
                .withColumn("TYPE", lit("nan")) \
                .withColumn("MOLE_NAME", lit("nan")) \
                .withColumn("QUANTITY", lit("nan")) \
                .withColumn("UNIT", lit("nan")) \
                .select("ID", "PRODUCTS", "TYPE", "MOLE_NAME", "QUANTITY", "UNIT")

            union_df = mole_df.unionAll(spec_df).withColumnRenamed("ID", "MOLE_SPEC_ID") \
                .join(mole_spec_df, [col("ID") == col("MOLE_SPEC_ID")], "right_outer") \
                .selectExpr("ID", "PRODUCTS", "TYPE", "MOLE_NAME", "QUANTITY", "UNIT") \
                .unionAll(spec_nan_row)

            union_df.selectExpr(*list(map(hump_str, union_df.columns))) \
                .write.format("jdbc") \
                .option("url", _postgres_uri) \
                .option("dbtable", mole_spec[0]["table_name"]) \
                .option("user", _postgres_user) \
                .option("password", _postgres_pass) \
                .option("driver", "org.postgresql.Driver") \
                .mode("overwrite") \
                .save()

    def manufacturer_dim():
        manufacturer = filter_by_key(_tables, "manufacturer")
        product = filter_by_key(_tables, "product")
        if manufacturer and product:
            product_df = spark.read.parquet(product[0]["dw_path"] + _version).withColumnRenamed("MNF_ID", "PRODUCT_MNF_ID")
            mnf_df = spark.read.parquet(manufacturer[0]["dw_path"] + _version)

            join_df = mnf_df.join(product_df, [col("MNF_ID") == col("PRODUCT_MNF_ID")], "left_outer") \
                .groupBy(col("MNF_ID")).agg(collect_list("ID").alias("PRODUCTS")).withColumnRenamed("MNF_ID",
                                                                                                    "PRODUCT_MNF_ID") \
                .join(mnf_df, [col("MNF_ID") == col("PRODUCT_MNF_ID")], "left_outer")

            join_df = join_df.selectExpr("MNF_ID as  ID", "PRODUCTS", "MNF_NAME_CH", "MNF_TYPE",
                                         "MNF_TYPE_NAME", "MNF_TYPE_NAME_CH", "CORP_ID",
                                         "CORP_NAME_EN", "CORP_NAME_CH", "LOCATION", "VERSION")

            join_df.selectExpr(*list(map(hump_str, join_df.columns))) \
                .write.format("jdbc") \
                .option("url", _postgres_uri) \
                .option("dbtable", manufacturer[0]["table_name"]) \
                .option("user", _postgres_user) \
                .option("password", _postgres_pass) \
                .option("driver", "org.postgresql.Driver") \
                .mode("overwrite") \
                .save()

    lexicon()
    product_dim()
    product_relationship_dim()
    product_category_dim()
    product_category_dim()
    mole_spec_dim()
    manufacturer_dim()

    return {}
