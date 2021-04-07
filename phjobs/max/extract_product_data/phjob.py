# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

import random
import string
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from pyspark.sql.functions import lit, udf, col, array_contains
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
    
    _substr_tag = "v0.0.1-2020-06-08"
    _inputs = str(kwargs["product_inputs"]).replace(" ", "").split(",")
    _output = str(kwargs["clean_output"])
    
    _column_mapping = {
        "标准通用名": "COMMON_NAME",
        "通用名_标准": "COMMON_NAME",
        "通用名": "COMMON_NAME",
        "药品名称_标准": "COMMON_NAME",
        "S_Molecule_Name": "COMMON_NAME",
        "标准商品名": "PRODUCT_NAME",
        "商品名_标准": "PRODUCT_NAME",
        "S_Product_Name": "PRODUCT_NAME",
        "product_name_std": "PRODUCT_NAME",
        "规格_标准": "SPECIFICATIONS",
        "标准规格": "SPECIFICATIONS",
        "药品规格_标准": "SPECIFICATIONS",
        "Specifications_std": "SPECIFICATIONS",
        "S_Pack": "SPECIFICATIONS",
        "Form_std": "DOSAGE",
        "S_Dosage": "DOSAGE",
        "剂型_标准": "DOSAGE",
        "标准剂型": "DOSAGE",
        "包装数量_标准": "PACK_NUMBER",
        "标准包装数量": "PACK_NUMBER",
        "Pack_Number_std": "PACK_NUMBER",
        "S_PackNumber": "PACK_NUMBER",
        "最小包装数量": "PACK_NUMBER",
        "标准企业": "MANUFACTURER",
        "生产企业_标准": "MANUFACTURER",
        "Manufacturer_std": "MANUFACTURER",
        "S_CORPORATION": "MANUFACTURER",
        "标准生产厂家": "MANUFACTURER",
        "packcode": "PACK_ID",
        "Pack_ID": "PACK_ID",
        "Pack_Id": "PACK_ID",
        "PackID": "PACK_ID",
        "packid": "PACK_ID",
    }
    
    
    
    def get_company_for_url(path):
        tmp = path[path.index(_substr_tag) + len(_substr_tag) + 1:]
        return tmp[:tmp.index("/")]
    
    def get_df(path):
        company = get_company_for_url(path)
        original_product_df = spark.read.parquet(path)
        # original_product_df.show()
        # original_product_df.printSchema()
        return original_product_df.columns
    
    # list(map(get_df, _inputs))
    
    # tmp = reduce(lambda x, y: x + y, list(map(get_df, _inputs)))
    # print(list(set(tmp)))
    
    df = spark.read.parquet("s3a://ph-max-auto/v0.0.1-2020-06-08/奥鸿/202012/prod_mapping")
    cols = list(filter(lambda x: x != "", list(map(lambda x: x if x in _column_mapping.keys() else "", df.columns))))
    print(cols)
    # df = df.select("标准通用名")
    # df.select([col(c).alias(_column_mapping[c]) for c in df.columns]).show()
    
    return {}
