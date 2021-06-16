# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

import random
import string
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from pyspark.sql.functions import lit, udf, col, concat_ws
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
    _product_master_input = str(kwargs["product_master_input"])
    _product_map_atc_input = str(kwargs["product_map_atc_input"])
    _time = str(kwargs["time"])
    _output = str(kwargs["clean_output"]) + _time
    
    # _column_mapping = {
    #     "Manufacturer": "ORIGINAL_MNF",
    #     "生产企业": "ORIGINAL_MNF",
    #     "生产企业1": "ORIGINAL_MNF",
    #     "生产厂家": "ORIGINAL_MNF",
    #     "企业名称1": "ORIGINAL_MNF",
    #     "CORPORATION": "ORIGINAL_MNF",
    #     "Corporation": "ORIGINAL_MNF",
    #     # "company_name": "ORIGINAL_MNF",
    #     "min1": "ORIGINAL_MIN",
    #     "标准通用名": "COMMON_NAME",
    #     "通用名_标准": "COMMON_NAME",
    #     "通用名": "COMMON_NAME",
    #     "药品名称_标准": "COMMON_NAME",
    #     "S_Molecule_Name": "COMMON_NAME",
    #     "标准商品名": "PRODUCT_NAME",
    #     "商品名_标准": "PRODUCT_NAME",
    #     "S_Product_Name": "PRODUCT_NAME",
    #     "规格_标准": "SPECIFICATIONS",
    #     "标准规格": "SPECIFICATIONS",
    #     "药品规格_标准": "SPECIFICATIONS",
    #     "Specifications_std": "SPECIFICATIONS",
    #     "S_Pack": "SPECIFICATIONS",
    #     "Form_std": "DOSAGE",
    #     "S_Dosage": "DOSAGE",
    #     "剂型_标准": "DOSAGE",
    #     "标准剂型": "DOSAGE",
    #     "包装数量_标准": "PACK_NUMBER",
    #     "包装数量2": "PACK_NUMBER",
    #     "标准包装数量": "PACK_NUMBER",
    #     "Pack_Number_std": "PACK_NUMBER",
    #     "S_PackNumber": "PACK_NUMBER",
    #     "最小包装数量": "PACK_NUMBER",
    #     "标准企业": "MANUFACTURER",
    #     "标准生产企业": "MANUFACTURER",
    #     "生产企业_标准": "MANUFACTURER",
    #     "Manufacturer_std": "MANUFACTURER",
    #     "S_CORPORATION": "MANUFACTURER",
    #     "标准生产厂家": "MANUFACTURER",
    #     "packcode": "PACK_ID",
    #     "Pack_ID": "PACK_ID",
    #     "Pack_Id": "PACK_ID",
    #     "PackID": "PACK_ID",
    #     "packid": "PACK_ID",
    #     "pfc": "PACK_ID",
    #     "PFC": "PACK_ID",
    #     "最终pfc": "PACK_ID",
    # }
    
    # _column_mapping_janssen = dict({
    #     "剂型": "DOSAGE",
    #     "规格": "SPECIFICATIONS",
    #     "包装数量": "PACK_NUMBER",
    #     "company_name_std": "MANUFACTURER"
    # }, **_column_mapping)
    
    # _column_mapping_nhwa = dict({
    #     "包装数量": "PACK_NUMBER"
    # }, **_column_mapping)
    
    
    # product_map_atc_df = spark.read.csv("s3a://ph-max-auto/v0.0.1-2020-06-08//Common_files/extract_data_files/product_map_all_ATC.csv", header=True) \
    #     .filter("project == '贝达'") \
    #     .select("pfc", "通用名", 
    #     "标准商品名", "标准剂型", 
    #     "标准规格", "标准包装数量", 
    #     "标准生产企业", "ATC4_CODE", "ATC4_DESC") \
    #     .withColumnRenamed("ATC4_CODE", "ATC") \
    #     .withColumnRenamed("ATC4_DESC", "ATCDES") \
    #     .withColumn("MIN_TWO", concat_ws("|", col("通用名"), col("标准商品名"), col("标准剂型"), col("标准规格"), col("标准包装数量"), col("标准生产企业"))).distinct()
    
    
    product_master_df = spark.read.csv(_product_master_input, header=True) \
        .selectExpr("PACK_ID", "CORP_ID",
        "MOLE_NAME_CH", "MOLE_NAME_EN", 
        "PROD_NAME_CH",
        "CORP_NAME_CH", "CORP_NAME_EN",
        "MNF_NAME_CH", "MNF_NAME_EN",
        "MNF_TYPE_NAME", "MNF_TYPE_NAME_CH",
        "DOSAGE", "SPEC", "PACK",
        "NFC123", "NFC123_NAME",
        "ATC4_CODE", "ATC4_DESC"
        ).distinct() #.withColumn("MIN_ONE", concat_ws("|", col("MOLE_NAME_CH"), col("PROD_NAME_CH"), col("DOSAGE"), col("SPEC"), col("PACK"), col("MNF_NAME_CH"))).distinct()
    
    product_master_df.write.mode("overwrite").parquet(_output)
    
    # product_map_atc_null_df = product_map_atc_df.filter("pfc is null").join(product_master_df, [col("MIN_ONE") == col("MIN_TWO")], "left_outer") \
    #     .select("PACK_ID", "通用名", "标准商品名", "标准剂型", "标准规格", "标准包装数量", "标准生产企业", "ATC", "ATCDES") \
    #     .filter("PACK_ID is null") \
    #     .withColumnRenamed("通用名", "MOLE_NAME_CH") \
    #     .withColumnRenamed("标准商品名", "PROD_NAME_CH") \
    #     .withColumnRenamed("标准剂型", "DOSAGE") \
    #     .withColumnRenamed("标准规格", "SPEC") \
    #     .withColumnRenamed("标准包装数量", "PACK") \
    #     .withColumnRenamed("标准生产企业", "MNF_NAME_CH") \
    #     .withColumnRenamed("ATC", "ATC4_CODE") \
    #     .withColumnRenamed("ATCDES", "ATC4_DESC") \
    #     .withColumn("CORP_ID", lit("null")) \
    #     .withColumn("MOLE_NAME_EN", lit("null")) \
    #     .withColumn("CORP_NAME_CH", lit("null")) \
    #     .withColumn("CORP_NAME_EN", lit("null")) \
    #     .withColumn("MNF_NAME_EN", lit("null")) \
    #     .withColumn("MNF_TYPE_NAME", lit("null")) \
    #     .withColumn("MNF_TYPE_NAME_CH", lit("null")) \
    #     .withColumn("NFC123", lit("null")) \
    #     .withColumn("NFC123_NAME", lit("null")) \
    #     .selectExpr("PACK_ID", "CORP_ID",
    #         "MOLE_NAME_CH", "MOLE_NAME_EN", 
    #         "PROD_NAME_CH",
    #         "CORP_NAME_CH", "CORP_NAME_EN",
    #         "MNF_NAME_CH", "MNF_NAME_EN",
    #         "MNF_TYPE_NAME", "MNF_TYPE_NAME_CH",
    #         "DOSAGE", "SPEC", "PACK",
    #         "NFC123", "NFC123_NAME",
    #         "ATC4_CODE", "ATC4_DESC")
    
    # min_to_upper = udf(lambda x: x.upper(), StringType())
    # all_product_df = product_master_df.drop("MIN_ONE").union(product_map_atc_null_df) 
    # # \
    # #     .withColumn("MIN", concat_ws("|", col("MOLE_NAME_CH"), col("PROD_NAME_CH"), col("DOSAGE"), col("SPEC"), col("PACK"), col("MNF_NAME_CH") )) \
    # #     .withColumn("MIN", min_to_upper(col("MIN")))
    # all_product_df.persist()
    
    # def get_company_for_url(path):
    #     tmp = path[path.index(_substr_tag) + len(_substr_tag) + 1:]
    #     return tmp[:tmp.index("/")]
    
    # def get_col_mapping(col):
    #     if col in _column_mapping.keys():
    #         return col
    #     else:
    #         return ""
    
    # def get_df(path):
    #     company = get_company_for_url(path)
    #     original_product_df = spark.read.parquet(path)
    #     original_product_df.persist()
    #     cols = list(filter(lambda x: x != "", list(map(lambda col: col if col in _column_mapping.keys() else "", original_product_df.columns))))
    #     if company == "Janssen":
    #         cols = list(filter(lambda x: x != "", list(map(lambda col: col if col in _column_mapping_janssen.keys() else "", original_product_df.columns))))
    #         original_product_df = original_product_df.select([col(c).alias(_column_mapping_janssen[c]) for c in cols])
    #     elif company == "NHWA":
    #         cols = list(filter(lambda x: x != "", list(map(lambda col: col if col in _column_mapping_nhwa.keys() else "", original_product_df.columns))))
    #         original_product_df = original_product_df.select([col(c).alias(_column_mapping_nhwa[c]) for c in cols])
    #     elif company == "Qilu":
    #         cols = list(filter(lambda x: x != "" and x.lower() != "pfc", list(map(lambda col: col if col in _column_mapping.keys() else "", original_product_df.columns))))
    #         original_product_df = original_product_df.select([col(c).alias(_column_mapping[c]) for c in cols])
    #     else:
    #         original_product_df = original_product_df.select([col(c).alias(_column_mapping[c]) for c in cols])
        
    #     original_product_df = original_product_df \
    #         .withColumnRenamed("PACK_ID", "ORIGINAL_PACK_ID") \
    #         .withColumnRenamed("COMMON_NAME", "ORIGINAL_COMMON_NAME") \
    #         .withColumnRenamed("PRODUCT_NAME", "ORIGINAL_PRODUCT_NAME") \
    #         .withColumnRenamed("DOSAGE", "ORIGINAL_DOSAGE") \
    #         .withColumnRenamed("SPECIFICATIONS", "ORIGINAL_SPECIFICATIONS") \
    #         .withColumnRenamed("PACK_NUMBER", "ORIGINAL_PACK_NUMBER") \
    #         .withColumnRenamed("MANUFACTURER", "ORIGINAL_MANUFACTURER")
            
    #     select_str = ["ORIGINAL_PACK_ID", "ORIGINAL_MIN", "ORIGINAL_MNF", 
    #             "ORIGINAL_COMMON_NAME", "ORIGINAL_PRODUCT_NAME", "ORIGINAL_DOSAGE", 
    #             "ORIGINAL_SPECIFICATIONS", "ORIGINAL_PACK_NUMBER", "ORIGINAL_MANUFACTURER"]
        
    #     original_product_null_df = original_product_df.filter("ORIGINAL_PACK_ID is null").selectExpr(*select_str)
        
    #     original_product_df = original_product_df.filter("ORIGINAL_PACK_ID is not null").selectExpr(*select_str)
        
    #     # original_product_join_df = original_product_df.join(all_product_df, [col("ORIGINAL_PACK_ID") == col("PACK_ID")], "left_outer")
    #     # original_product_match_df = original_product_join_df.filter("PACK_ID is not null") \
    #     #     .selectExpr("PACK_ID", "CORP_ID", "MOLE_NAME_CH", "MOLE_NAME_EN", 
    #     #         "PROD_NAME_CH", "CORP_NAME_CH", "CORP_NAME_EN", "MNF_NAME_CH", "MNF_NAME_EN", 
    #     #         "MNF_TYPE_NAME", "MNF_TYPE_NAME_CH", "DOSAGE", "SPEC", "PACK", "NFC123", 
    #     #         "NFC123_NAME", "ATC4_CODE", "ATC4_DESC")
    #     # original_product_match_df.show()
        
        
    #     # pa = "s3a://ph-max-auto/2020-08-11/data_matching/refactor/data/MAX/TMP/RAW_DATA/2021-04-06"
    #     # raw = spark.read.parquet(pa)
    #     # print(raw.count())
        
        
        
    #     # a = original_product_df.filter("PACK_ID is null") \
    #     #     .selectExpr(*select_str).union(original_product_null_df)
    #     # a.show()
    #     # b = a.join(product_map_atc_df, [a.ORIGINAL_COMMON_NAME == product_map_atc_df.MOLE_NAME_CH], "left_outer") \
    #     #     .selectExpr("ORIGINAL_PACK_ID", "ORIGINAL_MIN", "ORIGINAL_MNF", 
    #     #         "ORIGINAL_COMMON_NAME", "ORIGINAL_PRODUCT_NAME", "ORIGINAL_DOSAGE", 
    #     #         "ORIGINAL_SPECIFICATIONS", "ORIGINAL_PACK_NUMBER", "ORIGINAL_MANUFACTURER", "ATC4_CODE", "ATC4_DESC").distinct()
    #     # b.show()
    #     # print(b.count())
        
    #     # original_product_null_df = original_product_null_df.union(original_product_df.filter("ORIGINAL_PACK_ID is null"))
    #     # original_product_df.filter("PACK_ID is null").select("ORIGINAL_COMMON_NAME").show()
    #     # all_product_df.filter("MOLE_NAME_CH == '吉非替尼'").show()
    #     # all_product_df.filter("MOLE_NAME_CH == '阿美替尼'").show() 7342102
    #     # a = original_product_df.filter("PACK_ID is null")
    #     # a.show()
    #     # all_product_df.filter("MOLE_NAME_CH == '吉非替尼' ").show()
    #     # product_map_atc_df.filter(product_map_atc_df["通用名"] =='吉非替尼').show()
    #     # product_map_atc_df.filter(product_map_atc_df["通用名"] == '阿美替尼').show()
    #     # a.join(all_product_df, [col("ORIGINAL_COMMON_NAME") == col("MOLE_NAME_CH")]).show()
    #     # print(original_product_df.count())
        
    #     # original_product_df = original_product_df \
    #     #     .withColumn("COMPANY", lit(company)) \
    #     #     .withColumn("TIME", lit(_time))
        
    #     # return original_product_df.select("ORIGINAL_MIN", "ORIGINAL_MNF", "PACK_ID", "COMMON_NAME", "PRODUCT_NAME", "SPECIFICATIONS", "DOSAGE", "PACK_NUMBER", "MANUFACTURER", "TIME", "COMPANY")
    #     return 1
    
    
    # list(map(get_df, _inputs))
    
    # df = reduce(lambda x, y: x.union(y), list(map(get_df, _inputs)))
    # df.write.mode("overwrite").parquet(_output)
    
    
    return {}
