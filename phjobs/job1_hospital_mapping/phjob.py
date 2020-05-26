# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

import numpy as np
import logging

from pyspark.sql import SparkSession
import time
from pyspark.sql.types import *
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql import functions as func


def execute(max_path, project_name, cpa_gyc, test_out_path):
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("sparkOutlier") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.instance", "2") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()

    # logging配置
    logger = logging.getLogger("log")
    logger.setLevel(level=logging.INFO)
    file_handler = logging.FileHandler('job1_hospital_mapping.log','w')
    file_handler.setLevel(level=logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - [line:%(lineno)d] - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.info('job1_hospital_mapping')

    # 输入输出
    universe_path = max_path + "/" + project_name + "/universe_base"
    cpa_pha_mapping_path = max_path + "/" + project_name + "/cpa_pha_mapping"
    raw_data_path = max_path + "/" + project_name + "/raw_data"
    hospital_mapping_out_path = test_out_path + "/" + project_name + "/hospital_mapping_out"

    # =========== 数据检查 =============
    logger.info('数据检查-start')

    # 存储文件的缺失列
    misscols_dict = {}

    # universe file
    # universe_path = "/common/projects/max/Sankyo/universe_base"
    universe = spark.read.parquet(universe_path)
    colnames_universe = universe.columns
    misscols_dict.setdefault("universe", [])
    if ("City_Tier" not in colnames_universe) and ("CITYGROUP" not in colnames_universe) and ("City_Tier_2010" not in colnames_universe):
        misscols_dict["universe"].append("City_Tier/CITYGROUP")
    if ("Panel_ID" not in colnames_universe) and ("PHA" not in colnames_universe):
        misscols_dict["universe"].append("Panel_ID/PHA")
    if ("Hosp_name" not in colnames_universe) and ("HOSP_NAME" not in colnames_universe):
        misscols_dict["universe"].append("Hosp_name/HOSP_NAME")
    if "City" not in colnames_universe:
        misscols_dict["universe"].append("City")
    if "Province" not in colnames_universe:
        misscols_dict["universe"].append("Province")

    # cpa_pha_mapping file
    # cpa_pha_mapping_path = "/common/projects/max/Sankyo/cpa_pha_mapping"
    cpa_pha_mapping = spark.read.parquet(cpa_pha_mapping_path)
    colnames_map = cpa_pha_mapping.columns
    misscols_dict.setdefault("cpa_pha_mapping", [])
    if "推荐版本" not in colnames_map:
        misscols_dict["cpa_pha_mapping"].append("推荐版本")
    if ("ID" not in colnames_map) and ("BI_hospital_code" not in colnames_map):
        misscols_dict["cpa_pha_mapping"].append("ID/BI_hospital_code")
    if ("PHA" not in colnames_map) and ("PHA_ID_x" not in colnames_map):
        misscols_dict["cpa_pha_mapping"].append("PHA")

    # raw_data file
    # raw_data_path = "/common/projects/max/Sankyo/raw_data"
    raw_data = spark.read.parquet(raw_data_path)
    colnames_raw_data = raw_data.columns
    misscols_dict.setdefault("raw_data", [])
    if ("Units" not in colnames_raw_data) and ("数量（支/片）" not in colnames_raw_data) and ("最小制剂单位数量" not in colnames_raw_data) and ("total_units" not in colnames_raw_data) and ("SALES_QTY" not in colnames_raw_data):
        misscols_dict["raw_data"].append("about Units")
    if ("Sales" not in colnames_raw_data) and ("金额（元）" not in colnames_raw_data) and ("金额" not in colnames_raw_data) and ("sales_value__rmb_" not in colnames_raw_data) and ("SALES_VALUE" not in colnames_raw_data):
        misscols_dict["raw_data"].append("about Sales")
    if ("year_month" not in colnames_raw_data) and ("Yearmonth" not in colnames_raw_data) and ("YM" not in colnames_raw_data) and ("Date" not in colnames_raw_data):
        misscols_dict["raw_data"].append("about year_month")
    if "ID" not in colnames_raw_data:
        misscols_dict["raw_data"].append("ID")
    if ("Molecule" not in colnames_raw_data) and ("通用名" not in colnames_raw_data) and ("药品名称" not in colnames_raw_data) and ("molecule_name" not in colnames_raw_data) and ("MOLE_NAME" not in colnames_raw_data):
        misscols_dict["raw_data"].append("about Molecule")
    if ("Brand" not in colnames_raw_data) and ("商品名" not in colnames_raw_data) and ("药品商品名" not in colnames_raw_data) and ("product_name" not in colnames_raw_data) and ("PRODUCT_NAME" not in colnames_raw_data):
        misscols_dict["raw_data"].append("about Brand")
    if ("Specifications" not in colnames_raw_data) and ("规格" not in colnames_raw_data) and ("pack_description" not in colnames_raw_data) and ("SPEC" not in colnames_raw_data):
        misscols_dict["raw_data"].append("about Specifications")
    if ("Form" not in colnames_raw_data) and ("剂型" not in colnames_raw_data) and ("formulation_name" not in colnames_raw_data) and ("DOSAGE" not in colnames_raw_data):
        misscols_dict["raw_data"].append("about Form")
    if ("Manufacturer" not in colnames_raw_data) and ("生产企业" not in colnames_raw_data) and ("company_name" not in colnames_raw_data) and ("MANUFACTURER_NAME" not in colnames_raw_data):
        misscols_dict["raw_data"].append("about Manufacturer")

    # 判断输入文件是否有缺失列
    for eachfile in misscols_dict.keys():
        if len(misscols_dict[eachfile]) == 0:
            del misscols_dict[eachfile]
    # 如果有缺失列，则报错，停止运行
    if misscols_dict:
        logger.error('miss columns: %s' % (misscols_dict))
        raise ValueError('miss columns: %s' % (misscols_dict))

    logger.info('数据检查-Pass')

    # =========== 数据执行 =============
    logger.info('数据执行-start')

    # 1. 首次补数

    # read_universe
    for col in universe.columns:
        if col in ["City_Tier", "CITYGROUP"]:
            universe = universe.withColumnRenamed(col, "City_Tier_2010")
    universe = universe.withColumnRenamed("Panel_ID", "PHA") \
        .withColumnRenamed("Hosp_name", "HOSP_NAME")

    universe = universe.withColumn("City_Tier_2010", universe["City_Tier_2010"].cast(StringType()))

    PHA_city_in_universe = universe.select("PHA", "City", "City_Tier_2010").distinct()

    # 1.2 读取CPA与PHA的匹配关系:
    # map_cpa_pha
    cpa_pha_mapping = cpa_pha_mapping.filter(cpa_pha_mapping["推荐版本"] == 1) \
        .withColumnRenamed("ID", "BI_hospital_code") \
        .withColumnRenamed("PHA", "PHA_ID_x") \
        .select("PHA_ID_x", "BI_hospital_code").distinct()
    cpa_pha_mapping = cpa_pha_mapping.join(universe.select("PHA", "Province", "City"),
                                           cpa_pha_mapping.PHA_ID_x == universe.PHA,
                                           how="left") \
        .drop("PHA")

    # 1.3 读取原始样本数据:
    # read_raw_data
    raw_data = raw_data.withColumnRenamed("ID", "BI_Code") \
        .drop("Province", "City")
    raw_data = raw_data.join(cpa_pha_mapping.select("PHA_ID_x", "BI_hospital_code", 'Province', 'City'),
                             raw_data.BI_Code == cpa_pha_mapping.BI_hospital_code,
                             how="left")

    # format_raw_data
    # cpa_gyc = True
    for col in raw_data.columns:
        if col in ["数量（支/片）", "最小制剂单位数量", "total_units", "SALES_QTY"]:
            raw_data = raw_data.withColumnRenamed(col, "Units")
        if col in ["金额（元）", "金额", "sales_value__rmb_", "SALES_VALUE"]:
            raw_data = raw_data.withColumnRenamed(col, "Sales")
        if col in ["Yearmonth", "YM", "Date"]:
            raw_data = raw_data.withColumnRenamed(col, "year_month")
        if col in ["年", "年份", "YEAR"]:
            raw_data = raw_data.withColumnRenamed(col, "Year")
        if col in ["月", "月份", "MONTH"]:
            raw_data = raw_data.withColumnRenamed(col, "Month")
        if col in ["医院编码", "BI_Code", "HOSP_CODE"]:
            raw_data = raw_data.withColumnRenamed(col, "ID")
        if col in ["通用名", "药品名称", "molecule_name", "MOLE_NAME"]:
            raw_data = raw_data.withColumnRenamed(col, "Molecule")
        if col in ["商品名", "药品商品名", "product_name", "PRODUCT_NAME"]:
            raw_data = raw_data.withColumnRenamed(col, "Brand")
        if col in ["规格", "pack_description", "SPEC"]:
            raw_data = raw_data.withColumnRenamed(col, "Specifications")
        if col in ["剂型", "formulation_name", "DOSAGE"]:
            raw_data = raw_data.withColumnRenamed(col, "Form")
        if col in ["包装数量", "包装规格", "PACK_QTY"]:
            raw_data = raw_data.withColumnRenamed(col, "Pack_Number")
        if col in ["生产企业", "company_name", "MANUFACTURER_NAME"]:
            raw_data = raw_data.withColumnRenamed(col, "Manufacturer")
        if col in ["省份", "省", "省/自治区/直辖市", "province_name", "PROVINCE_NAME"]:
            raw_data = raw_data.withColumnRenamed(col, "Province")
        if col in ["城市", "city_name", "CITY_NAME"]:
            raw_data = raw_data.withColumnRenamed(col, "City")
        if col in ["PHA_ID_x", "PHA_ID"]:
            raw_data = raw_data.withColumnRenamed(col, "PHA")

    if (cpa_gyc == "True"):
        def distinguish_cpa_gyc(col, gyc_hospital_id_length):
            # gyc_hospital_id_length是国药诚信医院编码长度，一般是7位数字，cpa医院编码一般是6位数字。医院编码长度可以用来区分cpa和gyc
            return (func.length(col) < gyc_hospital_id_length)

        raw_data = raw_data.withColumn("ID", raw_data["ID"].cast(StringType()))
        raw_data = raw_data.withColumn("ID", func.when(distinguish_cpa_gyc(raw_data.ID, 7), func.lpad(raw_data.ID, 6, "0")).
                                       otherwise(func.lpad(raw_data.ID, 7, "0")))
    if "year_month" in raw_data.columns:
        raw_data = raw_data.withColumn("year_month", raw_data["year_month"].cast(IntegerType()))
    if "Month" not in raw_data.columns:
        raw_data = raw_data.withColumn("Month", raw_data.year_month % 100)
    if "Pack_Number" not in raw_data.columns:
        raw_data = raw_data.withColumn("Pack_Number", func.lit(0))
    if "Year" not in raw_data.columns:
        raw_data = raw_data.withColumn("Year", (raw_data.year_month - raw_data.Month) / 100)

    raw_data = raw_data.withColumn("Year", raw_data["Year"].cast(IntegerType())) \
        .withColumn("Month", raw_data["Month"].cast(IntegerType()))

    raw_data.persist()

    raw_data = raw_data.join(PHA_city_in_universe, on=["PHA", "City"], how="left")

    # 与原R流程运行的结果比较正确性
    hospital_mapping_out = raw_data.repartition(2)
    hospital_mapping_out.write.format("parquet") \
        .mode("overwrite").save(hospital_mapping_out_path)

    raw_data.show(2)

    logger.info('数据执行-Finish')

    # =========== 数据验证 =============

    if True:
        logger.info('数据验证-start')

        R_hospital_mapping_out_path = "/user/ywyuan/max/Sankyo/Rout/hospital_mapping_out"
        R_hospital_mapping_out = spark.read.parquet(R_hospital_mapping_out_path)

        # 检查内容：列的类型，列的值
        for colname, coltype in raw_data.dtypes:
            # 数据类型检查
                # print ("different type columns:", colname, coltype, "right type: " + R_product_mapping_out.select(colname).dtypes[0][1])
            if R_hospital_mapping_out.select(colname).dtypes[0][1] != coltype:
                logger.warning ("different type columns: "  + colname + ", " + coltype + ", " + "right type: " + R_product_mapping_out.select(colname).dtypes[0][1])

            # 数值列的值检查
            if coltype == "double" or coltype == "int":
                # year_month, Pack_Number, Sales, Units, Units_Box, BI_hospital_code, Month, Year
                sum_raw_data = raw_data.groupBy().sum(colname).toPandas().iloc[0,0]
                sum_R = R_hospital_mapping_out.groupBy().sum(colname).toPandas().iloc[0,0]
                # print (colname, sum_raw_data, sum_R)
                if (sum_raw_data - sum_R) != 0:
                    logger.warning ("different value(sum) columns: " + colname + ", " + str(sum_raw_data) + ", " + "right value: " + str(sum_R))

        logger.info('数据验证-Finish')

    return raw_data




