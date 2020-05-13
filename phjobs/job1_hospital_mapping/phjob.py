# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

import click
import numpy as np

from pyspark.sql import SparkSession
import time
from pyspark.sql.types import *
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql import functions as func


@click.command()
@click.option('--uni_path')
@click.option('--cpa_pha_mapping_path')
@click.option('--raw_data_path')
@click.option('--std_names')
@click.option('--cpa_gyc')
@click.option('--raw_data_job1_out_path')
def execute(uni_path, cpa_pha_mapping_path, raw_data_path, std_names, cpa_gyc, raw_data_job1_out_path):
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("sparkOutlier") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.instance", "2") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()

    # 1. 首次补数
    # uni_path = "/common/projects/max/Sankyo/universe_base"
    universe = spark.read.parquet(uni_path)

    # read_universe
    for col in universe.columns:
        if col in ["City_Tier", "CITYGROUP"]:
            universe = universe.withColumnRenamed(col, "City_Tier_2010")
    universe = universe.withColumnRenamed("Panel_ID", "PHA")
    universe = universe.withColumnRenamed("Hosp_name", "HOSP_NAME")

    universe = universe.withColumn("City_Tier_2010", universe["City_Tier_2010"].cast(StringType()))

    id_city = universe.select("PHA", "City", "City_Tier_2010").distinct()

    # 1.2 读取CPA与PHA的匹配关系:
    # map_cpa_pha
    # cpa_pha_mapping_path = "/common/projects/max/Sankyo/cpa_pha_mapping"
    cpa_pha_mapping = spark.read.parquet(cpa_pha_mapping_path)
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
    # raw_data_path = "/common/projects/max/Sankyo/raw_data"
    raw_data = spark.read.parquet(raw_data_path)
    raw_data = raw_data.withColumnRenamed("ID", "BI_Code") \
        .drop("Province", "City")
    raw_data = raw_data.join(cpa_pha_mapping.select("PHA_ID_x", "BI_hospital_code", 'Province', 'City'),
                             raw_data.BI_Code == cpa_pha_mapping.BI_hospital_code,
                             how="left")

    # format_raw_data
    std_names = std_names.split(', ')
    # std_names = ['PHA', 'ID', 'Year', 'Month', 'Molecule', 'Brand', 'Form','Specifications', 'Pack_Number', 'Manufacturer', 'Sales', 'Units','Province', 'City', 'Corp', 'Route']
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

    if (cpa_gyc):
        raw_data = raw_data.withColumn("ID", raw_data["ID"].cast(StringType()))
        raw_data = raw_data.withColumn("ID", func.when(func.length(raw_data.ID) < 7, func.lpad(raw_data.ID, 6, "0")).
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

    #raw_data = raw_data.select(std_names)

    raw_data.persist()

    raw_data = raw_data.join(id_city, on=["PHA", "City"], how="left")

    raw_data.show(2)
    
    raw_data_job1_out = raw_data.repartition(2)
    raw_data_job1_out.write.format("parquet")\
        .mode("overwrite").save(raw_data_job1_out_path)

    return raw_data
