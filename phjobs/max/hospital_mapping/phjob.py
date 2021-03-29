# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
from phcli.ph_logs.ph_logs import phs3logger
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql import functions as func


def execute(max_path, project_name, cpa_gyc, if_others, out_path, out_dir, auto_max, need_test):
    logger = phs3logger()
    
    os.environ["PYSPARK_PYTHON"] = "python3"    
    if auto_max == "False":
        raise ValueError('auto_max: False 非自动化')
    
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("data from s3") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.instance", "1") \
        .config("spark.executor.memory", "1g") \
        .config('spark.sql.codegen.wholeStage', False) \
        .getOrCreate()

    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    # access_key = "AKIAWPBDTVEAJ6CCFVCP"
    # secret_key = "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"
    if access_key is not None:
        spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
        spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
        spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
        spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
        # spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
        spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.cn-northwest-1.amazonaws.com.cn")

    logger.info('job1_hospital_mapping')

    # 输入
    if if_others != "False" and if_others != "True":
        logger.error('wrong input: if_others, False or True') 
        raise ValueError('wrong input: if_others, False or True')
        
    universe_path = max_path + "/" + project_name + "/universe_base"
    cpa_pha_mapping_path = max_path + "/" + project_name + "/cpa_pha_mapping"
    if if_others == "True":
        raw_data_path = max_path + "/" + project_name + "/" + out_dir + "/raw_data_box"
    else:
        raw_data_path = max_path + "/" + project_name + "/" + out_dir + "/raw_data"
        
    # 输出
    if if_others == "True":
        out_dir = out_dir + "/others_box/"
    hospital_mapping_out_path = out_path + "/" + project_name + "/" + out_dir  + "/hospital_mapping_out"

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
    # if ("Hosp_name" not in colnames_universe) and ("HOSP_NAME" not in colnames_universe):
    #    misscols_dict["universe"].append("Hosp_name/HOSP_NAME")
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
    misscols_dict_final = {}
    for eachfile in misscols_dict.keys():
        if len(misscols_dict[eachfile]) != 0:
            misscols_dict_final[eachfile] = misscols_dict[eachfile]
    # 如果有缺失列，则报错，停止运行
    if misscols_dict_final:
        logger.error('miss columns: %s' % (misscols_dict_final))
        raise ValueError('miss columns: %s' % (misscols_dict_final))

    logger.info('数据检查-Pass')

    # =========== 数据执行 =============
    logger.info('数据执行-start')

    # 1. 首次补数

    # read_universe
    if "CITYGROUP" in universe.columns:
        universe = universe.withColumnRenamed("CITYGROUP", "City_Tier_2010")
    elif "City_Tier" in universe.columns:
        universe = universe.withColumnRenamed("City_Tier", "City_Tier_2010")
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

    # ID 的长度统一
    def distinguish_cpa_gyc(col, gyc_hospital_id_length):
        # gyc_hospital_id_length是国药诚信医院编码长度，一般是7位数字，cpa医院编码一般是6位数字。医院编码长度可以用来区分cpa和gyc
        return (func.length(col) < gyc_hospital_id_length)
    def deal_ID_length(df):
        df = df.withColumn("ID", df["ID"].cast(StringType()))
        df = df.withColumn("ID", func.regexp_replace("ID", "\\.0", ""))
        df = df.withColumn("ID", func.when(distinguish_cpa_gyc(df.ID, 7), func.lpad(df.ID, 6, "0")).otherwise(df.ID))
        return df
    raw_data = deal_ID_length(raw_data)

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

    raw_data = raw_data.join(PHA_city_in_universe, on=["PHA", "City"], how="left")

    
    hospital_mapping_out = raw_data.repartition(2)
    hospital_mapping_out.write.format("parquet") \
        .mode("overwrite").save(hospital_mapping_out_path)

    logger.info("输出 hospital_mapping 结果：" + hospital_mapping_out_path)

    logger.info('数据执行-Finish')

    # =========== 数据验证 =============

    if int(need_test) > 0:
        logger.info('数据验证-start')

        my_out = raw_data

        if project_name == "Sanofi":
            R_out_path = "/common/projects/max/AZ_Sanofi/hospital_mapping/raw_data_with_pha"
        elif project_name == "AZ":
            R_out_path = "/common/projects/max/AZ_Sanofi/hospital_mapping/raw_data_with_pha_az"
        elif project_name == "Sankyo":
            R_out_path = "/user/ywyuan/max/Sankyo/Rout/hospital_mapping_out"

        R_out = spark.read.parquet(R_out_path)

        # 检查内容：列缺失，列的类型，列的值
        for colname, coltype in R_out.dtypes:
            # 列是否缺失
            if colname not in my_out.columns:
                logger.warning ("miss columns:", colname)
            else:
                # 数据类型检查
                if my_out.select(colname).dtypes[0][1] != coltype:
                    logger.warning("different type columns: " + colname + ", " + my_out.select(colname).dtypes[0][1] + ", " + "right type: " + coltype)

                # 数值列的值检查
                if coltype == "double" or coltype == "int":
                    sum_my_out = my_out.groupBy().sum(colname).toPandas().iloc[0, 0]
                    sum_R = R_out.groupBy().sum(colname).toPandas().iloc[0, 0]
                    # logger.info(colname, sum_raw_data, sum_R)
                    if (sum_my_out - sum_R) != 0:
                        logger.warning("different value(sum) columns: " + colname + ", " + str(sum_my_out) + ", " + "right value: " + str(sum_R))

        logger.info('数据验证-Finish')

    # =========== return =============
    return raw_data




