# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
import numpy as np
import pandas as pd
from phlogs.phlogs import phlogger
from phs3.phs3 import s3
import os

from pyspark.sql import SparkSession
import time
from pyspark.sql.types import *
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql import functions as func


def execute(max_path, max_path_local, project_name, model_month_left, model_month_right, paths_foradding, test_out_path, need_test):
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
    if access_key is not None:
        spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
        spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
        spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
        spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
        # spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
        spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.cn-northwest-1.amazonaws.com.cn")

    phlogger.info('job4_panel')

    # 输入
    if project_name == "Sanofi" or project_name == "AZ":
        universe_path = max_path + "/AZ_Sanofi/universe_az_sanofi_base"
        market_path = max_path + u"/AZ_Sanofi/az_sanofi清洗_ma"
        Notarrive_unpublished_paths = paths_foradding.replace(" ","").split(",")
    else:
        universe_path = max_path + "/" + project_name + "/universe_base"
        market_path  = max_path + "/" + project_name + "/mkt_mapping"

    raw_data_adding_final_path = test_out_path + "/" + project_name + "/raw_data_adding_final"
    new_hospital_path = test_out_path + "/" + project_name + "/new_hospital"


    # 输出
    panel_path = test_out_path + "/" + project_name + "/panel_result"


    # =========== 数据检查 =============
    phlogger.info('数据检查-start')

    # 存储文件的缺失列
    misscols_dict = {}

    # universe file
    universe = spark.read.parquet(universe_path)
    colnames_universe = universe.columns
    misscols_dict.setdefault("universe", [])
    if ("Panel_ID" not in colnames_universe) and ("PHA" not in colnames_universe):
        misscols_dict["universe"].append("Panel_ID/PHA")
    if ("Hosp_name" not in colnames_universe) and ("HOSP_NAME" not in colnames_universe):
        misscols_dict["universe"].append("Hosp_name/HOSP_NAME")
    if "City" not in colnames_universe:
        misscols_dict["universe"].append("City")
    if "Province" not in colnames_universe:
        misscols_dict["universe"].append("Province")

    # market file
    market = spark.read.parquet(market_path)
    colnames_market = market.columns
    misscols_dict.setdefault("market", [])
    if ("标准通用名" not in colnames_market) and ("通用名" not in colnames_market):
        misscols_dict["market"].append("标准通用名")
    if ("model" not in colnames_market) and ("mkt" not in colnames_market):
        misscols_dict["market"].append("model")

    # raw_data_adding_final file
    raw_data_adding_final = spark.read.parquet(raw_data_adding_final_path)
    colnames_adding = raw_data_adding_final.columns
    misscols_dict.setdefault("adding_data", [])
    colnamelist = ['PHA', 'City', 'ID', 'Molecule', 'Sales', 'Units', 'Province', 'Month', 'Year', 'min2', 'S_Molecule', 'std_route', 'add_flag']
    # 'min1', 'year_month', 'Brand', 'Form', 'Specifications', 'Pack_Number', 'Manufacturer', 'Source', 'Corp', 'Route', 'Path', 'Sheet', 'BI_hospital_code', 'City_Tier_2010',
    # "标准商品名", 'S_Molecule_for_gr',
    for each in colnamelist:
        if each not in colnames_adding:
            misscols_dict["adding_data"].append(each)

    # 判断输入文件是否有缺失列
    misscols_dict_final = {}
    for eachfile in misscols_dict.keys():
        if len(misscols_dict[eachfile]) != 0:
            misscols_dict_final[eachfile] = misscols_dict[eachfile]
    # 如果有缺失列，则报错，停止运行
    if misscols_dict_final:
        phlogger.error('miss columns: %s' % (misscols_dict_final))
        raise ValueError('miss columns: %s' % (misscols_dict_final))

    phlogger.info('数据检查-Pass')

    # =========== 数据执行 =============

    phlogger.info('数据执行-start')

    # read_universe
    universe = spark.read.parquet(universe_path)
    for col in universe.columns:
        if col in ["City_Tier", "CITYGROUP"]:
            universe = universe.withColumnRenamed(col, "City_Tier_2010")
    universe = universe \
        .withColumnRenamed("Panel_ID", "PHA") \
        .withColumnRenamed("Hosp_name", "HOSP_NAME") \
        .withColumn("City_Tier_2010", universe["City_Tier_2010"].cast(StringType())) \
        .select("PHA", "HOSP_NAME", "Province", "City").distinct()
    universe.persist()

    # 读取 market
    market = spark.read.parquet(market_path)
    markets = market.withColumnRenamed("标准通用名", "通用名") \
        .withColumnRenamed("model", "mkt") \
        .select("mkt", "通用名").distinct()

    # 读取 raw_data_adding_final
    raw_data_adding_final = spark.read.parquet(raw_data_adding_final_path)
    raw_data_adding_final.persist()

    # 生成 panel
    panel = raw_data_adding_final \
        .join(markets, raw_data_adding_final["S_Molecule"] == markets["通用名"], how="left") \
        .drop("Province", "City") \
        .join(universe, on="PHA", how="left") \
        .withColumn("Date", raw_data_adding_final.Year * 100 + raw_data_adding_final.Month)

    panel = panel \
        .groupBy("ID", "Date", "min2", "mkt", "HOSP_NAME", "PHA", "S_Molecule", "Province", "City", "add_flag", "std_route") \
        .agg(func.sum("Sales").alias("Sales"), func.sum("Units").alias("Units"))

    # 对 panel 列名重新命名
    old_names = ["ID", "Date", "min2", "mkt", "HOSP_NAME", "PHA", "S_Molecule", "Province", "City", "add_flag", "std_route"]
    new_names = ["ID", "Date", "Prod_Name", "DOI", "Hosp_name", "HOSP_ID", "Molecule", "Province", "City", "add_flag", "std_route"]
    for index, name in enumerate(old_names):
        panel = panel.withColumnRenamed(name, new_names[index])

    panel = panel \
        .withColumn("Prod_CNAME", panel.Prod_Name) \
        .withColumn("Strength", panel.Prod_Name) \
        .withColumn("DOIE", panel.DOI) \
        .withColumn("Date", panel["Date"].cast(DoubleType()))

    # 拆分 panel_raw_data， panel_add_data
    panel_raw_data = panel.where(panel.add_flag == 0)
    panel_raw_data.persist()
    panel_add_data = panel.where(panel.add_flag == 1)
    panel_add_data.persist()

    original_Date_molecule = panel_raw_data.select("Date", "Molecule").distinct()
    original_Date_ProdName = panel_raw_data.select("Date", "Prod_Name").distinct()

    panel_add_data = panel_add_data \
        .join(original_Date_molecule, on=["Date", "Molecule"], how="inner") \
        .join(original_Date_ProdName, on=["Date", "Prod_Name"], how="inner")

    # new_hospital = pd.read_excel(new_hospital_path)
    new_hospital = spark.read.parquet(new_hospital_path)
    new_hospital = new_hospital.toPandas()["PHA"].tolist()

    # 生成 panel_filtered
    # 早于model所用时间（历史数据），用new_hospital补数;
    # 处于model所用时间（模型数据），不补数；
    # 晚于model所用时间（月更新数据），用unpublished和not arrived补数
    if project_name == "Sanofi" or project_name == "AZ":
        kct = [u'北京市', u'长春市', u'长沙市', u'常州市', u'成都市', u'重庆市', u'大连市', u'福厦泉市', u'广州市',
               u'贵阳市', u'杭州市', u'哈尔滨市', u'济南市', u'昆明市', u'兰州市', u'南昌市', u'南京市', u'南宁市', u'宁波市',
               u'珠三角市', u'青岛市', u'上海市', u'沈阳市', u'深圳市', u'石家庄市', u'苏州市', u'太原市', u'天津市', u'温州市',
               u'武汉市', u'乌鲁木齐市', u'无锡市', u'西安市', u'徐州市', u'郑州市', u'合肥市', u'呼和浩特市', u'福州市', u'厦门市',
               u'泉州市', u'珠海市', u'东莞市', u'佛山市', u'中山市']
        city_list = [u'北京市', u'上海市', u'天津市', u'重庆市', u'广州市', u'深圳市', u'西安市', u'大连市', u'成都市', u'厦门市', u'沈阳市']
        Province_list = [u'河北省', u'福建省', u'河北', u"福建"]

        panel_add_data = panel_add_data \
            .where(~panel_add_data.City.isin(city_list)) \
            .where(~panel_add_data.Province.isin(Province_list)) \
            .where(~(~(panel_add_data.City.isin(kct)) & (panel_add_data.Molecule == u"奥希替尼")))

        # 晚于model所用时间（月更新数据），用unpublished和not arrived补数
        for index, eachfile in enumerate(Notarrive_unpublished_paths):
            if index == 0:
                # Notarrive_unpublished = pd.read_excel(eachfile, dtype=str)
                Notarrive_unpublished = spark.read.csv(eachfile, header=True)
            else:
                # tmp_file = pd.read_excel(eachfile, dtype=str)
                tmp_file =  spark.read.csv(eachfile, header=True)
                Notarrive_unpublished = Notarrive_unpublished.union(tmp_file)

        future_range = Notarrive_unpublished.withColumn("Date", Notarrive_unpublished["Date"].cast(DoubleType()))

        panel_add_data_future = panel_add_data.where(panel_add_data.Date > int(model_month_right)) \
            .join(future_range, on=["Date", "ID"], how="inner") \
            .select(panel_raw_data.columns)

        # 早于model所用时间（历史数据），用new_hospital补数;
        panel_add_data_history = panel_add_data.where(panel_add_data.HOSP_ID.isin(new_hospital)) \
            .where(panel_add_data.Date < int(model_month_left)) \
            .select(panel_raw_data.columns)

        panel_filtered = (panel_raw_data.union(panel_add_data_history)).union(panel_add_data_future)

    else:
        city_list = [u'北京市', u'上海市', u'天津市', u'重庆市', u'广州市', u'深圳市', u'西安市', u'大连市', u'成都市', u'厦门市', u'沈阳市']
        Province_list = [u'河北省', u'福建省', u'河北', u"福建"]

        # 去除 city_list和 Province_list
        panel_add_data = panel_add_data \
            .where(~panel_add_data.City.isin(city_list)) \
            .where(~panel_add_data.Province.isin(Province_list))
        panel_add_data_history = panel_add_data \
            .where(panel_add_data.HOSP_ID.isin(new_hospital)) \
            .where(panel_add_data.Date < int(model_month_left)) \
            .select(panel_raw_data.columns)
        panel_filtered = panel_raw_data.union(panel_add_data_history)

    # panel_filtered.groupBy("add_flag").agg({"Sales": "sum"}).show()
    # panel_filtered.groupBy("add_flag").agg({"Sales": "sum"}).show()

    panel_filtered = panel_filtered.repartition(2)
    panel_filtered.write.format("parquet") \
        .mode("overwrite").save(panel_path)

    phlogger.info("输出 panel_filtered 结果：" + str(panel_path))

    phlogger.info('数据执行-Finish')

    # =========== 数据验证 =============
    # 与原R流程运行的结果比较正确性: Sanofi与Sankyo测试通过
    if int(need_test) > 0:
        phlogger.info('数据验证-start')

        my_out = spark.read.parquet(panel_path)

        if project_name == "Sanofi":
            R_out_path = "/user/ywyuan/max/Sanofi/Rout/panel-result"
        elif project_name == "AZ":
            R_out_path = "/user/ywyuan/max/AZ/Rout/panel-result"
        elif project_name == "Sankyo":
            R_out_path = "/user/ywyuan/max/Sankyo/Rout/panel-result"

        R_out = spark.read.parquet(R_out_path)

        if project_name == "AZ":
            R_out = R_out.where(R_out.Date/100 < 2020)

        # 检查内容：列缺失，列的类型，列的值
        for colname, coltype in R_out.dtypes:
            # 列是否缺失
            if colname not in my_out.columns:
                phlogger.warning ("miss columns:", colname)
            else:
                # 数据类型检查
                if my_out.select(colname).dtypes[0][1] != coltype:
                    phlogger.warning("different type columns: " + colname + ", " + my_out.select(colname).dtypes[0][1] + ", " + "right type: " + coltype)
                # 数值列的值检查
                if coltype == "double" or coltype == "int":
                    sum_my_out = my_out.groupBy().sum(colname).toPandas().iloc[0, 0]
                    sum_R = R_out.groupBy().sum(colname).toPandas().iloc[0, 0]
                    # phlogger.info(colname, sum_raw_data, sum_R)
                    if (sum_my_out - sum_R) != 0:
                        phlogger.warning("different value(sum) columns: " + colname + ", " + str(sum_my_out) + ", " + "right value: " + str(sum_R))

        phlogger.info('数据验证-Finish')


