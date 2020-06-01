# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
import numpy as np
import pandas as pd
import logging

from pyspark.sql import SparkSession
import time
from pyspark.sql.types import *
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql import functions as func


def execute(a, b):
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("sparkOutlier") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.instance", "2") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()
        
    project_name = "Sankyo"
        
    # logging配置
    logger = logging.getLogger("log")
    logger.setLevel(level=logging.INFO)
    file_handler = logging.FileHandler('job4_panel_' + project_name +'.log','w')
    file_handler.setLevel(level=logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - [line:%(lineno)d] - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)	
    
    # 输入
    project_name = "Sankyo"
    uni_path = "/common/projects/max/Sankyo/universe_base"
    mkt_path = "/common/projects/max/Sankyo/mkt_mapping"
    add_data_path = "/user/ywyuan/max/Sankyo/raw_data_adding_final"
    new_hospital_path = "/workspace/BP_Max_AutoJob/Sankyo/2019new_hospital.xlsx"
    panel_path = "/user/ywyuan/max/Sankyo/panel_result"
    model_month_l = 201901
    # 输出
    panel_path = "/user/ywyuan/max/Sankyo/panel_result"
    
    '''
    # 输入
    project_name = "Sanofi"
    uni_path = "/common/projects/max/AZ_Sanofi/universe_az_sanofi_base"
    mkt_path = "/common/projects/max/AZ_Sanofi/az_sanofi清洗_ma"
    add_data_path = "/user/ywyuan/max/Sanofi/raw_data_adding_final"
    new_hospital_path = "/workspace/BP_Max_AutoJob/Sanofi/2019new_hospital.xlsx"
    Notarrive_and_unpublished_paths=["/workspace/BP_Max_AutoJob/Sanofi/Not_arrived202001.xlsx",
                                         "/workspace/BP_Max_AutoJob/Sanofi/No_arrived201912.xlsx",
                                         "/workspace/BP_Max_AutoJob/Sanofi/Unpublished2020.xlsx"]
    model_month_l = 201901
    panel_path = "/user/ywyuan/max/Sanofi/panel_result"
    '''
    # =========== 数据执行 =============
    logger.info('数据执行-start')
    
    # read_universe
    uni = spark.read.parquet(uni_path)
    for col in uni.columns:
        if col in ["City_Tier", "CITYGROUP"]:
            uni = uni.withColumnRenamed(col, "City_Tier_2010")
    uni = uni.withColumnRenamed("Panel_ID", "PHA") \
        .withColumnRenamed("Hosp_name", "HOSP_NAME")
    uni = uni.withColumn("City_Tier_2010", uni["City_Tier_2010"].cast(StringType()))
    uni = uni.select("PHA", "HOSP_NAME", "Province", "City").distinct()
    uni.persist()
    
    mkt = spark.read.parquet(mkt_path)
    mkts = mkt.withColumnRenamed("标准通用名", "通用名") \
        .withColumnRenamed("model", "mkt") \
        .select("mkt", "通用名").distinct()
    
    add_data = spark.read.parquet(add_data_path)
    add_data = add_data.persist()
    panel = add_data.join(mkts, add_data.S_Molecule == mkts["通用名"], how="left") \
        .drop("Province", "City") \
        .join(uni, on="PHA", how="left") \
        .withColumn("Date", add_data.Year * 100 + add_data.Month)
    
    panel = panel.groupBy("ID", "Date", "min2", "mkt", "HOSP_NAME", "PHA", "S_Molecule", "Province", "City", "add_flag", "std_route") \
            .agg(func.sum("Sales").alias("Sales"), func.sum("Units").alias("Units"))
    
    old_names = ["ID", "Date", "min2", "mkt", "HOSP_NAME", "PHA", "S_Molecule", "Province","City", "add_flag", "std_route"]
    new_names = ["ID", "Date", "Prod_Name", "DOI", "Hosp_name", "HOSP_ID", "Molecule", "Province", "City", "add_flag", "std_route"]
    for index, name in enumerate(old_names):
        panel = panel.withColumnRenamed(name, new_names[index])
    
    panel = panel.withColumn("Prod_CNAME", panel.Prod_Name) \
        .withColumn("Strength", panel.Prod_Name) \
        .withColumn("DOIE", panel.DOI) \
        .withColumn("Date", panel["Date"].cast(DoubleType()))
        
    
    #panel.repartition(2).write.format("parquet") \
    #    .mode("overwrite").save("/user/yyw/max/Sanofi/panel_tmp")
    
    panel_raw_data = panel.where(panel.add_flag == 0)
    panel_add_data = panel.where(panel.add_flag == 1)
    
    original_ym_molecule = panel_raw_data.select("Date", "Molecule").distinct()
    original_ym_min2 = panel_raw_data.select("Date", "Prod_Name").distinct()
    
    panel_add_data = panel_add_data \
        .join(original_ym_molecule, on=["Date", "Molecule"], how="inner") \
        .join(original_ym_min2, on=["Date", "Prod_Name"], how="inner")
    
    new_hospital = pd.read_excel(new_hospital_path)
    new_hospital = new_hospital["PHA"].tolist()
    
    print new_hospital
    
    # 早于model所用时间（历史数据），用new_hospital补数;
    # 处于model所用时间（模型数据），不补数；
    # 晚于model所用时间（月更新数据），用unpublished和not arrived补数
    
    # 获得 panel_filtered
    if project_name == "Sanofi" or project_name == "AZ":
        kct = [u'北京市', u'长春市', u'长沙市', u'常州市', u'成都市', u'重庆市', u'大连市', u'福厦泉市', u'广州市',
            u'贵阳市', u'杭州市', u'哈尔滨市', u'济南市', u'昆明市', u'兰州市', u'南昌市', u'南京市', u'南宁市', u'宁波市',
            u'珠三角市', u'青岛市', u'上海市', u'沈阳市', u'深圳市', u'石家庄市', u'苏州市', u'太原市', u'天津市', u'温州市',
            u'武汉市', u'乌鲁木齐市', u'无锡市', u'西安市', u'徐州市', u'郑州市', u'合肥市', u'呼和浩特市', u'福州市', u'厦门市',
            u'泉州市', u'珠海市', u'东莞市', u'佛山市', u'中山市']
        city_list = [u'北京市', u'上海市', u'天津市', u'重庆市', u'广州市', u'深圳市', u'西安市', u'大连市', u'成都市', u'厦门市', u'沈阳市']
        Province_list = [u'河北省', u'福建省']
    
        panel_add_data = panel_add_data.where(~panel_add_data.City.isin(city_list)) \
            .where(~panel_add_data.Province.isin(Province_list)) \
            .where(~(~(panel_add_data.City.isin(kct)) & (panel_add_data.Molecule == u"奥希替尼")))
    
        for index, eachfile in enumerate(Notarrive_and_unpublished_paths):
            if index == 0:
                Notarrive_and_unpublished = pd.read_excel(eachfile, dtype=str)
            else:
                tmp_file = pd.read_excel(eachfile, dtype=str)
                Notarrive_and_unpublished = Notarrive_and_unpublished.append(tmp_file)
    
        future_range = spark.createDataFrame(Notarrive_and_unpublished,
                                             schema=StructType([StructField("ID", StringType(), True),
                                                                StructField("Date", StringType(), True)]))
        future_range = future_range.withColumn("Date", future_range["Date"].cast(DoubleType()))
    
        # 早于model所用时间（历史数据），用new_hospital补数;
        panel_add_data_his = panel_add_data.where(panel_add_data.HOSP_ID.isin(new_hospital)) \
            .where(panel_add_data.Date < 201901) \
            .select(panel_raw_data.columns)
        # 晚于model所用时间（月更新数据），用unpublished和not arrived补数
        panel_add_data_fut = panel_add_data.where(panel_add_data.Date > 201911) \
            .join(future_range, on=["Date", "ID"], how="inner") \
            .select(panel_raw_data.columns)
    
        panel_filtered = (panel_raw_data.union(panel_add_data_his)).union(panel_add_data_fut)
    
    else:
        city_list = [u'北京市', u'上海市', u'天津市', u'重庆市', u'广州市', u'深圳市', u'西安市', u'大连市', u'成都市', u'厦门市', u'沈阳市']
        Province_list = [u'河北省', u'福建省']
    
        # 去除 city_list和 Province_list
        panel_add_data = panel_add_data.where(~panel_add_data.City.isin(city_list)) \
            .where(~panel_add_data.Province.isin(Province_list))
    
        panel_add_data_his = panel_add_data.where(panel_add_data.HOSP_ID.isin(new_hospital))\
            .where(panel_add_data.Date < int(model_month_l)) \
            .select(panel_raw_data.columns)
        panel_filtered = panel_raw_data.union(panel_add_data_his)
        
    #panel_filtered.groupBy("add_flag").agg({"Sales": "sum"}).show()
    #panel_filtered.groupBy("add_flag").agg({"Sales": "sum"}).show()
    
    panel_filtered = panel_filtered.repartition(2)
    panel_filtered.write.format("parquet") \
        .mode("overwrite").save(panel_path)
     
    logger.info('数据执行-Finish')
        
    # =========== 数据验证 =============
    # 与原R流程运行的结果比较正确性: Sanofi与Sankyo测试通过
    if True:
        logger.info('数据验证-start')
        
        my_out = spark.read.parquet(panel_path)
        
        if project_name == "Sanofi" or project_name == "AZ":
            R_out_path = "/user/ywyuan/max/Sanofi/Rout/panel-result"
        elif project_name == "Sankyo":
            R_out_path = "/user/ywyuan/max/Sankyo/Rout/panel-result"
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
    

    