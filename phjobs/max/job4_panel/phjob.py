# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL


def execute(**kwargs):
    logger = phs3logger(kwargs["job_id"], LOG_DEBUG_LEVEL)
    spark = kwargs['spark']()
    result_path_prefix = kwargs["result_path_prefix"]
    depends_path = kwargs["depends_path"]
    
    ### input args ###
    max_path = kwargs['max_path']
    project_name = kwargs['project_name']
    model_month_left = kwargs['model_month_left']
    model_month_right = kwargs['model_month_right']
    if_others = kwargs['if_others']
    current_year = kwargs['current_year']
    current_month = kwargs['current_month']
    paths_foradding = kwargs['paths_foradding']
    not_arrived_path = kwargs['not_arrived_path']
    published_path = kwargs['published_path']
    panel_for_union = kwargs['panel_for_union']
    monthly_update = kwargs['monthly_update']
    out_path = kwargs['out_path']
    out_dir = kwargs['out_dir']
    need_test = kwargs['need_test']
    add_47 = kwargs['add_47']
    time_left = kwargs['time_left']
    time_right = kwargs['time_right']
    ### input args ###
    
    ### output args ###
    a = kwargs['a']
    b = kwargs['b']
    ### output args ###

    from pyspark.sql import SparkSession, Window
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    import os
    import pandas as pd
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col    # %%
    # project_name = '京新'
    # out_dir = 'test'
    # monthly_update = "True"
    # model_month_left = "201901"
    # model_month_right = "201912"
    # all_models = "康复新液,益生菌,癫痫,他汀,帕金森,癫痫新分子"
    # universe_choice = "他汀:universe_他汀"
    # need_cleaning_cols = "Molecule, Brand, Form, Specifications, Pack_Number, Manufacturer, min1"
    # time_left = "202001"
    # time_right = "202004"
    # first_month = "1"
    # current_month = "4"
    # %%
    logger.debug('job4_panel')
    
    if if_others == "True":
        out_dir = out_dir + "/others_box/"
    
    out_path_dir = out_path + "/" + project_name + '/' + out_dir
    # 输入
    universe_path = max_path + "/" + project_name + "/universe_base"
    market_path  = max_path + "/" + project_name + "/mkt_mapping"
    raw_data_adding_final_path = out_path_dir + "/raw_data_adding_final"
    new_hospital_path = out_path_dir  + "/new_hospital"
    if panel_for_union != "Empty":
        panel_for_union_path = out_path + "/" + project_name + '/' + panel_for_union
    else:
        panel_for_union_path = "Empty"
    
    if add_47 != "False" and add_47 != "True":
        logger.error('wrong input: add_47, False or True') 
        raise ValueError('wrong input: add_47, False or True')
        
    # 月更新相关输入
    if monthly_update != "False" and monthly_update != "True":
        logger.error('wrong input: monthly_update, False or True') 
        raise ValueError('wrong input: monthly_update, False or True')
    if monthly_update == "False":
        Notarrive_unpublished_paths = paths_foradding.replace(", ",",").split(",")
    elif monthly_update == "True":
        current_year = int(current_year)
        current_month = int(current_month)
        if not_arrived_path == "Empty":    
            not_arrived_path = max_path + "/Common_files/Not_arrived" + str(current_year*100 + current_month) + ".csv"
            
        if published_path == "Empty":
            published_right_path = max_path + "/Common_files/Published" + str(current_year) + ".csv"
            published_left_path = max_path + "/Common_files/Published" + str(current_year - 1) + ".csv"
        else:
            published_path  = published_path.replace(" ","").split(",")
            published_left_path = published_path[0]
            published_right_path = published_path[1]
        
    # 输出
    if if_others == "True":
        panel_path = out_path_dir + "/panel_result_box"
    else:
        panel_path = out_path_dir + "/panel_result"

    # %%
    # =========== 数据检查 =============
    logger.debug('数据检查-start')
    # 存储文件的缺失列
    misscols_dict = {}
    # universe file
    universe = spark.read.parquet(universe_path)
    colnames_universe = universe.columns
    misscols_dict.setdefault("universe", [])
    if ("Panel_ID" not in colnames_universe) and ("PHA" not in colnames_universe):
        misscols_dict["universe"].append("Panel_ID/PHA")
    # if ("Hosp_name" not in colnames_universe) and ("HOSP_NAME" not in colnames_universe):
    #     misscols_dict["universe"].append("Hosp_name/HOSP_NAME")
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
    colnamelist = ['PHA', 'City', 'ID', 'Molecule', 'Sales', 'Units', 'Province', 'Month', 'Year', 'min2', 'S_Molecule', 'add_flag']
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
        logger.debug('miss columns: %s' % (misscols_dict_final))
        raise ValueError('miss columns: %s' % (misscols_dict_final))
    logger.debug('数据检查-Pass')

    # %%
    # =========== 数据执行 =============
    logger.debug('数据执行-start')
    # read_universe
    universe = spark.read.parquet(universe_path)
    if "CITYGROUP" in universe.columns:
        universe = universe.withColumnRenamed("CITYGROUP", "City_Tier_2010")
    elif "City_Tier" in universe.columns:
        universe = universe.withColumnRenamed("City_Tier", "City_Tier_2010")
    universe = universe \
        .withColumnRenamed("Panel_ID", "PHA") \
        .withColumnRenamed("Hosp_name", "HOSP_NAME") \
        .withColumn("City_Tier_2010", universe["City_Tier_2010"].cast(StringType()))
    if "HOSP_NAME" not in universe.columns:
        universe = universe.withColumn("HOSP_NAME",func.lit("0"))
    universe = universe.select("PHA", "HOSP_NAME", "Province", "City").distinct()
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
        .groupBy("ID", "Date", "min2", "mkt", "HOSP_NAME", "PHA", "S_Molecule", "Province", "City", "add_flag") \
        .agg(func.sum("Sales").alias("Sales"), func.sum("Units").alias("Units"))
    # 对 panel 列名重新命名
    old_names = ["ID", "Date", "min2", "mkt", "HOSP_NAME", "PHA", "S_Molecule", "Province", "City", "add_flag"]
    new_names = ["ID", "Date", "Prod_Name", "DOI", "Hosp_name", "HOSP_ID", "Molecule", "Province", "City", "add_flag"]
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
    if monthly_update == "False":
        new_hospital = spark.read.parquet(new_hospital_path)
        new_hospital = new_hospital.toPandas()["PHA"].tolist()
    # 生成 panel_filtered
    # 早于model所用时间（历史数据），用new_hospital补数;
    # 处于model所用时间（模型数据），不补数；
    # 晚于model所用时间（月更新数据），用unpublished和not arrived补数
    # 取消Sanofi AZ 特殊处理（20210506）
    city_list = [u'北京市', u'上海市', u'天津市', u'重庆市', u'广州市', u'深圳市', u'西安市', u'大连市', u'成都市', u'厦门市', u'沈阳市']
    Province_list = [u'河北省', u'福建省', u'河北', u"福建"]
    
    if monthly_update == "False":
        if project_name == u"贝达" or project_name == "Sanofi" or project_name == "AZ":
            panel_add_data = panel_add_data.where(panel_add_data.Molecule != u"奥希替尼")
    # 去除 city_list和 Province_list
    if add_47 == "False":
        panel_add_data = panel_add_data \
            .where(~panel_add_data.City.isin(city_list)) \
            .where(~panel_add_data.Province.isin(Province_list))
    
    if monthly_update == "False":
        panel_add_data_history = panel_add_data \
            .where(panel_add_data.HOSP_ID.isin(new_hospital)) \
            .where(panel_add_data.Date < int(model_month_left)) \
            .select(panel_raw_data.columns)
        panel_filtered = panel_raw_data.union(panel_add_data_history.select(panel_raw_data.columns))
    
    if monthly_update == "True":
        # unpublished文件
        # unpublished 列表创建：published_left中有而published_right没有的ID列表，然后重复12次，时间为current_year*100 + i
        published_left = spark.read.csv(published_left_path, header=True)
        published_left = published_left.select('ID').distinct()
        
        published_right = spark.read.csv(published_right_path, header=True)
        published_right = published_right.select('ID').distinct()
        
        unpublished_ID=published_left.subtract(published_right).toPandas()['ID'].values.tolist()
        unpublished_ID_num=len(unpublished_ID)
        all_month=list(range(1,13,1))*unpublished_ID_num
        all_month.sort()
        unpublished_dict={"ID":unpublished_ID*12,"Date":[current_year*100 + i for i in all_month]}
        
        df = pd.DataFrame(data=unpublished_dict)
        df = df[["ID","Date"]]
        schema = StructType([StructField("ID", StringType(), True), StructField("Date", StringType(), True)])
        unpublished = spark.createDataFrame(df, schema)
        unpublished = unpublished.select("ID","Date")
        
        # not_arrive文件
        Notarrive = spark.read.csv(not_arrived_path, header=True)
        Notarrive = Notarrive.select("ID","Date")
        
        # 合并unpublished和not_arrive文件
        Notarrive_unpublished = unpublished.union(Notarrive).distinct()
        
        future_range = Notarrive_unpublished.withColumn("Date", Notarrive_unpublished["Date"].cast(DoubleType()))
        panel_add_data_future = panel_add_data.where(panel_add_data.Date > int(model_month_right)) \
            .join(future_range, on=["Date", "ID"], how="inner") \
            .select(panel_raw_data.columns)
        panel_filtered = panel_raw_data.union(panel_add_data_future)
        # 与之前的panel结果合并
        if panel_for_union_path != "Empty":
            panel_for_union = spark.read.parquet(panel_for_union_path)
            panel_filtered =  panel_filtered.union(panel_for_union.select(panel_filtered.columns))
            
        

    # %%
    # panel_filtered.groupBy("add_flag").agg({"Sales": "sum"}).show()
    # panel_filtered.groupBy("add_flag").agg({"Sales": "sum"}).show()
    panel_filtered = panel_filtered.repartition(2)
    panel_filtered.write.format("parquet") \
        .mode("overwrite").save(panel_path)
        
    panel_filtered.where( (col('Date') >= time_left) & (col('Date') <= time_right) ) \
                .repartition(1).write.format("csv").option("header", "true") \
                .mode("overwrite").save(f"{panel_path}.csv")
                
    logger.debug("输出 panel_filtered 结果：" + panel_path)
    logger.debug('数据执行-Finish')



