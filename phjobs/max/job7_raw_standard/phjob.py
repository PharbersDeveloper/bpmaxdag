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
    extract_path = kwargs['extract_path']
    project_name = kwargs['project_name']
    if_two_source = kwargs['if_two_source']
    out_dir = kwargs['out_dir']
    minimum_product_sep = kwargs['minimum_product_sep']
    minimum_product_columns = kwargs['minimum_product_columns']
    ### input args ###
    
    ### output args ###
    a = kwargs['a']
    b = kwargs['b']
    ### output args ###

    from pyspark.sql import SparkSession, Window
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    import os
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col    # %%
    # project_name = "Takeda"
    # if_two_source = "True"
    # out_dir = "202012"

    # %%
    # 输入
    if minimum_product_sep == 'kong':
        minimum_product_sep = ''
    minimum_product_columns = minimum_product_columns.replace(' ', '').split(',')
    
    # 双源数据用 raw_data_std
    #if if_two_source == 'True':
    #	raw_data_path = max_path + '/' + project_name + '/' + out_dir + '/raw_data_std'
    #else:
    #	raw_data_path = max_path + '/' + project_name + '/' + out_dir + '/raw_data'
    
    raw_data_path = max_path + '/' + project_name + '/' + out_dir + '/raw_data_delivery'
    
    product_map_path = max_path + "/" + project_name + "/" + out_dir + "/prod_mapping"
    market_path  = max_path + "/" + project_name + "/mkt_mapping"
    
     # 通用匹配文件
    cpa_pha_mapping_path = max_path + "/Common_files/cpa_pha_mapping"
    universe_path = max_path + "/Common_files/universe_latest"
    molecule_ACT_path = max_path  + "/Common_files/extract_data_files/product_map_all_ATC.csv"
    MAX_city_normalize_path = max_path  + "/Common_files/extract_data_files/MAX_city_normalize.csv"
    master_data_map_path = max_path  + "/Common_files/extract_data_files/master_data_map.csv"
    
    # 输出
    raw_data_standard_path = extract_path + "/" + project_name + "_rawdata_standard"
    raw_data_standard_brief_path = extract_path + "/" + project_name + "_rawdata_standard_brief"
    

    # %%
    # ===========  数据执行 ============
    
    # 一. 标准化匹配文件处理
    
    # 1. 城市标准化
    MAX_city_normalize = spark.read.csv(MAX_city_normalize_path, header=True)
    
    # 2. master_data_map：PACK_ID - 标准通用名 - ACT, 无缺失
    master_data_map = spark.read.csv(master_data_map_path, header=True)
    packID_master_map = master_data_map.select("PACK_ID", "MOLE_NAME_CH", "PROD_NAME_CH", "CORP_NAME_CH", "DOSAGE", "SPEC", "PACK", "ATC4_CODE") \
                                        .distinct() \
                                        .withColumn("PACK_ID", master_data_map.PACK_ID.cast(IntegerType())) \
                                        .withColumnRenamed("MOLE_NAME_CH", "MOLE_NAME_CH_1") \
                                        .withColumnRenamed("ATC4_CODE", "ATC4_1")
    # 是否有重复
    num1 = packID_master_map.count()
    num2 = packID_master_map.dropDuplicates(["PACK_ID"]).count()
    logger.debug(num1 - num2)
    packID_master_map = packID_master_map.dropDuplicates(["PACK_ID"])
                        
    
    # 3. product_map_all_ATC: 有补充的新的 PACK_ID - 标准通用名 - ACT （0是缺失）
    molecule_ACT_map = spark.read.csv(molecule_ACT_path, header=True)
    
    add_PACK_ID = molecule_ACT_map.where(molecule_ACT_map.project == project_name).select("min2", "PackID").distinct() \
                    .withColumn("PackID", molecule_ACT_map.PackID.cast(IntegerType()))
    add_PACK_ID = add_PACK_ID.withColumn("PackID", func.when(add_PACK_ID.PackID == "0", None).otherwise(add_PACK_ID.PackID)) \
                    .withColumnRenamed("PackID", "PackID_add") 
    
    molecule_ACT_map = molecule_ACT_map.select("通用名", "MOLE_NAME_CH", "ATC4_CODE") \
                    .withColumnRenamed("MOLE_NAME_CH", "MOLE_NAME_CH_2") \
                    .withColumnRenamed("ATC4_CODE", "ATC4_2") \
                    .dropDuplicates(["通用名"])
    molecule_ACT_map = molecule_ACT_map.withColumn("MOLE_NAME_CH_2", func.when(molecule_ACT_map.MOLE_NAME_CH_2 == "0", None).otherwise(molecule_ACT_map.MOLE_NAME_CH_2)) \
                        .withColumn("ATC4_2", func.when(molecule_ACT_map.ATC4_2 == "0", None).otherwise(molecule_ACT_map.ATC4_2))
                        
    # 4. 产品信息，列名标准化
    product_map = spark.read.parquet(product_map_path)
    # a. 列名清洗统一
    # if project_name == "Sanofi" or project_name == "AZ":
    #     product_map = product_map.withColumnRenamed(product_map.columns[21], "pfc")
    # if project_name == "Eisai":
    #    product_map = product_map.withColumnRenamed(product_map.columns[22], "pfc")
        
    for col in product_map.columns:
        if col in ["标准通用名", "通用名_标准", "药品名称_标准", "S_Molecule_Name"]:
            product_map = product_map.withColumnRenamed(col, "通用名")
        if col in ["min1_标准"]:
            product_map = product_map.withColumnRenamed(col, "min2")
        if col in ["packcode", "Pack_ID", "Pack_Id", "PackID", "packid"]:
            product_map = product_map.withColumnRenamed(col, "pfc")
        if col in ["商品名_标准", "S_Product_Name"]:
            product_map = product_map.withColumnRenamed(col, "标准商品名")
        if col in ["剂型_标准", "Form_std", "S_Dosage"]:
            product_map = product_map.withColumnRenamed(col, "标准剂型")
        if col in ["规格_标准", "Specifications_std", "药品规格_标准", "S_Pack"]:
            product_map = product_map.withColumnRenamed(col, "标准规格")
        if col in ["包装数量2", "包装数量_标准", "Pack_Number_std", "S_PackNumber", "最小包装数量"]:
            product_map = product_map.withColumnRenamed(col, "标准包装数量")
        if col in ["标准企业", "生产企业_标准", "Manufacturer_std", "S_CORPORATION", "标准生产厂家"]:
            product_map = product_map.withColumnRenamed(col, "标准生产企业")
    if project_name == "Janssen" or project_name == "NHWA":
        if "标准剂型" not in product_map.columns:
            product_map = product_map.withColumnRenamed("剂型", "标准剂型")
        if "标准规格" not in product_map.columns:
            product_map = product_map.withColumnRenamed("规格", "标准规格")
        if "标准生产企业" not in product_map.columns:
            product_map = product_map.withColumnRenamed("生产企业", "标准生产企业")
        if "标准包装数量" not in product_map.columns:
            product_map = product_map.withColumnRenamed("包装数量", "标准包装数量")
            
    # b. 选取需要的列
    product_map = product_map \
                    .select("min1", "min2", "pfc", "通用名", "标准商品名", "标准剂型", "标准规格", "标准包装数量", "标准生产企业") \
                    .withColumn("pfc", product_map["pfc"].cast(IntegerType())) \
                    .withColumn("标准包装数量", product_map["标准包装数量"].cast(IntegerType())) \
                    .distinct()
    
    # c. pfc为0统一替换为null
    product_map = product_map.withColumn("pfc", func.when(product_map.pfc == 0, None).otherwise(product_map.pfc)).distinct()
    product_map = product_map.withColumn("project", func.lit(project_name)).distinct()
    
    # d. min2处理
    product_map = product_map.withColumnRenamed("pfc", "PACK_ID") \
                    .withColumn("min2", func.regexp_replace("min2", "&amp;", "&")) \
                    .withColumn("min2", func.regexp_replace("min2", "&lt;", "<")) \
                    .withColumn("min2", func.regexp_replace("min2", "&gt;", ">"))
                    
    # e. 补充PACK_ID
    product_map = product_map.join(add_PACK_ID, on="min2", how="left")
    product_map = product_map.withColumn("PACK_ID", 
                            func.when((product_map.PACK_ID.isNull()) & (~product_map.PackID_add.isNull()), 
                            product_map.PackID_add).otherwise(product_map.PACK_ID)) \
                            .drop("PackID_add")
    # f. 去重：保证每个min1只有一条信息, dropDuplicates会取first
    product_map = product_map.dropDuplicates(["min1"])
    # %%
    # 二. raw_data 基础信息匹配
    '''
    raw_data 用min1匹配 product_map的标准列
    '''
    raw_data = spark.read.parquet(raw_data_path)
    
    for colname, coltype in raw_data.dtypes:
        if coltype == "logical":
            raw_data = raw_data.withColumn(colname, raw_data[colname].cast(StringType()))
            
    # 1. cpa_pha_mapping 匹配获得 PHA
    cpa_pha_mapping = spark.read.parquet(cpa_pha_mapping_path)
    cpa_pha_mapping = cpa_pha_mapping.where(cpa_pha_mapping["推荐版本"] == 1) \
        .select("ID", "PHA").distinct()
    
    # ID 的长度统一
    def distinguish_cpa_gyc(col, gyc_hospital_id_length):
        # gyc_hospital_id_length是国药诚信医院编码长度，一般是7位数字，cpa医院编码一般是6位数字。医院编码长度可以用来区分cpa和gyc
        return (func.length(col) < gyc_hospital_id_length)
    def deal_ID_length(df):
        df = df.withColumn("ID", df["ID"].cast(StringType()))
        df = df.withColumn("ID", func.regexp_replace("ID", "\\.0", ""))
        df = df.withColumn("ID", func.when(distinguish_cpa_gyc(df.ID, 7), func.lpad(df.ID, 6, "0")).otherwise(df.ID))
        return df
    
    cpa_pha_mapping = deal_ID_length(cpa_pha_mapping)
    raw_data = deal_ID_length(raw_data)
                                    
    raw_data = raw_data.join(cpa_pha_mapping, on="ID", how="left")
    
    # 2. universe PHA 匹配获得 HOSP_NAME，Province，City
    universe = spark.read.parquet(universe_path)
    universe = universe.select('新版ID', '新版名称', 'Province', 'City').distinct() \
                        .withColumnRenamed('新版ID', 'PHA') \
                        .withColumnRenamed('新版名称', 'PHA医院名称')
    
    raw_data = raw_data.join(universe, on="PHA", how="left")
    
    # 3. 生成min1
    # Mylan不重新生成min1，其他项目生成min1
    # if project_name != "Mylan":
    if "min1" in raw_data.columns:
        raw_data = raw_data.drop("min1")
    raw_data = raw_data.withColumn('Brand', func.when((raw_data.Brand.isNull()) | (raw_data.Brand == 'NA'), raw_data.Molecule).otherwise(raw_data.Brand))
    raw_data = raw_data.withColumn("min1", func.when(raw_data[minimum_product_columns[0]].isNull(), func.lit("NA")).
                                       otherwise(raw_data[minimum_product_columns[0]]))
    for col in minimum_product_columns[1:]:
        raw_data = raw_data.withColumn(col, raw_data[col].cast(StringType()))
        raw_data = raw_data.withColumn("min1", func.concat(
            raw_data["min1"],
            func.lit(minimum_product_sep),
            func.when(func.isnull(raw_data[col]), func.lit("NA")).otherwise(raw_data[col])))
    

    # %%
    # 三. 标准化raw_data
    
    # 2. product_map 匹配 min2 ：获得 PACK_ID, 通用名, 标准商品名, 标准剂型, 标准规格, 标准包装数量, 标准生产企业
    # if project_name == "Mylan":
    #    product_map = product_map.drop('PACK_ID')
    #    raw_data = raw_data.withColumnRenamed('Pack_ID', 'PACK_ID')
    data_standard = raw_data.join(product_map, on='min1', how="left")
    
    # 市场名匹配
    market_map = spark.read.parquet(market_path)
    market_map = market_map.withColumnRenamed("标准通用名", "通用名") \
        .withColumnRenamed("model", "DOI") \
        .withColumnRenamed("mkt", "DOI") \
        .select("DOI", "通用名").distinct()
    
    data_standard = data_standard.join(market_map, on='通用名', how='left')
    
    # 3. packID_master_map 匹配 PACK_ID ：获得master标准列 --- MOLE_NAME_CH_1, ATC4_1, PROD_NAME_CH, "CORP_NAME_CH, DOSAGE, SPEC, PACK
    data_standard = data_standard.join(packID_master_map, on=["PACK_ID"], how="left")
    
    # 4. molecule_ACT_map 匹配 通用名：获得master标准分子名和ATC -- MOLE_NAME_CH_2, ATC4_2
    data_standard = data_standard.join(molecule_ACT_map, on=["通用名"], how="left")
    
    # 5. 整合 master 匹配结果 和 product_map, molecule_ACT_map 匹配结果
    '''
    ATC4_1 和 MOLE_NAME_CH_1 来自 master 有 pack_id 匹配得到 ; ATC4_2 和 MOLE_NAME_CH_2 来自 molecule_ACT_map 
    '''
    # A10C/D/E是胰岛素, 通用名和公司名用master, 其他信息用product_map
    data_standard = data_standard.withColumn("ATC", func.when(data_standard["ATC4_1"].isNull(), data_standard["ATC4_2"]) \
                                            .otherwise(data_standard["ATC4_1"]))
                                            
    data_standard_yidaosu = data_standard.where(func.substring(data_standard.ATC, 0, 4).isin(['A10C', 'A10D', 'A10E'])) \
                            .withColumn("PROD_NAME_CH", data_standard['标准商品名']) \
                            .withColumn("DOSAGE", data_standard['标准剂型']) \
                            .withColumn("SPEC", data_standard['标准规格']) \
                            .withColumn("PACK", data_standard['标准包装数量'])
    
    data_standard_others = data_standard.where((~func.substring(data_standard.ATC, 0, 4).isin(['A10C', 'A10D', 'A10E'])) | data_standard.ATC.isNull())
    
    # 合并 max_standard_yidaosu 和 max_standard_others
    data_standard = data_standard_others.union(data_standard_yidaosu.select(data_standard_others.columns))
    
    # master 匹配不上的(ATC4_1是null) 用 molecule_ACT_map 和 product_map 信息
    data_standard = data_standard.withColumn("标准通用名", func.when(data_standard["MOLE_NAME_CH_1"].isNull(), data_standard["MOLE_NAME_CH_2"]) \
                                            .otherwise(data_standard["MOLE_NAME_CH_1"])) \
                            .withColumn("标准商品名", func.when(data_standard["ATC4_1"].isNull(), data_standard["标准商品名"]) \
                                            .otherwise(data_standard["PROD_NAME_CH"])) \
                            .withColumn("标准剂型", func.when(data_standard["ATC4_1"].isNull(), data_standard["标准剂型"]) \
                                            .otherwise(data_standard["DOSAGE"])) \
                            .withColumn("标准规格", func.when(data_standard["ATC4_1"].isNull(), data_standard["标准规格"]) \
                                            .otherwise(data_standard["SPEC"])) \
                            .withColumn("标准包装数量", func.when(data_standard["ATC4_1"].isNull(), data_standard["标准包装数量"]) \
                                            .otherwise(data_standard["PACK"])) \
                            .withColumn("标准生产企业", func.when(data_standard["ATC4_1"].isNull(), data_standard["标准生产企业"]) \
                                            .otherwise(data_standard["CORP_NAME_CH"])) \
                            .drop("ATC4_1", "ATC4_2", "MOLE_NAME_CH_1", "MOLE_NAME_CH_2")
    
    # MOLE_NAME_CH_1 和 MOLE_NAME_CH_2 没有标准通用名的 用原始的通用名
    data_standard = data_standard.withColumn("标准通用名", func.when(data_standard['标准通用名'].isNull(), data_standard['通用名']) \
                                                        .otherwise(data_standard['标准通用名']))
    
    # city 标准化：
    '''
    先标准化省，再用(标准省份-City)标准化市
    '''
    data_standard = data_standard.join(MAX_city_normalize.select("Province", "标准省份名称").distinct(), on=["Province"], how="left")
    data_standard = data_standard.join(MAX_city_normalize.select("City", "标准省份名称", "标准城市名称").distinct(),
                            on=["标准省份名称", "City"], how="left")
    
    # 全量结果汇总
    std_names = ["Date", "ID", "Raw_Hosp_Name", "Brand", "Form", "Specifications", "Pack_Number", "Manufacturer", "Molecule",
             "Source", "Sales", "Units", "Units_Box", "PHA", "PHA医院名称", "Province", "City", "min1"]
    if "Raw_Hosp_Name" not in data_standard.columns:
        data_standard = data_standard.withColumn("Raw_Hosp_Name", func.lit("0"))
    if "Units_Box" not in data_standard.columns:
        data_standard = data_standard.withColumn("Units_Box", func.lit(0))
        
    # 有的项目Raw_Hosp_Name全都为null，会在提数中间结果写出再读取时引起报错
    data_standard = data_standard.withColumn("Raw_Hosp_Name", func.when(data_standard.Raw_Hosp_Name.isNull(), func.lit("0")) \
                                                                    .otherwise(data_standard.Raw_Hosp_Name))
    
    for each in data_standard.columns:                                                                
        data_standard = data_standard.withColumn(each, data_standard[each].cast(StringType()))
    
    data_standard = data_standard.withColumn("project", func.lit(project_name))
        
    raw_data_standard = data_standard.select(std_names + ["DOI", "标准通用名", "标准商品名", "标准剂型", "标准规格", 
        "标准包装数量", "标准生产企业", "标准省份名称", "标准城市名称", "PACK_ID", "ATC", "project"])
    
    raw_data_standard = raw_data_standard.withColumn("Date_copy", raw_data_standard.Date)
    
    # 目录结果汇总
    raw_data_standard_brief = raw_data_standard.select("project", "Date", "标准通用名", "ATC", "DOI", "PHA", "Source").distinct()
    # %%
    # 根据日期分桶写出
    raw_data_standard = raw_data_standard.repartition("Date_copy")
    raw_data_standard.write.format("parquet").partitionBy("Date_copy") \
        .mode("overwrite").save(raw_data_standard_path)
    # 输出brief结果
    raw_data_standard_brief = raw_data_standard_brief.repartition(2)
    raw_data_standard_brief.write.format("parquet") \
        .mode("overwrite").save(raw_data_standard_brief_path)
