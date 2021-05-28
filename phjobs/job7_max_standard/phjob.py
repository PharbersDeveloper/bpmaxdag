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
    max_path_list = kwargs['max_path_list']
    out_dir = kwargs['out_dir']
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
    project_name = '贝达'
    out_dir = '202012'
    # %%
    # max_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/"
    # extract_path = "s3a://ph-stream/common/public/max_result/0.0.5/"
    # project_name = "Beite"
    # "Astellas","Pfizer","Beite"
    
    # 月更的时候需要修改的max_list文件
    if max_path_list == "Empty":
        max_result_path_list_path = max_path  + "/" + project_name + "/max_result_path_list.csv"
    else:
        max_result_path_list_path = max_path_list
    
    # 通用匹配文件
    # product_map_path = max_path  + "/Common_files/extract_data_files/product_map_all"
    product_map_path = max_path + "/" + project_name + "/" + out_dir + "/prod_mapping"
    molecule_ACT_path = max_path  + "/Common_files/extract_data_files/product_map_all_ATC.csv"
    MAX_city_normalize_path = max_path  + "/Common_files/extract_data_files/MAX_city_normalize.csv"
    master_data_map_path = max_path  + "/Common_files/extract_data_files/master_data_map.csv"
    
    # 输出
    max_standard_path = extract_path + "/" + project_name + "_max_standard"
    max_standard_brief_path = extract_path + "/" + project_name + "_max_standard_brief"
    # %%
    # ========== 数据检查 prod_mapping =========
    misscols_dict = {}
    product_map = spark.read.parquet(product_map_path)
    
    if project_name == "Sanofi" or project_name == "AZ":
        product_map = product_map.withColumnRenamed(product_map.columns[21], "pfc")
    if project_name == "Eisai":
        product_map = product_map.withColumnRenamed(product_map.columns[22], "pfc")
    
    colnames_product_map = product_map.columns
    misscols_dict.setdefault(product_map_path, [])
    if ("标准通用名" not in colnames_product_map) and ("通用名_标准"  not in colnames_product_map)  \
    and ("药品名称_标准"  not in colnames_product_map) and ("通用名"  not in colnames_product_map) \
    and ("S_Molecule_Name"  not in colnames_product_map):
        misscols_dict[product_map_path].append("标准通用名")
    if ("min2" not in colnames_product_map) and ("min1_标准" not in colnames_product_map):
        misscols_dict[product_map_path].append("min2")
    if ("pfc" not in colnames_product_map) and ("packcode" not in colnames_product_map) \
    and ("Pack_ID" not in colnames_product_map) and ("Pack_Id" not in colnames_product_map) \
    and ("PackID" not in colnames_product_map) and ("packid" not in colnames_product_map):
        misscols_dict[product_map_path].append("pfc")
            
    # 判断输入文件是否有缺失列
    misscols_dict_final = {}
    for eachfile in misscols_dict.keys():
        if len(misscols_dict[eachfile]) != 0:
            misscols_dict_final[eachfile] = misscols_dict[eachfile]
    # 如果有缺失列，则报错，停止运行
    if misscols_dict_final:
        logger.debug('miss columns: %s' % (misscols_dict_final))
        raise ValueError('miss columns: %s' % (misscols_dict_final))
    # %%
    # ========== 数据 mapping =========
    
    # mapping用文件：注意各种mapping的去重，唯一匹配
    
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
    # ATC4_CODE 列如果是0，用ATC3_CODE补充，ATC3_CODE 列也是0，用ATC2_CODE补充
    molecule_ACT_map = molecule_ACT_map.withColumn('ATC4_CODE', func.when(molecule_ACT_map.ATC4_CODE == '0', 
                                                                    func.when(molecule_ACT_map.ATC3_CODE == '0', molecule_ACT_map.ATC2_CODE) \
                                                                      .otherwise(molecule_ACT_map.ATC3_CODE)) \
                                                               .otherwise(molecule_ACT_map.ATC4_CODE))
    
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
    # 有的min2结尾有空格与无空格的是两条不同的匹配
    if project_name == "Sanofi" or project_name == "AZ":
        product_map = product_map.withColumnRenamed(product_map.columns[21], "pfc")
    if project_name == "Eisai":
        product_map = product_map.withColumnRenamed(product_map.columns[22], "pfc")
        
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
                    .select("min2", "pfc", "通用名", "标准商品名", "标准剂型", "标准规格", "标准包装数量", "标准生产企业") \
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
    # f. 去重：保证每个min2只有一条信息, dropDuplicates会取first
    product_map = product_map.dropDuplicates(["min2"])
    
    # 5. 汇总max_result_path结果，并进行mapping                
    max_result_path_list = spark.read.csv(max_result_path_list_path, header=True)
    max_result_path_list = max_result_path_list.withColumn('time_left', max_result_path_list.time_left.cast(IntegerType())) \
                                            .withColumn('time_right', max_result_path_list.time_right.cast(IntegerType()))
    max_result_path_list = max_result_path_list.toPandas()
    

    # %%
    # 储存时间
    time_list= []
    for i in range(len(max_result_path_list)):
        max_result_path = max_result_path_list.loc[i].path.replace('s3a:', 's3:')
        time_left = max_result_path_list.loc[i].time_left
        time_list.append(time_left)
        time_right = max_result_path_list.loc[i].time_right
        time_list.append(time_right)
        
        # csv 文件和 parquet 文件判断
        if max_result_path.endswith(".csv"):
            max_result = spark.read.csv(max_result_path, header=True)
        else:
            max_result = spark.read.parquet(max_result_path)
        max_result = max_result.withColumn("Date", max_result.Date.cast(IntegerType()))
        max_result = max_result.where((max_result.Date >= int(time_left)) & (max_result.Date <= int(time_right)))
        
        # 杨森6月的max结果 衡水市- 湖北省 错误，先强制改为衡水市- 河北省
        if project_name == "Janssen":
            max_result = max_result.withColumn("Province", func.when(max_result.City == "衡水市", func.lit("河北省")) \
                                                        .otherwise(max_result.Province))
        
        # 1. max_result 的 Prod_Name（min2） 处理
        max_result = max_result.withColumn("Prod_Name_tmp", max_result.Prod_Name)
        max_result = max_result.withColumn("Prod_Name_tmp", func.regexp_replace("Prod_Name_tmp", "&amp;", "&")) \
                            .withColumn("Prod_Name_tmp", func.regexp_replace("Prod_Name_tmp", "&lt;", "<")) \
                            .withColumn("Prod_Name_tmp", func.regexp_replace("Prod_Name_tmp", "&gt;", ">"))
        if project_name == "Servier":
            max_result = max_result.withColumn("Prod_Name_tmp", func.regexp_replace("Prod_Name_tmp", "阿托伐他汀\\+齐鲁制药\\(海南\\)有限公司", "美达信"))
        if project_name == "NHWA":
            max_result = max_result.withColumn("Prod_Name_tmp", func.regexp_replace("Prod_Name_tmp", "迪施宁乳剂", "迪施乐乳剂"))
        
        # 2. product_map 匹配 min2 ：获得 PACK_ID, 通用名, 标准商品名, 标准剂型, 标准规格, 标准包装数量, 标准生产企业
        max_standard = max_result.join(product_map, max_result["Prod_Name_tmp"] == product_map["min2"], how="left") \
                            .drop("min2","Prod_Name_tmp")
                            
        # 3. packID_master_map 匹配 PACK_ID ：获得 MOLE_NAME_CH_1, ATC4_1, PROD_NAME_CH, "CORP_NAME_CH, DOSAGE, SPEC, PACK
        max_standard = max_standard.join(packID_master_map, on=["PACK_ID"], how="left")
        
        # 4. molecule_ACT_map 匹配 通用名：获得 MOLE_NAME_CH_2, ATC4_2
        max_standard = max_standard.join(molecule_ACT_map, on=["通用名"], how="left")
        
        # 5. 整合 master 匹配结果 和 product_map, molecule_ACT_map 匹配结果
        '''
        ATC4_1 和 MOLE_NAME_CH_1 来自 master 有 pack_id 匹配得到 ; ATC4_2 和 MOLE_NAME_CH_2 来自 molecule_ACT_map 
        '''
        # A10C/D/E是胰岛素, 通用名和公司名用master, 其他信息用product_map
        max_standard = max_standard.withColumn("ATC", func.when(max_standard["ATC4_1"].isNull(), max_standard["ATC4_2"]) \
                                                .otherwise(max_standard["ATC4_1"]))
                                                
        max_standard_yidaosu = max_standard.where(func.substring(max_standard.ATC, 0, 4).isin(['A10C', 'A10D', 'A10E'])) \
                                .withColumn("PROD_NAME_CH", max_standard['标准商品名']) \
                                .withColumn("DOSAGE", max_standard['标准剂型']) \
                                .withColumn("SPEC", max_standard['标准规格']) \
                                .withColumn("PACK", max_standard['标准包装数量'])
        
        max_standard_others = max_standard.where((~func.substring(max_standard.ATC, 0, 4).isin(['A10C', 'A10D', 'A10E'])) | max_standard.ATC.isNull())
        
        # 合并 max_standard_yidaosu 和 max_standard_others
        max_standard = max_standard_others.union(max_standard_yidaosu.select(max_standard_others.columns))
        
        # master 匹配不上的(ATC4_1是null)c用 molecule_ACT_map 和 product_map 信息
        max_standard = max_standard.withColumn("标准通用名", func.when(max_standard["MOLE_NAME_CH_1"].isNull(), max_standard["MOLE_NAME_CH_2"]) \
                                                .otherwise(max_standard["MOLE_NAME_CH_1"])) \
                                .withColumn("标准商品名", func.when(max_standard["ATC4_1"].isNull(), max_standard["标准商品名"]) \
                                                .otherwise(max_standard["PROD_NAME_CH"])) \
                                .withColumn("标准剂型", func.when(max_standard["ATC4_1"].isNull(), max_standard["标准剂型"]) \
                                                .otherwise(max_standard["DOSAGE"])) \
                                .withColumn("标准规格", func.when(max_standard["ATC4_1"].isNull(), max_standard["标准规格"]) \
                                                .otherwise(max_standard["SPEC"])) \
                                .withColumn("标准包装数量", func.when(max_standard["ATC4_1"].isNull(), max_standard["标准包装数量"]) \
                                                .otherwise(max_standard["PACK"])) \
                                .withColumn("标准生产企业", func.when(max_standard["ATC4_1"].isNull(), max_standard["标准生产企业"]) \
                                                .otherwise(max_standard["CORP_NAME_CH"])) \
                                .drop("ATC4_1", "ATC4_2", "MOLE_NAME_CH_1", "MOLE_NAME_CH_2")
        
        # 没有标准通用名的 用原始的通用名
        max_standard = max_standard.withColumn("标准通用名", func.when(max_standard['标准通用名'].isNull(), max_standard['通用名']) \
                                                            .otherwise(max_standard['标准通用名']))
        
        # city 标准化：
        '''
        先标准化省，再用(标准省份-City)标准化市
        '''
        max_standard = max_standard.join(MAX_city_normalize.select("Province", "标准省份名称").distinct(), on=["Province"], how="left")
        max_standard = max_standard.join(MAX_city_normalize.select("City", "标准省份名称", "标准城市名称").distinct(),
                                on=["标准省份名称", "City"], how="left")
        
        
        # 全量结果汇总
        if i == 0:
            max_standard_all = max_standard
        else:
            max_standard_all = max_standard_all.union(max_standard)
    
    
    # 全量结果汇总
    max_standard_all = max_standard_all.withColumn("project", func.lit(project_name))
    
    max_standard_all = max_standard_all.select("project", "Province", "City" ,"Date", "Prod_Name", "Molecule", "PANEL", "DOI", "Predict_Sales", "Predict_Unit", 
                                           "标准通用名", "标准商品名", "标准剂型", "标准规格", "标准包装数量", "标准生产企业", "标准省份名称", "标准城市名称", 
                                            "PACK_ID", "ATC")
    max_standard_all = max_standard_all.withColumn("Date_copy", max_standard_all.Date)
        
    # 目录结果汇总,
    max_standard_brief = max_standard_all.select("project", "Date", "标准通用名", "ATC", "DOI", "PACK_ID").distinct()
    
    # 获取时间范围
    time_list = [int(x) for x in time_list]
    time_range = str(min(time_list)) + '_' + str(max(time_list))
    # %%
    # 根据日期分桶写出
    max_standard_all = max_standard_all.repartition("Date_copy")
    max_standard_all.write.format("parquet").partitionBy("Date_copy") \
    .mode("overwrite").save(max_standard_path)
    # 输出brief结果
    max_standard_brief = max_standard_brief.repartition(1)
    max_standard_brief.write.format("parquet") \
    .mode("overwrite").save(max_standard_brief_path)

