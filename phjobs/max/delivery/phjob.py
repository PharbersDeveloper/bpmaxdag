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
    project_name = kwargs['project_name']
    time_left = kwargs['time_left']
    time_right = kwargs['time_right']
    out_dir = kwargs['out_dir']
    extract_path = kwargs['extract_path']
    max_path = kwargs['max_path']
    ### input args ###
    
    ### output args ###
    c = kwargs['c']
    d = kwargs['d']
    ### output args ###

    
    from pyspark.sql.types import StringType, IntegerType, DoubleType
    from pyspark.sql import functions as func
    import os
    import re
    from pyspark.sql.functions import pandas_udf, PandasUDFType, col, udf    
    # %%
    # project_name = "Takeda"
    # time_left = "201801"
    # time_right = "202012"
    # out_dir = "202012"

    # %%
    # MAX数据交付

    # %%
    ## 输入输出
    time_left = int(time_left)
    time_right = int(time_right)
    
    product_map_path = max_path + "/" + project_name + "/" + out_dir + "/prod_mapping"
    max_standard_path = extract_path + "/" + project_name + "_max_standard"
    province_city_mapping_path = max_path + '/' + project_name + '/province_city_mapping'
    if project_name == 'Gilead':
        corp_mapping_path = max_path + "/Gilead/FOR_MAX_standardization/corp_mapping"
        brand_mapping_path = max_path + "/Gilead/FOR_MAX_standardization/brand_mapping"
    if project_name == '奥鸿':    
        raw_data_path = max_path + "/" + project_name + "/" + out_dir + "/product_mapping_out"
    if project_name == 'NHWA':
        mofang_map_path = max_path + '/NHWA/FOR_delivery_files/恩华药品对照表' + out_dir[2:] + '.csv'
        city_tier_1_path = max_path + '/NHWA/FOR_delivery_files/Other_MKT_Segment_Pfizer_0823.csv'
    if project_name == 'Astellas':
        City_path = max_path + '/Astellas/FOR_delivery_files/ANS_新版省市mapping_City.csv'
        Province_path = max_path + '/Astellas/FOR_delivery_files/ANS_新版省市mapping_Province.csv'
    if project_name == 'Servier':
        ATC_molecule_path = max_path + '/Servier/FOR_delivery_files/ATC_molecule.csv'
    if project_name == 'XLT':
        Region_map_path = max_path + '/XLT/FOR_delivery_files/Region_map.csv'
        Province_City_map_path = max_path + '/XLT/FOR_delivery_files/信立泰省市匹配表.csv'
        china_Region_map_path = max_path + '/XLT/FOR_delivery_files/账号权限省份Mapping.csv'
        PTD_map_path = max_path + '/XLT/FOR_delivery_files/PTD_map.csv'
    if project_name == 'Pfizer':
        GEO_CD_city_map_path = max_path + '/Pfizer/FOR_delivery_files/GEO_CD_city.csv'
        change_min2_map_path = max_path + '/Pfizer/FOR_delivery_files/新老min2转换_2020.csv'
        sifutuo_ratio_map_path = max_path + '/Pfizer/FOR_delivery_files/思福妥系数.csv'
    if project_name == 'Takeda':
        ims_mapping_path  = max_path + '/Common_files/IMS_flat_files/' + out_dir + '/ims_mapping_' + out_dir + '.csv'
            
    # 输出
    time_range = str(time_left) + '_' + str(time_right)
    out_path = max_path + "/" + project_name + "/" + out_dir + "/Delivery/" + project_name + "_max_delivery_" + time_range + '.csv'
    if project_name == '奥鸿':
        out2_path = max_path + "/" + project_name + "/" + out_dir + "/Delivery/" + project_name + "_max_delivery_" + time_range + '_others.csv'
    if project_name == 'NHWA':
        out2_path = max_path + "/" + project_name + "/" + out_dir + "/Delivery/" + project_name + "_max_delivery_" + time_range + '_all.csv'
    if project_name == 'XLT':
        out_noPTD_path = max_path + "/" + project_name + "/" + out_dir + "/Delivery/待清洗PTD系数.csv"

    # %%
    # ==========  数据执行  ============
    
    # ====  一. 函数定义  ====
    
    def deal_ID_length(df):
        # ID不足7位的补足0到6位
        # 国药诚信医院编码长度是7位数字，cpa医院编码是6位数字。
        df = df.withColumn("ID", df["ID"].cast(StringType()))
        # 去掉末尾的.0
        df = df.withColumn("ID", func.regexp_replace("ID", "\\.0", ""))
        df = df.withColumn("ID", func.when(func.length(df.ID) < 7, func.lpad(df.ID, 6, "0")).otherwise(df.ID))
        return df

    # %%
    # NHWA
    if project_name == "NHWA":
        @udf(StringType())
        def for_ACC(name):
            # ACC1/ACC2
            if name in ["咪达唑仑", "右美托咪定"]:
                newname = "ACC1"
            elif name in ["丙泊酚", "依托咪酯","芬太尼","瑞芬太尼","舒芬太尼"]:
                newname = "ACC2"
            else:
                newname = "-"
            return newname
    
        @udf(StringType())
        def for_region(pro, acc):
            # 区域
            if pro in ["黑龙江","吉林","辽宁"] and (acc == "ACC1"):
                newname = "东北区"
            elif pro in ["北京","天津","上海"] and (acc == "ACC1"):
                newname = "华北一区"
            elif pro in ["河北","山西","内蒙古"] and (acc == "ACC1"):
                newname = "华北二区"
            elif pro in ["江苏"] and (acc == "ACC1"):
                newname = "华东一区"
            elif pro in ["浙江","福建"] and (acc == "ACC1"):
                newname = "华东二区"
            elif pro in ["湖南","湖北","江西"] and (acc == "ACC1"):
                newname = "中南区"
            elif pro in ["广东","广西","海南"] and (acc == "ACC1"):
                newname = "华南区"
            elif pro in ["重庆","四川"] and (acc == "ACC1"):
                newname = "西南一区"
            elif pro in ["贵州","云南"] and (acc == "ACC1"):
                newname = "西南二区"
            elif pro in ['河南',"陕西","宁夏","新疆"] and (acc == "ACC1"):
                newname = "西北一区"
            elif pro in ["安徽","山东"] and (acc == "ACC1"):
                newname = "华东三区"    
            elif pro in ["黑龙江","吉林","辽宁"] and (acc == "ACC2"):
                newname = "东北区"
            elif pro in ["北京","天津"] and (acc == "ACC2"):
                newname = "华北一区"
            elif pro in ["河北","山西","内蒙古"] and (acc == "ACC2"):
                newname = "华北二区"
            elif pro in ["江苏"] and (acc == "ACC2"):
                newname = "华东一区"
            elif pro in ["浙江","福建"] and (acc == "ACC2"):
                newname = "华东二区"
            elif pro in ["湖南","湖北","江西"] and (acc == "ACC2"):
                newname = "中南区"
            elif pro in ["广东","广西","海南"] and (acc == "ACC2"):
                newname = "华南区"
            elif pro in ["重庆","四川"] and (acc == "ACC2"):
                newname = "西南一区"
            elif pro in ["贵州","云南"] and (acc == "ACC2"):
                newname = "西南二区"
            elif pro in ['河南',"陕西","宁夏","新疆"] and (acc == "ACC2"):
                newname = "西北一区"
            elif pro in ["安徽","山东"] and (acc == "ACC2"):
                newname = "华东三区"
            elif pro in ["上海"] and (acc == "ACC2"):
                newname = "上海区" 
            else:
                newname = "-"
            return newname

    # %%
    # ====  二. 数据准备  ====  
    
    # 产品信息，列名标准化
    product_map = spark.read.parquet(product_map_path)
    # a. 列名清洗统一
    # 有的min2结尾有空格与无空格的是两条不同的匹配
    #if project_name == "Sanofi" or project_name == "AZ":
    #    product_map = product_map.withColumnRenamed(product_map.columns[21], "pfc")
    #if project_name == "Eisai":
    #    product_map = product_map.withColumnRenamed(product_map.columns[22], "pfc")
    
    for i in product_map.columns:
        if i in ["标准通用名", "通用名_标准", "药品名称_标准", "S_Molecule_Name"]:
            product_map = product_map.withColumnRenamed(i, "通用名")
        if i in ["min1_标准"]:
            product_map = product_map.withColumnRenamed(i, "min2")
        if i in ["packcode", "Pack_ID", "Pack_Id", "PackID", "packid"]:
            product_map = product_map.withColumnRenamed(i, "pfc")
        if i in ["商品名_标准", "S_Product_Name"]:
            product_map = product_map.withColumnRenamed(i, "标准商品名")
        if i in ["剂型_标准", "Form_std", "S_Dosage"]:
            product_map = product_map.withColumnRenamed(i, "标准剂型")
        if i in ["规格_标准", "Specifications_std", "药品规格_标准", "S_Pack"]:
            product_map = product_map.withColumnRenamed(i, "标准规格")
        if i in ["包装数量2", "包装数量_标准", "Pack_Number_std", "S_PackNumber", "最小包装数量"]:
            product_map = product_map.withColumnRenamed(i, "标准包装数量")
        if i in ["标准企业", "生产企业_标准", "Manufacturer_std", "S_CORPORATION", "标准生产厂家"]:
            product_map = product_map.withColumnRenamed(i, "标准生产企业")
        if i in ["ROAD"]:
            product_map = product_map.withColumnRenamed(i, "Route")
    if project_name == "Janssen" or project_name == "NHWA":
        if "标准剂型" not in product_map.columns:
            product_map = product_map.withColumnRenamed("剂型", "标准剂型")
        if "标准规格" not in product_map.columns:
            product_map = product_map.withColumnRenamed("规格", "标准规格")
        if "标准生产企业" not in product_map.columns:
            product_map = product_map.withColumnRenamed("生产企业", "标准生产企业")
        if "标准包装数量" not in product_map.columns:
            product_map = product_map.withColumnRenamed("包装数量", "标准包装数量")
    
    # b. min2处理
    product_map = product_map.withColumn("min2", func.regexp_replace("min2", "&amp;", "&")) \
                    .withColumn("min2", func.regexp_replace("min2", "&lt;", "<")) \
                    .withColumn("min2", func.regexp_replace("min2", "&gt;", ">"))
    
    if 'Route' not in product_map.columns:
        product_map = product_map.withColumn('Route', func.lit('-'))
        
    # c. 列处理
    if project_name != 'Pfizer':
        product_map = product_map.withColumn("pfc", product_map["pfc"].cast(IntegerType())) \
    
    product_map = product_map.withColumn("标准包装数量", product_map["标准包装数量"].cast(IntegerType())) \
                        .withColumnRenamed("pfc", "PACK_ID") \
                        .distinct()
    
    # d. pfc为0统一替换为null
    product_map = product_map.withColumn("PACK_ID", func.when((col('PACK_ID') == 0) | (col('PACK_ID') == 'nan'), None) \
                                                 .otherwise(col('PACK_ID'))).distinct()
    
    # e 选取需要的列
    if project_name == 'NHWA':
        product_map = product_map \
                    .withColumn('mofang_prod', func.concat(col('药品名称'), col('商品名'), col('剂型'),col('规格'), col('生产企业'))) \
                    .withColumn('mofang_prod', func.regexp_replace("mofang_prod", " ", "")) \
                    .withColumn('mofang_prod_2', func.concat(col('通用名'), col('标准商品名'), col('标准剂型'), col('标准规格'), col('标准生产企业'))) \
                    .withColumn('mofang_prod_2', func.regexp_replace("mofang_prod_2", " ", "")) \
                    .select("min1", "PACK_ID", "通用名", "标准商品名", "标准剂型", "标准规格", "标准包装数量", "标准生产企业", 'Route', 
                           "min2", "商品名+SKU", "毫克数", "mofang_prod", "mofang_prod_2")
    elif project_name == 'Takeda':
        product_map = product_map \
                    .select("min1", "PACK_ID", "通用名", "标准商品名", "标准剂型", "标准规格", "标准包装数量", "标准生产企业", 'Route', "min2", 
                           'Molecule_EN', 'Prd_desc', 'Form_EN', 'Size_EN', 'PckSize_Desc', 'Mnf_Desc') 
        
    else:
        # 项目通用标准列
        product_map = product_map \
                    .select("min1", "PACK_ID", "通用名", "标准商品名", "标准剂型", "标准规格", "标准包装数量", "标准生产企业", 'Route', "min2") 
        

    # %%
    # =====  三. Max数据处理 =====
    
    # ===== 1. 提取交付数据  =====
    
    # 1. 读取交付数据
    if time_left//100 == time_right//100:
        monthlist = range(time_left, time_right+1, 1)
    else:
        monthlist = list(range(time_right//100*100+1, time_right+1, 1))
        years=range(time_left//100, time_right//100, 1)
        for each in years:
            monthlist.extend(range(each*100+1, each*100+13, 1))
            
    path_list = [max_standard_path + '/Date_copy=' + str(i) for i in monthlist]
    
    index = 0
    for eachpath in path_list:
        df = spark.read.parquet(eachpath)
        if index ==0:
            data_standard = df
        else:
            data_standard = data_standard.union(df)
        index += 1
    
    data_standard = data_standard.withColumn("Prod_Name", func.regexp_replace("Prod_Name", "&amp;", "&")) \
                                .withColumn("Prod_Name", func.regexp_replace("Prod_Name", "&lt;", "<")) \
                                .withColumn("Prod_Name", func.regexp_replace("Prod_Name", "&gt;", ">"))

    # %%
    # ===== 2. Sales，Units 处理  =====
    # Sales，Units 处理
    '''
    包装数量为空的是others， Sales 或者 Units 可以为0
    包装数量不为空的，Sales和Units只要有一列为0，那么都调整为0；Units先四舍五入为整数，然后变化的系数乘以Sales获得新的Sales
    Sales 保留两位小数
    负值调整为0
    去掉 Sales，Units 同时为0的行
    '''
    data_standard_0 = data_standard.withColumn("Predict_Sales", col("Predict_Sales").cast(DoubleType())) \
                            .withColumn("Predict_Unit", col("Predict_Unit").cast(DoubleType())) \
                            .fillna(0, ["Predict_Sales", "Predict_Unit"])
    
    data_standard_0 = data_standard_0.withColumn("Units", func.when((~col("标准包装数量").isNull()) & (col('Predict_Unit') <= 0), func.lit(0)) \
                                                                    .otherwise(func.round(col('Predict_Unit'), 0)))
    
    data_standard_0 = data_standard_0.withColumn("p", col('Units')/col('Predict_Unit'))
    data_standard_0 = data_standard_0.withColumn("p", func.when((~col("标准包装数量").isNull()) & (col("p").isNull()), func.lit(0)) \
                                                        .otherwise(col('p')))
    data_standard_0 = data_standard_0.withColumn("p", func.when((col("标准包装数量").isNull()) & (col("p").isNull()), func.lit(1)) \
                                                        .otherwise(col('p')))
    
    data_standard_0 = data_standard_0.withColumn("Sales", col('Predict_Sales') * col('p'))
    
    data_standard_0 = data_standard_0.withColumn("Sales", func.round(col('Sales'), 2)) \
                                .withColumn("Units", col("Units").cast(IntegerType())) \
                                .drop("Predict_Unit", "Predict_Sales", "p")
    # 负值调整为0
    data_standard_0 = data_standard_0.withColumn("Sales", func.when(col('Sales') < 0 , func.lit(0)).otherwise(col('Sales'))) \
                                    .withColumn("Units", func.when(col('Sales') == 0, func.lit(0)).otherwise(col('Units')))
    
    # 去掉 Sales，Units 同时为0的行
    data_standard_1 = data_standard_0.where(col("标准包装数量").isNull())
    data_standard_2 = data_standard_0.where((~col("标准包装数量").isNull()) & (col('Sales') != 0) & (col('Units') != 0))
    
    data_standard_3 =  data_standard_1.union(data_standard_2)

    # %%
    # 测试
    #data_standard_3 = data_standard.withColumnRenamed('Predict_Unit', 'Units') \
    #                                .withColumnRenamed('Predict_Sales', 'Sales')

    # %%
    # ===== 3.信息匹配  =====
    
    # 信息匹配
    if project_name == 'Pfizer':
        # change_min2_map 新老min2转换
        change_min2_map = spark.read.csv(change_min2_map_path, header=True)
        change_min2_map = change_min2_map.select('min2_old', 'min2_new').distinct()
            
        data_standard_3 = data_standard_3.join(change_min2_map, data_standard_3.Prod_Name == change_min2_map.min2_old, 
                                                          how='left') \
                                        .withColumn('Prod_Name', func.when(~col('min2_new').isNull(), col('min2_new')) \
                                                                               .otherwise(col('Prod_Name')))
        
    # 通用匹配产品信息    
    data_standard_map_info = data_standard_3.select("Province", "City", "Date", "Prod_Name", "Molecule", "PANEL", "DOI",
                                                "Sales", "Units")
    data_standard_map_info = data_standard_map_info.join(product_map.dropDuplicates(['min2']), 
                                                         data_standard_map_info.Prod_Name == product_map.min2, how='left')
    
    
    # Pfizer 多了一些匹配信息
    if project_name == 'Takeda':
        # 匹配
        ims_mapping = spark.read.csv(ims_mapping_path, header=True)
        ims_mapping = ims_mapping.select('Pack_Id0', 'Mnf_Desc', 'MNF_TYPE').distinct() \
                                .withColumnRenamed('Pack_Id0', 'PACK_ID')
        data_standard_map_info = data_standard_map_info.join(ims_mapping.select('Mnf_Desc','MNF_TYPE').dropDuplicates(['Mnf_Desc']), 
                                                             on='Mnf_Desc', how='left')
        data_standard_map_info = data_standard_map_info.withColumn('MNF_TYPE', func.regexp_replace('MNF_TYPE', 'I|J', 'MNC')) \
                                                        .withColumn('MNF_TYPE', func.regexp_replace('MNF_TYPE', 'L', 'LOCAL'))
        
    # Pfizer 多了一些匹配信息
    if project_name == 'Pfizer':
        data_standard_map_info = data_standard_map_info.withColumn('City', func.regexp_replace('City', '市|地区', '')) \
                                                    .withColumn('Date', func.concat_ws('M', func.substring(col('Date'), 0, 4), 
                                                                                       func.substring(col('Date'), 5, 2)))
        
        # 1.GEO_CD_city_map
        GEO_CD_city_map = spark.read.csv(GEO_CD_city_map_path, header=True)
        GEO_CD_city_map = GEO_CD_city_map.withColumn('City', func.regexp_replace('City', '地区', '')) \
                                        .select('City', 'GEO_CD').distinct()
          
        # 2.sifutuo_ratio_map
        sifutuo_ratio_map = spark.read.csv(sifutuo_ratio_map_path, header=True)
        sifutuo_ratio_map = sifutuo_ratio_map.select('Date', 'ratio').distinct()
        
        data_standard_map_info = data_standard_map_info.join(GEO_CD_city_map, on='City', how='left') \
                                                    .join(sifutuo_ratio_map, on='Date', how='left')
    
    # XLT 多了一些匹配信息
    if project_name == 'XLT':        
        # 1. 省市标准化
        Province_City_map = spark.read.csv(Province_City_map_path, header=True)
        data_standard_map_info = data_standard_map_info.join(Province_City_map.select('Servier_Province','XLT_Province').distinct(), 
                                                            data_standard_map_info.Province == Province_City_map.XLT_Province, how='left') \
                                               .join(Province_City_map.select('Servier_City','XLT_City').distinct(), 
                                                            data_standard_map_info.City == Province_City_map.Servier_City, how='left')
        data_standard_map_info = data_standard_map_info.withColumn('Province', func.when(~col('Servier_Province').isNull(), col('Servier_Province')) \
                                                                                   .otherwise(col('Province'))) \
                                                       .withColumn('City', func.when(~col('XLT_City').isNull(), col('XLT_City')) \
                                                                                   .otherwise(col('City'))) \
                                                        .drop('Servier_Province','XLT_Province', 'Servier_City','XLT_City')
        # 2. 匹配区域
        # Region_map = spark.read.csv(Region_map_path, header=True)    
        # Region_map = Region_map.withColumnRenamed('省份', 'Province') \
        #                        .select('Province', '区域').distinct()
    
        china_Region_map = spark.read.csv(china_Region_map_path, header=True)
        china_Region_map = china_Region_map.withColumnRenamed('省份', 'Province') \
                                            .select('南北中国', '区域','Province').distinct()
        
        # 3. PTD匹配
        PTD_map = spark.read.csv(PTD_map_path, header=True)
        PTD_map = PTD_map.select('商品名+SKU', 'PTD系数').distinct() \
                        .withColumnRenamed('商品名+SKU', 'Prod_Name')
        
        data_standard_map_info = data_standard_map_info.withColumn('Prod_Name', func.regexp_replace("Prod_Name", "\\|", "")) \
                                                        .join(china_Region_map, on='Province', how='left') \
                                                        .join(PTD_map, on='Prod_Name', how='left')
        
        # 输出没匹配到PTD的条目
        noPTD = data_standard_map_info.where(col('PTD系数').isNull()).select("Molecule", "标准商品名", "Prod_Name", "PTD系数").distinct()
        if noPTD.count() > 0:
            noPTD = noPTD.repartition(1)
            noPTD.write.format("csv").option("header", "true") \
                .mode("overwrite").save(out_noPTD_path)
            
        
    # Servier 多了一些匹配信息
    if project_name == 'Servier':
        ATC_molecule_map = spark.read.csv(ATC_molecule_path, header=True)
        ATC_molecule_map = ATC_molecule_map.withColumnRenamed('S_Molecule.Name', '通用名') \
                                            .select('通用名', 'ATC3编码')
        
        data_standard_map_info = data_standard_map_info.join(ATC_molecule_map, on='通用名', how='left')
        
    # Astellas 多了一些匹配信息
    if project_name == 'Astellas':
        # 匹配标准化省市名称
        City_map = spark.read.csv(City_path, header=True)
        Province_map = spark.read.csv(Province_path, header=True)
        data_standard_map_info = data_standard_map_info.join(City_map.dropDuplicates(['City']), on='City', how='left') \
                                                    .join(Province_map.dropDuplicates(['Province']), on='Province', how='left')
        
        data_standard_map_info = data_standard_map_info.drop('City', 'Province') \
                                                    .withColumnRenamed('标准_市', 'City') \
                                                    .withColumnRenamed('标准_省', 'Province')
        
    # NHWA 多了一些匹配信息
    if project_name == 'NHWA':  
        # 魔方信息
        mofang_map = spark.read.csv(mofang_map_path, header=True)
        mofang_map = mofang_map.withColumn('mofang_prod', func.concat(col('药品名称'), col('商品名'), col('剂型'),
                                                                       col('规格'), col('生产企业'))) \
                                .withColumn('mofang_prod', func.regexp_replace("mofang_prod", " ", "")) \
                                .join(product_map.select('mofang_prod', 'mofang_prod_2').dropDuplicates(['mofang_prod']), 
                                      on='mofang_prod', how='left') \
                                .select('mofang_prod_2', '药品索引').distinct()
        # city_tier 城市
        city_tier_1 = spark.read.csv(city_tier_1_path, header=True)
        city_tier_1 = city_tier_1.select('City Tier 2010', 'Prefecture').distinct() \
                                .withColumn('City Tier 2010', city_tier_1['City Tier 2010'].cast(IntegerType())) \
                                .withColumnRenamed('Prefecture', 'City') \
                                .withColumnRenamed('City Tier 2010', 'City_Tier')
        
        # join
        data_standard_map_info = data_standard_map_info.join(mofang_map, on='mofang_prod_2', how='left') \
                                                    .join(city_tier_1, on='City', how='left')
        
        City_list_5 = ["省直辖县级行政单位","甘南州","毕节市","吐鲁番市","海东市","自治区直辖县级行政单位","哈密市","铜仁市","日喀则市","昌都市"]
        data_standard_map_info = data_standard_map_info.withColumn('City_Tier', 
                                                                func.when(col('City').isin(City_list_5), func.lit(5)).otherwise(col('City_Tier'))) \
                                                        .withColumn('City_Tier', 
                                                                func.when(col('City').isin("襄阳市"), func.lit(4)).otherwise(col('City_Tier')))

    # %%
    # ===== 4.groupby 以及列名重命名  =====
    
    # 通用
    if project_name != 'NHWA':
        # groupby 计算
        if project_name == 'Servier':
            max_standard_delivery_out = data_standard_map_info.groupby(["Date", "Province", "City", "Molecule", "标准商品名", "标准剂型", "标准规格", 
                                                           "标准包装数量", "标准生产企业", "Prod_Name", "DOI", "Route", "ATC3编码"]) \
                                                .agg(func.sum('Sales').alias('金额'), func.sum('Units').alias('数量'))
        elif project_name == 'XLT':
            max_standard_delivery_out = data_standard_map_info.groupby(["Date", "Province", "City", "Molecule", "标准商品名", "标准剂型", "标准规格", 
                                                           "标准包装数量", "标准生产企业", "Prod_Name",
                                                            "区域", "南北中国", "PTD系数"]) \
                                                .agg(func.sum('Sales').alias('金额'), func.sum('Units').alias('数量'))
        elif project_name == 'Pfizer':
            max_standard_delivery_out = data_standard_map_info.groupby(["Date", "Province", "City", "Molecule", "标准商品名", "标准剂型", "标准规格", 
                                                           "标准包装数量", "标准生产企业", "Prod_Name",
                                                            "ratio", "GEO_CD", "PACK_ID"]) \
                                                .agg(func.sum('Sales').alias('金额'), func.sum('Units').alias('数量'))
        elif project_name == 'Takeda':
            max_standard_delivery_out = data_standard_map_info.groupby(["Date", "Province", 
                                                            "Molecule", "标准商品名", "标准剂型", "标准规格", 
                                                           "标准包装数量", "标准生产企业", "Prod_Name", "PACK_ID", 
                                                            "Molecule_EN", "Prd_desc", "Form_EN", "Size_EN", "PckSize_Desc", "Mnf_Desc", "MNF_TYPE"]) \
                                                .agg(func.sum('Sales').alias('金额'), func.sum('Units').alias('数量'))
            
        else:
            max_standard_delivery_out = data_standard_map_info.groupby(["Date", "Province", "City", "Molecule", "标准商品名", "标准剂型", "标准规格", 
                                                           "标准包装数量", "标准生产企业", "Prod_Name", "DOI", "Route"]) \
                                                .agg(func.sum('Sales').alias('金额'), func.sum('Units').alias('数量'))
    
        # 列名重命名
        if project_name == 'Servier':
            rename_list = {'Date':'Period', 'City':'City', 'Province':'Province', 
                           'Molecule':'Molecule_Name', '标准商品名':'Product_Name', 
                          '标准剂型':'Dosage', '标准规格':'Pack', '标准包装数量':'Pack_Number', '标准生产企业':'manufacturer',
                          'DOI':'Market', '金额':'金额', '数量':'最小制剂单位数量', 
                            'Prod_Name':'Prod_Name', 'Route':'ROAD'}
        
        elif project_name == 'Pfizer':
            rename_list = {'Date':'Period_Code', 'City':'城市', 'Province':'省份', 
                           'Molecule':'通用名', '标准商品名':'商品名', 
                          '标准剂型':'剂型', '标准规格':'规格', '标准包装数量':'包装数量', '标准生产企业':'生产企业',
                           '金额':'LC', '数量':'SU', 
                            "PACK_ID":"Pack_ID", "GEO_CD":'Geography_id'}
            
        elif project_name == 'XLT':
            rename_list = {'Date':'Date', 'City':'城市', 'Province':'省份', 
                           'Molecule':'通用名', '标准商品名':'商品名', 
                          '标准剂型':'剂型', '标准规格':'规格', '标准包装数量':'包装数量', '标准生产企业':'生产企业',
                           '金额':'销售金额', '数量':'销售数量(片)', 
                            'Prod_Name':'商品名+SKU'}
            
        elif project_name == 'Astellas':
            rename_list = {'Date':'YearMonth', 'City':'City', 'Province':'Province', 
                           'Molecule':'Molecule_CN', '标准商品名':'BrandName', 
                          '标准剂型':'Formulation', '标准规格':'Specification', '标准包装数量':'PackageNo', '标准生产企业':'Manufacture',
                          'DOI':'MarketName_CN', '金额':'Predicted_Sales', '数量':'Predicted_Units'}
            
        elif project_name == 'Takeda':
            rename_list = {'Province':'省份', 
                           'Molecule_EN':'通用名', 'Prd_desc':'商品名', 
                          'Form_EN':'剂型', 'Size_EN':'规格', 'PckSize_Desc':'包装数量', 'Mnf_Desc':'生产企业'}
        else:
            # 通用列名
            rename_list = {'Date':'年月', 'City':'城市', 'Province':'省份', 
                           'Molecule':'通用名', '标准商品名':'商品名', 
                          '标准剂型':'剂型', '标准规格':'规格', '标准包装数量':'包装数量', '标准生产企业':'生产企业',
                          'DOI':'市场名', 'Route':'给药途径'}
            
        for old_name, newname in rename_list.items():
            max_standard_delivery_out = max_standard_delivery_out.withColumnRenamed(old_name, newname)

    # %%
    # ===== 5.最终交付处理  =====
    if project_name == 'Takeda':
        # 城市名修改
        @udf(StringType())
        def change_Q(date):
            if date[4:6] in ['01', '02', '03']:
                datenew = date[0:4] + 'Q1'
            elif date[4:6] in ['04', '05', '06']:
                datenew = date[0:4] + 'Q2'
            elif date[4:6] in ['07', '08', '09']:
                datenew = date[0:4] + 'Q3'
            elif date[4:6] in ['10', '11', '12']:
                datenew = date[0:4] + 'Q4'
            return datenew
        
        df1 = max_standard_delivery_out.withColumn('季度', change_Q(col('Date').cast(StringType()))) \
                                    .withColumnRenamed('金额', '金额(元)') \
                                    .withColumnRenamed('数量', '数量(支/片)')
        
        out = df1.select('季度', '省份', "通用名", "商品名", "剂型", "规格", "生产企业", "MNF_TYPE", "包装数量", "金额(元)", "数量(支/片)")
        
        out = out.repartition(1)
        out.write.format("csv").option("header", "true") \
            .mode("overwrite").save(out_path)

    # %%
    if project_name == 'Pfizer':
        df1 = max_standard_delivery_out.withColumn('UN', func.round(col('SU')/col('包装数量'), 0)) \
                                    .groupby('Period_Code', 'Geography_id', 'Pack_ID', 'ratio') \
                                    .agg(func.sum('LC').alias('LC'), func.sum('SU').alias('SU'), func.sum('UN').alias('UN'))
        
        df1 = df1.withColumn('LCD', func.lit(0)) \
                .withColumn('CU', func.lit(0)) \
                .withColumn('Channel_id', func.lit('P00'))
        
        # 调整思福妥
        df_sifutuo = df1.where(col('Pack_ID') == '9733702')
        df_sifutuo = df_sifutuo.withColumn('LC', func.round(col('LC') * col('ratio'), 2)) \
                                .withColumn('SU', func.round(col('SU') * col('ratio'), 0)) \
                                .withColumn('UN', func.round(col('UN') * col('ratio'), 0))
        
        df_others = df1.where( col('Pack_ID') != '9733702')
        
        df_all = df_others.union(df_sifutuo.select(df_others.columns))
        
        # 去掉一些条目
        out = df_all.where(~col('Pack_ID').isNull()).where(~col('Geography_id').isNull()) \
                    .where(~col('Geography_id').isin('ROC')) \
                    .drop('ratio')
        
        out = out.repartition(1)
        out.write.format("csv").option("header", "true") \
                .mode("overwrite").save(out_path)

    # %%
    if project_name == 'XLT':
        df1 = max_standard_delivery_out.withColumn('市场', func.lit('高血压市场')) \
                                        .withColumn('年', func.substring(col('Date'), 0, 4).cast(IntegerType())) \
                                        .withColumn('月', func.substring(col('Date'), 5, 2).cast(IntegerType())) \
                                        .withColumn('销售数量(盒)', col('销售数量(片)')/col('包装数量')) \
                                        .withColumn('销售数量(盒)', func.round(col('销售数量(盒)'), 0).cast(IntegerType()))
        
        df2 = df1.where(col('商品名') != 'Others') \
                .withColumn('区域', func.when(col('区域').isin('华东1区'), func.lit('华东区')).otherwise(col('区域'))) \
                .withColumn('区域', func.when(col('区域').isin('华东2区'), func.lit('华中区')).otherwise(col('区域')))
        
        out = df2.withColumn('商品名', func.when(col('商品名') == col('通用名'), func.concat_ws('+', col('通用名'), col('生产企业'))) \
                                        .otherwise(col('商品名'))) \
                .drop('生产企业')
        
        out = out.repartition(1)
        out.write.format("csv").option("header", "true") \
            .mode("overwrite").save(out_path)
        

    # %%
    if project_name == 'Servier':
        df1 = max_standard_delivery_out.withColumn('Prod_Name', func.regexp_replace('Prod_Name', 'NA', '')) \
                                    .withColumn('Prod_Name', func.regexp_replace('Prod_Name', '分散片', '片剂'))
        
        df1 = df1.withColumn('Product_Name', func.when(col('Product_Name') == '亚旭', func.lit('曲美他嗪—远大医药')).otherwise(col('Product_Name')))
        
        @udf(StringType())
        def for_MarketValue(market, molname, ATC, sales):
            c07 = ['比索洛尔', '美托洛尔', '普萘洛尔', '卡维地洛', '阿替洛尔', '阿罗洛尔', '拉贝洛尔', '索他洛尔', '塞利洛尔']
            c08 = ['氨氯地平', '硝苯地平', '左旋氨氯地平', '非洛地平', '西尼地平', '尼卡地平', '尼群地平', '拉西地平', 
                   '贝尼地平', '乐卡地平', '马尼地平', '阿折地平']
            if market == 'IHD':
                if molname in c07:
                    value = sales * 0.25
                elif molname in c08:
                    value = sales * 0.2
                else:
                    value = sales
            elif market == 'CHF':
                if ATC in ['C01A']:
                    value = sales * 0.5
                elif ATC in ['C03A']:
                    value = sales * 0.2
                elif molname in ['卡维地洛', '卡替洛尔']:
                    value = sales * 0.06
                else:
                    value = sales
            else:
                value = sales
            return value
        
        all_result = df1.withColumn('MarketValue', for_MarketValue(col('Market'), col('Molecule_Name'), col('ATC3编码'), col('金额'))) \
                .where( ~((col('Molecule_Name') == '卡维地洛') & (col('Market') == 'IHD')) )
        
        # [1] "贝那普利"      "福辛普利"      "卡托普利"      "赖诺普利"      "雷米普利"     
        # [6] "咪达普利"      "培哚普利"      "依那普利"      "依那普利/叶酸"
        PLAIN_s = all_result.where(col('ATC3编码') == 'C09A') \
                    .withColumn('MarketValue', col('金额') * 0.04) \
                    .withColumn('Market', func.lit('CHF'))
        # 没有卡维地洛
        CHF_s = all_result.where(col('ATC3编码') == 'C07A').where(col('Market') == 'IHD') \
                    .withColumn('MarketValue', col('金额') * 0.06) \
                    .withColumn('Market', func.lit('CHF')) 
        # 只卡维地洛
        IHD_s = all_result.where(col('ATC3编码') == 'C07A').where(col('Market') == 'CHF') \
                    .withColumn('MarketValue', col('金额') * 0.25) \
                    .withColumn('Market', func.lit('IHD'))
        
        all_result_f = all_result.union(PLAIN_s).union(CHF_s).union(IHD_s)
        all_result_f = all_result_f.where( (col('Molecule_Name').isin(['利拉鲁肽','艾塞那肽'])) | (col('ROAD').isin(['口服','口腔给药'])) | 
                                           (col('Market').isin(['CHF','ALVENOR'])) )
        
        std_names = ['Period', 'Province', 'City', 'Molecule Name', 'ATC3编码', 'Product Name', 
                   'manufacturer', 'Pack', 'Pack Number', 'Dosage', 'ROAD', '金额', 
                   '最小制剂单位数量', 'Market', 'MarketValue']
        
        out = all_result_f.toDF(*(re.sub(r'[\_\s]+', ' ', c) for c in all_result_f.columns)) \
                        .select(std_names)
        
        out = out.repartition(1)
        out.write.format("csv").option("header", "true") \
                .mode("overwrite").save(out_path)
        

    # %%
    if project_name in ['贝达', '神州', '康哲', '京新']:
        out = max_standard_delivery_out.select("年月", "省份", "城市", "通用名", "商品名", "剂型", "规格", 
                                                           "包装数量", "生产企业", "金额", "数量")    
        out = out.repartition(1)
        out.write.format("csv").option("header", "true") \
            .mode("overwrite").save(out_path)

    # %%
    if project_name == '奥鸿':
        df = max_standard_delivery_out.select("年月", "省份", "城市", "Prod_Name", "通用名", "商品名", "剂型", "规格", 
                                                               "包装数量", "生产企业", "金额", "数量")
    
        province_city_mapping = spark.read.parquet(province_city_mapping_path)
        province_city_mapping = deal_ID_length(province_city_mapping)
        province_city_mapping = province_city_mapping.select('ID', 'Province', 'City').distinct()
    
        raw_data = spark.read.parquet(raw_data_path)
        raw_data = deal_ID_length(raw_data)
        raw_data_info = raw_data.where((col('year_month') >= time_left) & (col('year_month') <= time_right)) \
                            .drop('Province', 'City') \
                            .join(province_city_mapping, on='ID', how='left') \
                            .select('year_month', 'Province', 'min2') \
                            .withColumn('f_raw', func.concat_ws('+', col('year_month'), col('Province'), col('min2'))) \
                            .select('f_raw').distinct()
        ## 这四个省样本量不足 删掉邦亭，其他省取样本有的所有产品
        province_delete = ['甘肃省','广西壮族自治区','青海省','西藏自治区']
    
        df2 = df.withColumn('f_max', func.concat_ws('+', col('年月'), col('省份'), col('Prod_Name')))
        df3 = df2.join(raw_data_info, df2.f_max == raw_data_info.f_raw, how='left')
        df4 = df3.withColumn('delete', func.when(~(col('f_raw').isNull()) | (col('省份').isin(province_delete) & col('Prod_Name').contains('邦亭')), 
                                                  func.lit(0)).otherwise(func.lit(1)))
    
        out1 = df4.where(col('delete') == 0).drop('f_raw', 'f_max', 'delete')
        out2 = df4.where(col('delete') == 1).drop('f_raw', 'f_max', 'delete')
        
        out1 = out1.repartition(1)
        out1.write.format("csv").option("header", "true") \
            .mode("overwrite").save(out_path)
        
        out2 = out2.repartition(1)
        out2.write.format("csv").option("header", "true") \
            .mode("overwrite").save(out2_path)

    # %%
    if project_name == '海坤':
        out = max_standard_delivery_out.select("年月", "省份", "城市", "通用名", "商品名", "剂型", "规格", 
                                                               "包装数量", "生产企业", "金额", "数量")
        # 城市名修改
        @udf(StringType())
        def city_change(name):
            # 城市名统一
            dict = {"毕节市":"毕节地区", "哈密市":"哈密地区", "日喀则市":"日喀则地区", "楚雄彝族自治州":"楚雄市"}
            if name in dict.keys():
                newname = dict[name]
            else:
                newname = name
            return newname
        
        out = out.withColumn('城市', city_change(col('城市')))
        
        out = out.repartition(1)
        out.write.format("csv").option("header", "true") \
            .mode("overwrite").save(out_path)

    # %%
    if project_name == '汇宇':
        out = max_standard_delivery_out.select("年月", "省份", "城市", "通用名", "商品名", "剂型", "规格", 
                                                               "包装数量", "生产企业", "金额", "数量")
        out = out.where(~col('通用名').isin('多西他赛','培美曲塞'))
        
        out = out.repartition(1)
        out.write.format("csv").option("header", "true") \
            .mode("overwrite").save(out_path)

    # %%
    if project_name == 'Tide':
        out = max_standard_delivery_out.select("市场名", "年月", "省份", "城市", "通用名", "商品名", "剂型", "规格", 
                                                               "包装数量", "生产企业", "金额", "数量")
        # 城市名修改
        @udf(StringType())
        def city_change(name):
            # 城市名统一
            dict = {"毕节市":"毕节地区", "哈密市":"哈密地区", "日喀则市":"日喀则地区", "楚雄彝族自治州":"楚雄市"}
            if name in dict.keys():
                newname = dict[name]
            else:
                newname = name
            return newname
        
        @udf(StringType())
        def province_change(name):
            # 城市名统一
            dict = {"内蒙":"内蒙古", "黑龙":"黑龙江"}
            if name in dict.keys():
                newname = dict[name]
            else:
                newname = name
            return newname
        
        out = out.withColumn('城市', city_change(col('城市'))) \
                .withColumn('省份', func.substring(col('省份'), 0, 2)) \
                .withColumn('省份', province_change(col('省份'))) \
                .withColumn('市场名', func.when((col('通用名') == '氟比洛芬') & (col('剂型') == '注射剂'), func.lit('凯纷')).otherwise(col('市场名')))
        
        out = out.repartition(1)
        out.write.format("csv").option("header", "true") \
            .mode("overwrite").save(out_path)

    # %%
    if project_name == 'Gilead':
        df = max_standard_delivery_out.select("年月", "省份", "城市", "通用名", "商品名", "剂型", "规格", 
                                                               "包装数量", "生产企业", "金额", "数量", "给药途径")
        df = df.withColumnRenamed('金额', '销售金额') \
                .withColumnRenamed('数量', '销售数量（最小单位:片支）') \
        
        corp_mapping = spark.read.parquet(corp_mapping_path)
        corp_mapping = corp_mapping.where(~col('Corp_STD_EN').isNull()) \
                                    .select(corp_mapping.columns[0:3]).distinct() \
                                    .withColumnRenamed('Corporation', '生产企业')
        
        brand_mapping = spark.read.parquet(brand_mapping_path)
        brand_mapping = brand_mapping.where(~col('Molecue_STD_EN').isNull()) \
                                    .select(brand_mapping.columns[1:8]).distinct() \
                                    .withColumnRenamed('DrugName_Molecue', '通用名') \
                                    .withColumnRenamed('ProductName', '商品名') \
                                    .withColumnRenamed('Corp_CN', '生产企业')
        
        df2 = df.join(brand_mapping, on=['通用名', '商品名', '生产企业'], how='left') \
               .join(corp_mapping, on=['生产企业'], how='left')
        
        @udf(StringType())
        def Corp_change(name, corp, brand, std):
            # 公司名修改    
            if corp == '株洲千金药业股份有限公司' and brand == '艾普丁':
                if std == 'EN':
                    newname = 'HN.QIANJIN XIELI'
                elif std == 'CN':
                    newname = '湖南千金协力药业有限公司'
            elif corp == '株洲千金药业股份有限公司' and (brand in ['健甘灵','乾力安']):
                if std == 'EN':
                    newname = 'QIANJINXIANGJIANG'
                elif std == 'CN':
                    newname = '湖南千金湘江药业股份有限公司'
            else:
                newname = name
            return newname
        
        out = df2.withColumn('Corp_STD_EN', Corp_change(col('Corp_STD_EN'), col('生产企业'), col('商品名'), func.lit('EN'))) \
                .withColumn('Corp_STD_CN', Corp_change(col('Corp_STD_CN'), col('生产企业'), col('商品名'), func.lit('CN')))
        
        out = out.select("年月", "省份", "城市", "通用名", "商品名", "剂型", "规格", "包装数量", "生产企业", "销售金额", "销售数量（最小单位:片支）", 
                         "给药途径", "Molecue_STD_EN", "Molecue_STD_CN", "Brand_STD_EN", "Brand_STD_CN", "Corp_STD_EN", "Corp_STD_CN")
        
        out = out.repartition(1)
        out.write.format("csv").option("header", "true") \
            .mode("overwrite").save(out_path)
            

    # %%
    if project_name == 'Qilu':
        out = max_standard_delivery_out.select("年月", "省份", "城市", "通用名", "商品名", "剂型", "规格", 
                                                           "包装数量", "生产企业", "金额", "数量")
        out = out.where( ~ ((col('通用名')=='替诺福韦二吡呋酯') & (col('省份')=='西藏')))
        
        out = out.repartition(1)
        out.write.format("csv").option("header", "true") \
            .mode("overwrite").save(out_path)

    # %%
    if project_name == 'NHWA':
        df = data_standard_map_info.withColumnRenamed('Date', '时间') \
                                                        .withColumnRenamed('Province', '省份') \
                                                        .withColumnRenamed('City', '城市') \
                                                        .withColumnRenamed('Molecule', '分子名') \
                                                        .withColumnRenamed('标准商品名', '商品名_标准') \
                                                        .withColumnRenamed('标准剂型', '剂型') \
                                                        .withColumnRenamed('标准规格', '规格') \
                                                        .withColumnRenamed('标准包装数量', '包装数量') \
                                                        .withColumnRenamed('标准生产企业', '生产企业_标准') \
                                                        .withColumnRenamed('DOI', '市场名') \
                                                        .withColumnRenamed('City_Tier_2010', '给药途径') \
                                                        .withColumnRenamed('Sales', '销售金额') \
                                                        .withColumnRenamed('Units', '销售数量')
    
        df2 = df.withColumn('省', func.regexp_replace("省份", "省|回族自治区|壮族自治区|维吾尔族自治区|维吾尔自治区|自治区|市", ""))
        # 根据分子名定义ACC1/ACC2
        df2 = df2.withColumn('ACC1/ACC2', for_ACC(col('分子名')))
        # 根据省和ACC1/ACC2定义区域
        df2 = df2.withColumn('区域', for_region(col('省'), col('ACC1/ACC2')))
    
        df2 = df2.withColumn('市场I', func.lit("麻醉市场")) \
                    .withColumn('市场II', func.lit(None).cast(StringType())) \
                    .withColumn('市场III', func.lit(None).cast(StringType())) \
                    .withColumn('商品名_标准', func.when(col('商品名_标准') == "乐维伽", func.lit("右美宁")).otherwise(col('商品名_标准'))) \
                    .fillna(0, '毫克数') \
                    .withColumn('销售毫克数', col('销售数量')*col('毫克数')) \
                    .withColumn('power', func.concat(col('分子名'), col('区域'))) \
                    .withColumn('年', func.substring(col('时间'), 0, 4)) \
                    .withColumn('月', func.substring(col('时间'), 5, 2))
    
        out = df2.groupby(["时间", "城市", "min2", "商品名+SKU", "商品名_标准", "生产企业_标准", "年", "月", "省份", "City_Tier",
                        "市场I", "区域", "分子名", "ACC1/ACC2", "power", "规格", "剂型", "包装数量", "药品索引"]) \
                .agg(func.sum('销售数量').alias('销售数量'), func.sum('销售金额').alias('销售金额'), func.sum('销售毫克数').alias('销售毫克数'))
    
        out = out.repartition(1)
        out.write.format("csv").option("header", "true") \
            .mode("overwrite").save(out_path)
    
        out2 = out.groupby(["商品名+SKU", "商品名_标准","年", "月", "市场I", "区域", "分子名", "规格", "剂型"]) \
                .agg(func.sum('销售数量').alias('销售数量'), func.sum('销售金额').alias('销售金额'), func.sum('销售毫克数').alias('销售毫克数'))
     
        out2 = out2.repartition(1)
        out2.write.format("csv").option("header", "true") \
            .mode("overwrite").save(out2_path)

    # %%
    if project_name == 'Astellas':
        df = max_standard_delivery_out.select("YearMonth", "Province", "City", "Molecule_CN", "BrandName", "Formulation", "Specification", 
                                                           "PackageNo", "Manufacture", 'MarketName_CN', "Predicted_Sales", "Predicted_Units")
        
        df1 = df.withColumn('BrandName', func.when((col('BrandName') == '哈乐') & (col('Formulation') == '片剂') & (col('PackageNo') == 14), 
                                                 func.lit('新哈乐')).otherwise(col('BrandName'))) \
                .withColumn('BrandName', func.when((col('BrandName') == '新哈乐') & (col('Formulation') == '片剂') & (col('PackageNo') == 10), 
                                                 func.lit('哈乐')).otherwise(col('BrandName')))
        
        df2 = df1.where( ~( (~col('Formulation').isin('粉针剂', '注射剂')) & (col('MarketName_CN').isin('米开民市场', '佩尔市场')) ) & 
                         ~( (col('Formulation').isin('粉针剂', '注射剂', '滴眼剂','气雾剂','喷雾剂')) & (col('MarketName_CN').isin('阿洛刻市场')) ) &
                        ~( (col('Formulation').isin('滴眼剂')) & (col('MarketName_CN').isin('普乐可复市场')) ) & 
                        ~( col('BrandName').isin('保法止')) )
        
        df3 = df2.where(~col('BrandName').isin("倍他司汀", "阿魏酰γ-丁二胺/植物生长素", "丙磺舒", "复方别嘌醇"))
        
        df4 = df3.where(~( (~col('BrandName').isin('可多华', '保列治', '高特灵', '贝可', '得妥', '宁通', '舍尼亭', '托特罗定')) & 
                          (col('Molecule_CN').isin('多沙唑嗪', '特拉唑嗪', '非那雄胺', '托特罗定')) ))
        
        df5 = df4.withColumn('MarketName_CN', func.when(col('MarketName_CN') == '白血病市场', func.lit('急性髓系白血病市场')) \
                                                 .otherwise(col('MarketName_CN')))
        
        out = df5.withColumn('Quarter', func.concat(func.substring('YearMonth', 0, 4), func.lit('Q') ,func.ceil(func.substring('YearMonth', 5, 2)/3) ))
        
        out = out.repartition(1)
        out.write.format("csv").option("header", "true") \
            .mode("overwrite").save(out_path)

    # %%
    #out.count()

    # %%
    #out.agg(func.sum('金额'), func.sum('最小制剂单位数量')).collect()

    # %%
    #out.agg(*[func.count(func.when(func.isnull(c), c)).alias(c) for c in out.columns]).show()

    # %%
    #out.show(5)

    # %%

