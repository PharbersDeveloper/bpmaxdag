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

    
    
    # from phcli.ph_logs.ph_logs import phs3logger
    # from pyspark.sql.types import *
    from pyspark.sql.types import StringType, IntegerType, DoubleType
    from pyspark.sql import functions as func
    import os
    from pyspark.sql.functions import pandas_udf, PandasUDFType, col, udf
    from pyspark.sql import Window    
    # %%
    
    # project_name = "XLT"
    # time_left = "202001"
    # time_right = "202012"
    # out_dir = "202012"
    

    # %%
    # 样本数据交付

    # %%
    # ==========  输入输出  ============
    # 输入
    time_left = int(time_left)
    time_right = int(time_right)
    
    CPA_GYC_hospital_map_path = max_path + "/Common_files/CPA_GYC_hospital_map"
    HAIHONG_hospital_map_path = max_path + "/Common_files/HAIHONG_hospital_map"
    
    product_map_path = max_path + "/" + project_name + "/" + out_dir + "/prod_mapping"
    province_city_mapping_path = max_path + '/' + project_name + '/province_city_mapping'
    cpa_pha_mapping_path = max_path + '/' + project_name + '/cpa_pha_mapping'
    raw_standard_path = extract_path + "/" + project_name + "_rawdata_standard"
    
    if project_name == 'NHWA':
        mofang_map_path = max_path + '/NHWA/FOR_delivery_files/恩华药品对照表' + out_dir[2:] + '.csv'
        city_tier_path = max_path + '/NHWA/FOR_delivery_files/城市级别匹配.csv'
        CPA_GYC_hospital_map_old_path = max_path + "/Common_files/CPA_GYC_hospital_map_old.csv"
    if project_name == 'XLT':
        Hospital_id_map_path = max_path + '/XLT/FOR_delivery_files/Hospital_id_map.csv'
        # Region_map_path = max_path + '/XLT/FOR_delivery_files/Region_map.csv'
        Province_City_map_path = max_path + '/XLT/FOR_delivery_files/信立泰省市匹配表.csv'
        china_Region_map_path = max_path + '/XLT/FOR_delivery_files/账号权限省份Mapping.csv'
        PTD_map_path = max_path + '/XLT/FOR_delivery_files/PTD_map.csv'   
    
    # 输出
    time_range = str(time_left) + '_' + str(time_right)
    out_path = max_path + "/" + project_name + "/" + out_dir + "/Delivery/" + project_name + "_raw_delivery_" + time_range + '.csv'
    if project_name == 'XLT':
        out_noPTD_path = max_path + "/" + project_name + "/" + out_dir + "/Delivery/待清洗PTD系数.csv"
    # tmp_path = max_path + "/" + project_name + "/" + out_dir + "/Delivery/tmp"

    # %%
    ## 数据执行

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
    
    # 1. hospital_map：ID - 医院名称
    # 1.1 CPA_GYC_hospital_map
    CPA_GYC_hospital_map0 = spark.read.parquet(CPA_GYC_hospital_map_path)
    CPA_GYC_hospital_map0 = CPA_GYC_hospital_map0.select("医院编码", "医院名称", "等级" ,"标准化省份", "标准化城市", "新版ID", "Source").distinct() \
                                .withColumnRenamed("医院编码", "ID")
    CPA_GYC_hospital_map0 = CPA_GYC_hospital_map0.withColumn('ID', col('ID').cast(IntegerType()))
    # 优先选择cpa医院名
    id_window = Window.partitionBy('新版ID').orderBy(col('ID'))
    CPA_GYC_hospital_map1 = CPA_GYC_hospital_map0.withColumn('rank',func.row_number().over(id_window))
    CPA_GYC_hospital_map2 = CPA_GYC_hospital_map1.where(col('rank') == 1).select('新版ID', '医院名称') \
                                .withColumnRenamed("医院名称", "统一名称")
    CPA_GYC_hospital_map3 = CPA_GYC_hospital_map1.join(CPA_GYC_hospital_map2, on='新版ID', how='left')
    CPA_GYC_hospital_map = CPA_GYC_hospital_map3.select('ID', '统一名称', '等级').distinct()
    CPA_GYC_hospital_map = deal_ID_length(CPA_GYC_hospital_map)
    
    # 1.2 HAIHONG_hospital_map 海虹编码
    HAIHONG_hospital_map = spark.read.parquet(HAIHONG_hospital_map_path)
    HAIHONG_hospital_map = HAIHONG_hospital_map.select('医院编码', '医院名称', '等级').distinct() \
                                            .withColumnRenamed("医院编码", "ID") \
                                            .withColumnRenamed("医院名称", "统一名称")
    
    # 1.3 合并
    hospital_map = CPA_GYC_hospital_map.union(HAIHONG_hospital_map)
    
    # 2. hospital_map：ID - 新版名称
    CPA_GYC_hospital_map0 = spark.read.parquet(CPA_GYC_hospital_map_path)
    hospital_map_newname = CPA_GYC_hospital_map0.select("医院编码", "新版名称", '等级') \
                                .withColumnRenamed("医院编码", "ID") \
                                .withColumnRenamed("新版名称", "统一名称") \
                                .dropDuplicates(['ID'])

    # %%
    # 2. province_city_mapping ：ID - 匹配省市
    province_city_mapping = spark.read.parquet(province_city_mapping_path)
    province_city_mapping = deal_ID_length(province_city_mapping)
    province_city_mapping = province_city_mapping.select('ID', 'Province', 'City').distinct()

    # %%
    # 3. cpa_pha_mapping ：ID - 匹配PHA
    cpa_pha_mapping = spark.read.parquet(cpa_pha_mapping_path)
    cpa_pha_mapping = cpa_pha_mapping.where(cpa_pha_mapping["推荐版本"] == 1) \
            .select("ID", "PHA").distinct()
    cpa_pha_mapping = deal_ID_length(cpa_pha_mapping)

    # %%
    # 4. product_map 文件
    product_map = spark.read.parquet(product_map_path)
    # a. 列名清洗统一
    # 有的min2结尾有空格与无空格的是两条不同的匹配
    if project_name == "Sanofi" or project_name == "AZ":
        product_map = product_map.withColumnRenamed(product_map.columns[21], "pfc")
    if project_name == "Eisai":
        product_map = product_map.withColumnRenamed(product_map.columns[22], "pfc")
    
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
    product_map = product_map.withColumn("pfc", product_map["pfc"].cast(IntegerType())) \
                        .withColumn("标准包装数量", product_map["标准包装数量"].cast(IntegerType())) \
                        .withColumnRenamed("pfc", "PACK_ID") \
                        .distinct()
    
    # d. pfc为0统一替换为null
    product_map = product_map.withColumn("PACK_ID", func.when(col('PACK_ID') == 0, None).otherwise(col('PACK_ID'))).distinct()
    
    # e 选取需要的列(不同项目有区别)
    if project_name == 'NHWA':
        product_map = product_map \
                    .select("min1", "PACK_ID", "通用名", "标准商品名", "标准剂型", "标准规格", "标准包装数量", "标准生产企业", 'Route', 
                           "min2", "商品名+SKU", "毫克数") 
    else:
        product_map = product_map \
                    .select("min1", "PACK_ID", "通用名", "标准商品名", "标准剂型", "标准规格", "标准包装数量", "标准生产企业", 'Route', 'min2') 

    # %%
    # ====  三.样本数据处理  ====  
    # ====  1. 提取交付数据  ====  
    
    # =====  Raw =====
    
    # 1. 读取交付数据
    if time_left//100 == time_right//100:
        monthlist = range(time_left, time_right+1, 1)
    else:
        monthlist = list(range(time_right//100*100+1, time_right+1, 1))
        years=range(time_left//100, time_right//100, 1)
        for each in years:
            monthlist.extend(range(each*100+1, each*100+13, 1))
            
    path_list = [raw_standard_path + '/Date_copy=' + str(i) for i in monthlist]
    
    index = 0
    for eachpath in path_list:
        df = spark.read.parquet(eachpath)
        if index ==0:
            data_standard = df
        else:
            data_standard = data_standard.union(df)
        index += 1

    # %%
    # ====  2. 信息匹配  ====  
    # 2. 信息匹配
    data_standard = deal_ID_length(data_standard)
    data_standard_map_info = data_standard.select("Date", "ID", "Brand", "Form", "Specifications", "Pack_Number", "Manufacturer", 
                                                  "Sales", "Units", "min1", "Molecule")
    if project_name == 'XLT':
        # XLT 用新版名称（因为后续匹配医院编号文件的需要）
        hospital_map = hospital_map_newname
    
    data_standard_map_info = data_standard_map_info.join(product_map.dropDuplicates(['min1']), on=["min1"], how='left') \
                                                    .join(hospital_map, on='ID', how='left') \
                                                    .join(province_city_mapping, on='ID', how='left') \
                                                    .join(cpa_pha_mapping, on='ID', how='left').persist()
    
    # XLT 多了一些匹配信息
    if project_name == 'XLT':
        # 5323003 没有PHA  
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
        #                         .select('Province', '区域').distinct()
    
        china_Region_map = spark.read.csv(china_Region_map_path, header=True)
        china_Region_map = china_Region_map.withColumnRenamed('省份', 'Province') \
                                            .select('南北中国', '区域','Province').distinct()
        
        # 3. PTD匹配
        PTD_map = spark.read.csv(PTD_map_path, header=True)
        PTD_map = PTD_map.select('商品名+SKU', 'PTD系数').distinct() \
                        .withColumnRenamed('商品名+SKU', 'min2')
                        
        
        data_standard_map_info = data_standard_map_info.withColumn('min2', func.regexp_replace("min2", "\\|", "")) \
                                                        .join(china_Region_map, on='Province', how='left') \
                                                        .join(PTD_map, on='min2', how='left').persist()
            
        # 4.医院编号匹配
        Hospital_id_map = spark.read.csv(Hospital_id_map_path, header=True)
        Hospital_id_map = Hospital_id_map.withColumnRenamed('Hosp_name', '统一名称').distinct()
        data_standard_map_info = data_standard_map_info.join(Hospital_id_map, on='统一名称', how='left').persist()        
            
            
    # NHWA 多了一些匹配信息        
    if project_name == 'NHWA':
        
        # 魔方信息
        mofang_map = spark.read.csv(mofang_map_path, header=True)
        mofang_map = mofang_map.withColumn('mofang_prod', func.concat(col('药品名称'), col('商品名'), col('剂型'),
                                                                       col('规格'), col('生产企业'))) \
                                .withColumn('mofang_prod', func.regexp_replace("mofang_prod", " ", "")) \
                                .select('mofang_prod', '药品索引').distinct()
        
        # 城市tier信息
        city_tier = spark.read.csv(city_tier_path, header=True)
        city_tier = city_tier.withColumnRenamed('City Name', 'City') \
                        .withColumnRenamed('City.Tier.2010', 'BMS_Segment')
        
        # 旧的医院名信息  
        CPA_GYC_hospital_map_old = spark.read.csv(CPA_GYC_hospital_map_old_path, header=True)
        CPA_GYC_hospital_map_old = CPA_GYC_hospital_map_old.groupby('CPA') \
                                        .agg(func.first('PHA Hosp name').alias('Hosp_name'), func.first('PHA ID').alias('HOSP_ID')) \
                                        .withColumnRenamed('CPA', 'ID')
        CPA_GYC_hospital_map_old = deal_ID_length(CPA_GYC_hospital_map_old)
        
        # 匹配
        data_standard_map_info = data_standard_map_info.withColumn('mofang_prod', func.concat(col('Molecule'), col('Brand'), col('Form'),
                                                                       col('Specifications'), col('Manufacturer'))) \
                                                        .withColumn('mofang_prod', func.regexp_replace("mofang_prod", " ", "")) \
                                                        .join(mofang_map, on='mofang_prod', how='left') \
                                                        .join(CPA_GYC_hospital_map_old, on='ID', how='left') \
                                                        .join(city_tier, on='City', how='left').fillna("5", 'BMS_Segment').persist()        

    # %%
    # ====  3.列名统一  ====
    # 列名统一
    if project_name == 'NHWA':
        rename_list = {'Date':'年月', 'City':'城市', 'Province':'省份', 'ID':'医院编码',
                        '通用名':'分子名', '标准商品名':'标准商品名', 
                        '标准剂型':'剂型', '标准规格':'规格', '标准包装数量':'包装数量', '标准生产企业':'生产企业_标准',
                        'Hosp_name':'医院名称', '统一名称':'医院名称II', 'PHA':'医院编码_标准',
                        'Sales':'销售金额', 'Units':'销售数量'}
    elif project_name == 'XLT':
        rename_list = {'Date':'Date', 'City':'城市', 'Province':'省份', 'ID':'ID',
                       '通用名':'通用名', '标准商品名':'商品名', 
                        '标准剂型':'剂型', '标准规格':'规格', '标准包装数量':'包装数量', '标准生产企业':'生产企业',
                        '统一名称':'Hosp_name', 'PHA':'Panel_ID',
                        'Sales':'销售金额', 'Units':'销售数量(片)', 
                        'min2':'商品名+SKU'}
    else:
        rename_list = {'Date':'年月', 'City':'城市', 'Province':'省份', 'ID':'医院编码',
                        '通用名':'通用名', '标准商品名':'商品名', 
                        '标准剂型':'剂型', '标准规格':'规格', '标准包装数量':'包装数量', '标准生产企业':'生产企业',
                        '统一名称':'医院名称', 'PHA':'PHA编码', 'Route':'给药途径',
                        'Sales':'金额', 'Units':'数量'}
    
    for old_name, newname in rename_list.items():
        data_standard_map_info = data_standard_map_info.withColumnRenamed(old_name, newname)
        
    # 列选择
    if project_name != 'NHWA':
        if project_name == 'XLT':
            data_standard_delivery_out = data_standard_map_info.select("Date","Hosp_name","ID","Panel_ID","商品名","商品名+SKU",
                          "城市","省份","区域","医院编号", "销售金额","销售数量(片)","通用名","规格", "包装数量","南北中国",'PTD系数','生产企业')
        else:
            data_standard_delivery_out = data_standard_map_info.select('年月', '省份', '城市', '医院编码', '金额', '数量', '通用名', '商品名', '剂型',
                                                          '规格', '包装数量', '生产企业', '医院名称', '等级', 'PHA编码', '给药途径')

    # %%
    # ====  4.最终交付处理  ====
    if project_name == 'XLT':
        
        # 筛选分子
        mole_list = ['阿利沙坦酯','奥美沙坦','奥美沙坦/氨氯地平','厄贝沙坦','氯沙坦','培哚普利','培哚普利/氨氯地平',
                   '缬沙坦','缬沙坦/氨氯地平','坎地沙坦','替米沙坦']
        df = data_standard_delivery_out.where(col('通用名').isin(mole_list))
        
        # 输出没匹配到PTD的条目
        noPTD = df.where(col('PTD系数').isNull()).select("通用名", "商品名", "商品名+SKU", "PTD系数").distinct()
        if noPTD.count() > 0:
            noPTD = noPTD.repartition(1)
            noPTD.write.format("csv").option("header", "true") \
                .mode("overwrite").save(out_noPTD_path)
        
        df1 = df.withColumn('市场', func.lit('高血压市场')) \
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
        
        out = out.select("Date","Hosp_name","ID","Panel_ID","商品名","商品名+SKU","城市","市场","年","月","省份","区域","医院编号",
                          "销售金额","销售数量(片)","销售数量(盒)","通用名","规格","包装数量","南北中国",'PTD系数')

    # %%
    if project_name in ['贝达', '康哲', '奥鸿', '京新', '海坤', '汇宇', 'Tide']:
        out = data_standard_delivery_out.select("年月", "省份", "城市", "医院编码", "医院名称", "等级", "通用名", "商品名", "剂型", "规格", 
                                                           "包装数量", "生产企业", "金额", "数量")
    
    if project_name in ['神州', 'Qilu']:
        out = data_standard_delivery_out.select("年月", "省份", "城市", "医院编码", "PHA编码", "医院名称", "等级", "通用名", "商品名", "剂型", "规格", 
                                                           "包装数量", "生产企业", "金额", "数量")
    
    if project_name in ['Gilead']:
        out = data_standard_delivery_out.select("年月", "省份", "城市", "医院编码", "医院名称", "等级", "通用名", "商品名", "剂型", "规格", 
                                                           "包装数量", "生产企业", "金额", "数量", "给药途径")

    # %%
    # NHWA 处理较多
    if project_name == 'NHWA':
        df = data_standard_delivery.groupby('年月', '省份', '城市', '医院编码', '分子名', '标准商品名', '剂型',
                                                          '规格', '包装数量', '生产企业_标准', '医院名称', '医院名称II', '医院编码_标准', 'min2', 
                                                           '商品名+SKU', '药品索引', 'BMS_Segment') \
                                    .agg(func.sum('销售金额').alias('销售金额'), func.sum('销售数量').alias('销售数量'), 
                                         func.sum('毫克数').alias('毫克数'))
    
        df2 = df.withColumn('省', func.regexp_replace("省份", "省|回族自治区|壮族自治区|维吾尔族自治区|维吾尔自治区|自治区|市", ""))
        # 根据分子名定义ACC1/ACC2
        df2 = df2.withColumn('ACC1/ACC2', for_ACC(col('分子名')))
        # 根据省和ACC1/ACC2定义区域
        df2 = df2.withColumn('区域', for_region(col('省'), col('ACC1/ACC2')))
        # 710133 raw是陕西 - map是北京
        # 华北一区 4684,  4725
        # 西北一区 10103,  10062
    
        df2 = df2.withColumn('市场I', func.lit("麻醉市场")) \
                .withColumn('市场II', func.lit(None).cast(StringType())) \
                .withColumn('市场III', func.lit(None).cast(StringType())) \
                .withColumn('标准商品名', func.when(col('标准商品名') == "乐维伽", func.lit("右美宁")).otherwise(col('标准商品名'))) \
                .fillna(0, '毫克数') \
                .withColumn('销售毫克数', col('销售数量')*col('毫克数')) \
                .withColumn('power', func.concat(col('分子名'), col('区域'))) \
                .withColumn('年', func.substring(col('年月'), 0, 4)) \
                .withColumn('月', func.substring(col('年月'), 4, 2))
    
        out = df2.select('医院编码', '医院编码_标准', '医院名称', 'min2', '销售金额', '销售数量', '市场I', '规格', '剂型', '包装数量', '商品名+SKU', 
                  '标准商品名', '生产企业_标准', '分子名', '毫克数', '年', '月', '省份', '城市', 'BMS_Segment', '市场II', '市场III', 'ACC1/ACC2', '区域', 
                   '销售毫克数', 'power', '医院名称II', '药品索引')

    # %%
    # ====  5.结果输出  ====
    # 结果输出
    out = out.repartition(1)
    out.write.format("csv").option("header", "true") \
           .mode("overwrite").save(out_path)

    # %%
    #out.count()

    # %%
    #out.agg(func.sum('销售金额'), func.sum('销售数量(片)'), func.sum('销售数量(盒)')).collect()

    # %%
    #out.agg(*[func.count(func.when(func.isnull(c), c)).alias(c) for c in out.columns]).show()

    # %%
    #out.show(5)

