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
    time_left = kwargs['time_left']
    time_right = kwargs['time_right']
    out_dir = kwargs['out_dir']
    if_others = kwargs['if_others']
    ### input args ###
    
    ### output args ###
    c = kwargs['c']
    d = kwargs['d']
    ### output args ###

    from pyspark.sql.types import StringType, IntegerType, DoubleType
    from pyspark.sql import functions as func
    import os
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col
    from pyspark.sql import Window    # %%
    # project_name = "贝达"
    # time_left = "202001"
    # time_right = "202012"
    # out_dir = "202012"

    # %%
    # 输入文件
    product_map_path = max_path + "/" + project_name + "/" + out_dir + "/prod_mapping"
    time_range = str(time_left) + '_' + str(time_right)
    if if_others == 'True':
        max_result_city_path = max_path + "/" + project_name + "/" + out_dir + "/others_box/MAX_result/MAX_result_" + time_range + "_city_level"
        panel_path = max_path + "/" + project_name + "/" + out_dir + "/others_box/panel_result_box"
    else:
        max_result_city_path = max_path + "/" + project_name + "/" + out_dir + "/MAX_result/MAX_result_" + time_range + "_city_level"
        panel_path = max_path + "/" + project_name + "/" + out_dir + "/panel_result"
    
    hospital_map_path = max_path + "/Common_files/CPA_GYC_hospital_map"
    ims_city_map_path = max_path + '/Common_files/IMS_mapping_citycode.csv'
    IMS_flat_files_dir = max_path + "/Common_files/IMS_flat_files/" + out_dir
    
    ims_Sales_path = IMS_flat_files_dir + '/cn_IMS_Sales_Fdata_' + out_dir + '_1.csv'
    ims_mol_lkp_path = IMS_flat_files_dir + '/cn_mol_lkp_' + out_dir + '_1.csv'
    ims_mol_ref_path = IMS_flat_files_dir + '/cn_mol_ref_' + out_dir + '_1.csv'
    
    # 输出文件
    if if_others == 'True':
        check_path = max_path + '/' + project_name + '/' + out_dir + '/others_box/max_check/'
    else:
        check_path = max_path + '/' + project_name + '/' + out_dir + '/max_check/'
    check_1_path = check_path + '/check_1_byProduct.csv'
    check_2_path = check_path + '/check_2_byCity.csv'
    check_3_path = check_path + '/check_3_补数比例.csv'
    check_4_path = check_path + '/check_4_放大比例.csv'
    check_5_path = check_path + '/check_5_产品个数.csv'
    check_6_path = check_path + '/check_6_样本医院个数.csv'

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
    
    @udf(StringType())
    def city_change(name):
        # 城市名统一
        if name in ["福州市", "厦门市", "泉州市"]:
            newname = "福厦泉"
        elif name in ["苏州市", "无锡市"]:
            newname = "苏锡城市群"
        else:
            newname = name
        return newname
    # ====  二. 数据准备  ====  
    
    # 1. prod_map 文件
    product_map = spark.read.parquet(product_map_path)
    # a. 列名清洗统一
    if project_name == "Sanofi" or project_name == "AZ":
        product_map = product_map.withColumnRenamed(product_map.columns[21], "pfc")
    if project_name == "Eisai":
        product_map = product_map.withColumnRenamed(product_map.columns[22], "pfc")
    for i in product_map.columns:
        if i in ["标准通用名", "通用名_标准", "药品名称_标准", "S_Molecule_Name"]:
            product_map = product_map.withColumnRenamed(i, "通用名")
        if i in ["packcode", "Pack_ID", "Pack_Id", "PackID", "packid"]:
            product_map = product_map.withColumnRenamed(i, "pfc")
        if i in ["min1_标准"]:
            product_map = product_map.withColumnRenamed(i, "min2")
    # b. 选取需要的列
    product_map = product_map \
                    .select("min2", "pfc", "通用名") \
                    .withColumn("pfc", product_map["pfc"].cast(IntegerType())) \
                    .distinct()
    # c. pfc为0统一替换为null
    product_map = product_map.withColumn("pfc", func.when(product_map.pfc == 0, None).otherwise(product_map.pfc)).distinct()
    # d. min2处理
    product_map = product_map.withColumnRenamed("pfc", "Pack_ID") \
                    .withColumn("min2", func.regexp_replace("min2", "&amp;", "&")) \
                    .withColumn("min2", func.regexp_replace("min2", "&lt;", "<")) \
                    .withColumn("min2", func.regexp_replace("min2", "&gt;", ">")) \
                    .withColumnRenamed("通用名", "标准通用名")
    # 2. hospital_map 文件
    hospital_map = spark.read.parquet(hospital_map_path)
    hospital_map = hospital_map.select("医院编码", "医院名称", "等级" ,"标准化省份", "标准化城市").distinct() \
                                .withColumnRenamed('医院编码', 'ID')
    hospital_map = deal_ID_length(hospital_map)
    # 3. ims 文件
    ims_Sales = spark.read.csv(ims_Sales_path, header=True)
    ims_Sales = ims_Sales.withColumn('Period_Code', func.regexp_replace("Period_Code", "M", "")).distinct()
    # 匹配英文通用名
    ims_mol_lkp = spark.read.csv(ims_mol_lkp_path, header=True).distinct()
    ims_mol_ref = spark.read.csv(ims_mol_ref_path, header=True).distinct()
    # 合并复方分子：将Pack_ID相同的Molecule_Desc合并到一起
    ims_mol = ims_mol_lkp.join(ims_mol_ref, ims_mol_lkp['Molecule_ID']==ims_mol_ref['Molecule_Id'], how='left')
    
    Source_window = Window.partitionBy("Pack_ID").orderBy(func.col('Molecule_Desc'))
    rank_window = Window.partitionBy("Pack_ID").orderBy(func.col('Molecule_Desc').desc())
    
    ims_mol = ims_mol.select("Pack_ID", "Molecule_Desc").distinct() \
                    .select("Pack_ID", func.collect_list(func.col('Molecule_Desc')).over(Source_window).alias('Molecule_Composition'), 
                                                        func.rank().over(rank_window).alias('rank')).persist()
    ims_mol = ims_mol.where(ims_mol.rank == 1).drop('rank')
    ims_mol = ims_mol.withColumn('Molecule_Composition', func.concat_ws('_', func.col('Molecule_Composition'))) \
                        .orderBy('Pack_ID')
    # ims城市范围
    ims_city_map = spark.read.csv(ims_city_map_path, header=True)
    # 4. max 文件
    max_data = spark.read.parquet(max_result_city_path)
    max_data_m = max_data.join(product_map, max_data.Prod_Name==product_map.min2, how='left') \
                        .withColumn('Date', col('Date').cast(IntegerType())) \
                        .withColumn('PANEL', col('PANEL').cast(IntegerType())) \
                        .withColumn("Pack_ID", func.when(col('Pack_ID').isNull(), func.lit(0)).otherwise(col('Pack_ID'))).distinct()
    # 5. panel 文件
    panel = spark.read.parquet(panel_path)
    panel = panel.withColumn('Date', col('Date').cast(IntegerType()))
    panel = panel.where((col('Date') >= int(time_left)) & (col('Date') <= int(time_right)))
    panel = deal_ID_length(panel)

    # %%
    # ====  三. 检查步骤  ====
    '''
    1.产品层面：样本 vs MAX vs IMS 对比，group by Packid&Date
    2.bycity比对：样本与ims的gap应小于10%
    3.补数金额比例：总金额的 1-3%
    4.放大比例范围：1-3倍
    '''
    # 1.产品层面：样本 vs MAX vs IMS 对比，group by Packid&Date
    # MAX
    check_max_1 = max_data_m.groupby('Date', 'Prod_Name', '标准通用名', 'Pack_ID') \
                            .agg(func.sum('Predict_Sales').alias('max_Sales'))
    
    # CPA
    check_cpa_1 = max_data_m.where(col('PANEL')==1) \
                            .groupby('Date', 'Prod_Name', '标准通用名', 'Pack_ID') \
                            .agg(func.sum('Predict_Sales').alias('cpa_Sales')) 
    
    # IMS
    ims_Sales_1 = ims_Sales.where(col('Geography_id') == 'CHT') \
                            .groupby('Pack_ID', 'Period_Code').agg(func.sum('LC').alias('ims_Sales')) \
                            .withColumn('month_pack', func.concat_ws('_', col('Period_Code'), col('Pack_ID'))) \
                            .join(ims_mol, on='Pack_ID', how='left')
    # 匹配筛选出max月份有的pack
    check_max_1_tmp = check_max_1.withColumn('month_pack', func.concat_ws('_', col('Date'), col('Pack_ID'))) \
                                .withColumn('flag', func.lit(1)) \
                                .select('month_pack', 'flag').distinct()
    ims_Sales_2 = ims_Sales_1.join(check_max_1_tmp, on='month_pack', how='left')
    ims_Sales_2 = ims_Sales_2.where(~col('flag').isNull()) \
                            .groupby('Period_Code', 'Pack_ID', 'Molecule_Composition') \
                            .agg(func.sum('ims_Sales').alias('ims_Sales')) \
                            .withColumn('Period_Code', col('Period_Code').cast(IntegerType())) \
                            .withColumnRenamed('Period_Code', 'Date') \
                            .withColumn("Pack_ID", col("Pack_ID").cast(IntegerType()))
    
    check_result = check_max_1.join(check_cpa_1, on=['Date', 'Prod_Name', '标准通用名', 'Pack_ID'], how='full') \
                               .join(ims_Sales_2, on=['Date', 'Pack_ID'], how='full').persist()
    check_result = check_result.withColumn('Rank',func.row_number() \
                                               .over(Window.partitionBy('Date', 'Pack_ID').orderBy(col('Pack_ID').desc()))).persist()
    check_result = check_result.withColumn('ims_Sales', func.when(col('Rank') > 1, func.lit(0)).otherwise(col('ims_Sales'))) \
                                .withColumn('Year', func.substring(col('Date'), 0, 4)) \
                                .select('Date', 'Prod_Name', '标准通用名', 'Pack_ID', 'Molecule_Composition', 'max_Sales', 'cpa_Sales', 
                                        'ims_Sales', 'Rank', 'Year')
    check_result = check_result.repartition(1)
    check_result.write.format("csv").option("header", "true") \
            .mode("overwrite").save(check_1_path)
    

    # %%
    # 2.按城市检查：样本 vs MAX vs IMS 对比，group by City&Date
    
    # MAX
    ims_city = ims_city_map.select('City').distinct().toPandas()['City'].tolist()
    max_data_m_all = max_data_m.withColumn('City', func.lit('全国')) \
                                .withColumn('Province', func.lit('全国'))
    
    check_max_bycity = max_data_m.withColumn('City', city_change(col('City'))) \
                                .union(max_data_m_all) \
                                .where(col('City').isin(ims_city)) \
                                .groupby('Date', 'Province', 'City', 'Prod_Name', '标准通用名', 'Pack_ID') \
                                .agg(func.sum('Predict_Sales').alias('max_Sales'))
    # CPA
    check_cpa_bycity = max_data_m.where(col('PANEL') == 1) \
                                .withColumn('City', city_change(col('City'))) \
                                .union(max_data_m_all.where(col('PANEL') == 1)) \
                                .where(col('City').isin(ims_city)) \
                                .groupby('Date', 'Province', 'City', 'Prod_Name', '标准通用名', 'Pack_ID') \
                                .agg(func.sum('Predict_Sales').alias('cpa_Sales'))
    # IMS
    ims_Sales_bycity = ims_Sales.join(ims_city_map.select("AUDIT SHORT DESC", "Province", "City"), 
                                    ims_Sales['Geography_id']==ims_city_map['AUDIT SHORT DESC'], how='left')
    ims_Sales_bycity = ims_Sales_bycity.where(~col('Province').isNull()) \
                                        .groupby('Pack_ID', 'Period_Code', 'Province', 'City') \
                                        .agg(func.sum('LC').alias('ims_Sales')) \
                                        .withColumn('month_pack', func.concat_ws('_', col('Period_Code'), col('Pack_ID'))) \
                                        .join(ims_mol, on='Pack_ID', how='left')
    # 匹配筛选出max月份有的pack
    check_max_bycity_tmp = check_max_bycity.withColumn('month_pack', func.concat_ws('_', col('Date'), col('Pack_ID'))) \
                                            .withColumn('flag', func.lit(1)) \
                                            .select('month_pack', 'flag').distinct()
    ims_Sales_bycity_2 = ims_Sales_bycity.join(check_max_bycity_tmp, on='month_pack', how='left') \
                                        .where(~col('flag').isNull()) \
                                        .groupby('Period_Code', 'Province', 'City', 'Pack_ID', 'Molecule_Composition') \
                                        .agg(func.sum('ims_Sales').alias('ims_Sales')) \
                                        .withColumn('Period_Code', col('Period_Code').cast(IntegerType())) \
                                        .withColumnRenamed('Period_Code', 'Date') \
                                        .withColumn("Pack_ID", col("Pack_ID").cast(IntegerType()))
    
    check_result_bycity = check_max_bycity.join(check_cpa_bycity, on=['Date', 'Province', 'City', 'Prod_Name', '标准通用名', 'Pack_ID'], how='full') \
                               .join(ims_Sales_bycity_2, on=['Date', 'Province', 'City', 'Pack_ID'], how='full').persist()
    check_result_bycity = check_result_bycity.withColumn('Rank',func.row_number() \
                                               .over(Window.partitionBy('Date', 'Province', 'City', 'Pack_ID').orderBy(col('Pack_ID').desc()))).persist()
    check_result_bycity = check_result_bycity.withColumn('ims_Sales', func.when(col('Rank') > 1, func.lit(0)).otherwise(col('ims_Sales'))) \
                                .withColumn('Year', func.substring(col('Date'), 0, 4)) \
                                .select('Date', 'Province', 'City', 'Prod_Name', '标准通用名', 'Pack_ID', 'max_Sales', 'cpa_Sales', 'ims_Sales')
    check_result_bycity = check_result_bycity.repartition(1)
    check_result_bycity.write.format("csv").option("header", "true") \
        .mode("overwrite").save(check_2_path)

    # %%
    # 3.补数金额比例：占总金额 1-3%
    check_sales = panel.groupby('Date', 'Molecule', 'add_flag').agg(func.sum('Sales').alias('Sales'))
    check_sales = check_sales.groupBy('Date', 'Molecule').pivot('add_flag').agg(func.sum('Sales')).persist()
    check_sales = check_sales.withColumn('补数比例', col('1')/col('0')) \
                              .withColumnRenamed('Molecule', '标准通用名') 
    
    check_sales = check_sales.repartition(1)
    check_sales.write.format("csv").option("header", "true") \
        .mode("overwrite").save(check_3_path)
    # 4.放大比例
    check_max_sales = max_data_m.groupby('Date', '标准通用名', 'PANEL').agg(func.sum('Predict_Sales').alias('金额'))
    check_max_sales = check_max_sales.groupBy('Date', '标准通用名').pivot('PANEL').agg(func.sum('金额')).persist()
    check_max_sales = check_max_sales.withColumn('放大比例', (col('1')+col('0'))/col('1'))
    
    check_max_sales = check_max_sales.repartition(1)
    check_max_sales.write.format("csv").option("header", "true") \
        .mode("overwrite").save(check_4_path)
    # 5.每个月的产品个数（min2）MAX，CPA
    check_max_1_count = check_max_1.select('Date', 'Prod_Name').distinct() \
                        .groupby('Date').count() \
                        .withColumnRenamed('count', 'max_prd')
    check_cpa_1_count = check_cpa_1.select('Date', 'Prod_Name').distinct() \
                        .groupby('Date').count() \
                        .withColumnRenamed('count', 'cpa_prd')
    data_prd = check_max_1_count.join(check_cpa_1_count, on='Date', how='left') \
                                .orderBy('Date')
    
    data_prd = data_prd.repartition(1)
    data_prd.write.format("csv").option("header", "true") \
        .mode("overwrite").save(check_5_path)
    # 6.每个月的CPA医院个数
    data_cpa_pha = panel.join(hospital_map.select('ID', '医院名称').distinct(), on='ID', how='left') \
                        .select('Date', '医院名称').distinct() \
                        .groupby('Date').count() \
                        .withColumnRenamed('count', '医院个数') \
                        .orderBy('Date')
    data_cpa_pha = data_cpa_pha.repartition(1)
    data_cpa_pha.write.format("csv").option("header", "true") \
        .mode("overwrite").save(check_6_path)
