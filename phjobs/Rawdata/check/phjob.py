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
    outdir = kwargs['outdir']
    minimum_product_sep = kwargs['minimum_product_sep']
    minimum_product_columns = kwargs['minimum_product_columns']
    current_year = kwargs['current_year']
    current_month = kwargs['current_month']
    three = kwargs['three']
    twelve = kwargs['twelve']
    test = kwargs['test']
    g_id_molecule = kwargs['g_id_molecule']
    ### input args ###
    
    ### output args ###
    c = kwargs['c']
    d = kwargs['d']
    ### output args ###

    
    
    from pyspark.sql import SparkSession, Window
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    import os
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, greatest, least, col
    import time
    import pandas as pd
    import numpy as np    
    # %%
    '''
    project_name = 'Gilead'
    outdir = '202101'
    current_month = '1'
    current_year = '2021'
    minimum_product_sep = "|"
    '''
    # %%
    # 输入
    current_year = int(current_year)
    current_month = int(current_month)
    three = int(three)
    twelve = int(twelve)
    
    if minimum_product_sep == "kong":
        minimum_product_sep = ""
    minimum_product_columns = minimum_product_columns.replace(" ","").split(",")
    product_map_path = max_path + '/' + project_name + '/' + outdir + '/prod_mapping'
    cpa_pha_mapping_path = max_path + '/' + project_name + '/cpa_pha_mapping'
    province_city_mapping_path = max_path + '/' + project_name + '/province_city_mapping'
    if test == 'True':
        raw_data_path = max_path + '/' + project_name + '/' + outdir + '/raw_data_check/raw_data'
    else:
        raw_data_path = max_path + '/' + project_name + '/' + outdir + '/raw_data'
    raw_data_check_path = max_path + '/' + project_name + '/' + outdir + '/raw_data_check/'
    
    # 输出
    check_result_path = raw_data_check_path + '/check_result.csv'
    check_1_path = raw_data_check_path + '/check_1_每个月产品个数.csv'
    check_2_path = raw_data_check_path + '/check_2_各产品历史月份销量.csv'
    check_3_path = raw_data_check_path + '/check_3_历史医院个数.csv'
    check_5_path = raw_data_check_path + '/check_5_最近12期每家医院每个月的金额规模.csv'
    check_8_path = raw_data_check_path + '/check_8_每个医院每个月产品个数.csv'
    check_9_1_path = raw_data_check_path + '/check_9_1_所有产品每个月金额.csv'
    check_9_2_path = raw_data_check_path + '/check_9_2_所有产品每个月份额.csv'
    check_9_3_path = raw_data_check_path + '/check_9_3_所有产品每个月排名.csv'
    check_10_path = raw_data_check_path + '/check_10_在售产品医院个数.csv'
    check_11_path = raw_data_check_path + '/check_11_金额_医院贡献率等级.csv'
    check_12_path = raw_data_check_path + '/check_12_金额_医院分子贡献率等级.csv'
    check_13_path = raw_data_check_path + '/check_13_数量_医院贡献率等级.csv'
    check_14_path = raw_data_check_path + '/check_14_数量_医院分子贡献率等级.csv'
    check_15_path = raw_data_check_path + '/check_15_最近12期每家医院每个月每个产品的价格与倍数.csv'
    check_16_path = raw_data_check_path + '/check_16_各医院各产品价格与所在地区对比.csv'
    #tmp_1_path  = raw_data_check_path + '/tmp_1'
    #tmp_2_path  = raw_data_check_path + '/tmp_2'

    # %%
    # ================= 数据执行 ==================	
    
    MTH = current_year*100 + current_month
    
    if MTH%100 == 1:
        PREMTH = (MTH//100 -1)*100 +12
    else:
        PREMTH = MTH - 1
            
    # 当前月的前3个月
    if three > (current_month - 1):
        diff = three - current_month
        RQMTH = [i for i in range((current_year - 1)*100 +12 - diff , (current_year - 1)*100 + 12 + 1)] + [i for i in range(MTH - current_month + 1 , MTH)]
    else:
        RQMTH = [i for i in range(MTH - current_month + 1 , MTH)][-three:]
    
    # 当前月的前12个月
    if twelve > (current_month - 1):
        diff = twelve - current_month
        mat_month = [i for i in range((current_year - 1)*100 + 12 - diff , (current_year - 1)*100 + 12 + 1)] + [i for i in range(MTH - current_month + 1 , MTH)]
    else:
        mat_month = [i for i in range(MTH - current_month + 1 , MTH)][-twelve:]

    # %%
    # ==== 一. 数据准备  ==== 
    
    def deal_ID_length(df):
        # ID不足7位的补足0到6位
        # 国药诚信医院编码长度是7位数字，cpa医院编码是6位数字。
        df = df.withColumn("ID", df["ID"].cast(StringType()))
        # 去掉末尾的.0
        df = df.withColumn("ID", func.regexp_replace("ID", "\\.0", ""))
        df = df.withColumn("ID", func.when(func.length(df.ID) < 7, func.lpad(df.ID, 6, "0")).otherwise(df.ID))
        return df
    
    # 1. cpa_pha_mapping，province_city_mapping 文件
    cpa_pha_mapping = spark.read.parquet(cpa_pha_mapping_path)
    cpa_pha_mapping = cpa_pha_mapping.where(cpa_pha_mapping["推荐版本"] == 1).select('ID', 'PHA').distinct()
    cpa_pha_mapping = deal_ID_length(cpa_pha_mapping)
    
    province_city_mapping = spark.read.parquet(province_city_mapping_path)
    province_city_mapping = deal_ID_length(province_city_mapping)
    city_list = ['北京市','常州市','广州市','济南市','宁波市','上海市','苏州市','天津市','温州市','无锡市']
    province_city_mapping = province_city_mapping.withColumn('Province', func.when(col('City').isin(['福州市','厦门市','泉州市']), func.lit('福厦泉')) \
                                                                    .otherwise(func.when(col('City').isin(city_list), col('City')) \
                                                                               .otherwise(col('Province'))))
    
    # 2. Raw_data 处理
    Raw_data = spark.read.parquet(raw_data_path)
    Raw_data = Raw_data.withColumn('Date', Raw_data['Date'].cast(IntegerType())) \
                    .withColumn('Units', Raw_data['Units'].cast(DoubleType())) \
                    .withColumn('Sales', Raw_data['Sales'].cast(DoubleType()))
    if 'Pack_ID' in Raw_data.columns:
        Raw_data = Raw_data.drop('Pack_ID')
    
    # 生成min1
    # if project_name != 'Mylan':
    Raw_data = Raw_data.withColumn('Brand_bak', Raw_data.Brand)
    Raw_data = Raw_data.withColumn('Brand', func.when((Raw_data.Brand.isNull()) | (Raw_data.Brand == 'NA'), Raw_data.Molecule).
                                                otherwise(Raw_data.Brand))
    Raw_data = Raw_data.withColumn("min1", func.when(Raw_data[minimum_product_columns[0]].isNull(), func.lit("NA")).
                                       otherwise(Raw_data[minimum_product_columns[0]]))
    for i in minimum_product_columns[1:]:
        Raw_data = Raw_data.withColumn(i, Raw_data[i].cast(StringType()))
        Raw_data = Raw_data.withColumn("min1", func.concat(
            Raw_data["min1"],
            func.lit(minimum_product_sep),
            func.when(func.isnull(Raw_data[i]), func.lit("NA")).otherwise(Raw_data[i])))
    Raw_data = Raw_data.withColumn('Brand', Raw_data.Brand_bak).drop('Brand_bak')
    
    # 3. 产品匹配表处理 
    product_map = spark.read.parquet(product_map_path)
    # a. 列名清洗统一
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
    
    # b. 选取需要的列
    product_map = product_map \
                    .select("min1", "min2", "pfc", "通用名", "标准商品名", "标准剂型", "标准规格", "标准包装数量", "标准生产企业") \
                    .distinct() \
                    .withColumnRenamed("标准商品名", "商品名") \
                    .withColumnRenamed("标准剂型", "剂型") \
                    .withColumnRenamed("标准规格", "规格") \
                    .withColumnRenamed("标准包装数量", "包装数量") \
                    .withColumnRenamed("标准生产企业", "生产企业") \
                    .withColumnRenamed("pfc", "Pack_ID")
    
    product_map = product_map.withColumn('Pack_ID', product_map.Pack_ID.cast(IntegerType()))
    
    # 4. 匹配产品匹配表，标准化min2通用名商品名
    Raw_data_1 = Raw_data.join(product_map.select('min1','min2','通用名','商品名','Pack_ID').dropDuplicates(['min1']), on='min1', how='left')
    Raw_data_1 = Raw_data_1.groupby('ID', 'Date', 'min2', '通用名','商品名','Pack_ID') \
                            .agg(func.sum('Sales').alias('Sales'), func.sum('Units').alias('Units')) \
                            .withColumnRenamed('min2', 'Prod_Name')

    # %%
    #========== check_1 ==========
    
    # 每个月产品个数(min2)
    check_1 = Raw_data_1.select('Date', 'Prod_Name').distinct() \
                        .groupby('Date').count() \
                        .withColumnRenamed('count', '每月产品个数_min2') \
                        .orderBy('Date').persist()
    
    ### 判断产品个数与上月相比是否超过 8%
    MTH_product_num = check_1.where(check_1.Date == MTH).toPandas()['每月产品个数_min2'][0]
    PREMTH_product_num = check_1.where(check_1.Date == PREMTH).toPandas()['每月产品个数_min2'][0]
    check_result_1 = (MTH_product_num/PREMTH_product_num < 0.08)
    
    check_1 = check_1.repartition(1)
    check_1.write.format("csv").option("header", "true") \
        .mode("overwrite").save(check_1_path)

    # %%
    #========== check_2 ==========
    
    # 各产品历史月份销量
    check_2 = Raw_data_1.groupby('Date', 'Prod_Name').agg(func.sum('Sales').alias('Sales'))
    check_2 = check_2.groupBy("Prod_Name").pivot("Date").agg(func.sum('Sales')).persist()
    
    ### 判断缺失产品是否在上个月销售金额超过 2%
    MTH_product_Sales = check_2.where(check_2[str(MTH)].isNull()).groupBy().agg(func.sum(str(PREMTH)).alias('sum')).toPandas()['sum'][0]
    PREMTH_product_Sales = check_2.groupBy().agg(func.sum(str(PREMTH)).alias('sum')).toPandas()['sum'][0]
    if MTH_product_Sales == None:
        MTH_product_Sales = 0
    check_result_2 = (MTH_product_Sales/PREMTH_product_Sales < 0.08)
    
    check_2 = check_2.repartition(1)
    check_2.write.format("csv").option("header", "true") \
        .mode("overwrite").save(check_2_path)

    # %%
    #========== check_3 ==========
    
    # 历史医院个数
    check_3 = Raw_data.select('Date', 'ID').distinct() \
                    .groupBy('Date').count() \
                    .withColumnRenamed('count', '医院个数') \
                    .orderBy('Date').persist()
    
    ### 判断历史医院个数是否超过1%                
    MTH_hospital_num = check_3.where(check_3.Date == MTH).toPandas()['医院个数'][0]
    PREMTH_hospital_num = check_3.where(check_3.Date == PREMTH).toPandas()['医院个数'][0]
    check_result_3 = (MTH_hospital_num/PREMTH_hospital_num -1 < 0.01)
    
    check_3 = check_3.repartition(1)
    check_3.write.format("csv").option("header", "true") \
        .mode("overwrite").save(check_3_path)

    # %%
    #========== check_5 ==========
    
    # 最近12期每家医院每个月的销量规模
    check_5_1 = Raw_data.where(Raw_data.Date > (current_year - 1)*100 + current_month - 1) \
                        .groupBy('ID', 'Date').agg(func.sum('Sales').alias('Sales'))
    check_5_1 = check_5_1.groupBy("ID").pivot("Date").agg(func.sum('Sales')).persist() \
                        .orderBy('ID').persist()
    
    ### 检查当月缺失医院在上个月的销售额占比
    MTH_hospital_Sales = check_5_1.where(check_5_1[str(MTH)].isNull()).groupBy().agg(func.sum(str(PREMTH)).alias('sum')).toPandas()['sum'][0]
    PREMTH_hospital_Sales = check_5_1.groupBy().agg(func.sum(str(PREMTH)).alias('sum')).toPandas()['sum'][0]
    if MTH_hospital_Sales == None:
        MTH_hospital_Sales = 0
    check_result_5 = (MTH_hospital_Sales/PREMTH_hospital_Sales < 0.01)
    
    # 每家医院的月销金额在最近12期的误差范围内（mean+-1.96std），范围内的医院数量占比大于95%；
    check_5_2 = Raw_data.where((Raw_data.Date > (current_year-1)*100+current_month-1 ) & (Raw_data.Date < current_year*100+current_month)) \
                        .groupBy('ID', 'Date').agg(func.sum('Sales').alias('Sales')) \
                        .groupBy('ID').agg(func.mean('Sales').alias('Mean_Sales'), func.stddev('Sales').alias('Sd_Sales')).persist()
    
    check_5_2 = check_5_2.join(Raw_data.where(Raw_data.Date == current_year*100+current_month).groupBy('ID').agg(func.sum('Sales').alias('Sales_newmonth')), 
                                            on='ID', how='left').persist()
    check_5_2 = check_5_2.withColumn('Check', func.when(check_5_2.Sales_newmonth < check_5_2.Mean_Sales-1.96*check_5_2.Sd_Sales, func.lit('F')) \
                                                .otherwise(func.when(check_5_2.Sales_newmonth > check_5_2.Mean_Sales+1.96*check_5_2.Sd_Sales, func.lit('F')) \
                                                                .otherwise(func.lit('T'))))
    check_5_2 = check_5_2.withColumn('Check', func.when(func.isnan(check_5_2.Mean_Sales) | func.isnan(check_5_2.Sd_Sales) | check_5_2.Sales_newmonth.isNull(), func.lit(None)) \
                                                    .otherwise(check_5_2.Check))                            
    
    check_5 = check_5_1.join(check_5_2, on='ID', how='left').orderBy('ID').persist()
    
    check_5 = check_5.repartition(1)
    check_5.write.format("csv").option("header", "true") \
        .mode("overwrite").save(check_5_path)

    # %%
    #========== check_6 ==========
    
    # 最近12期每家医院每个月的销量规模
    check_6_1 = Raw_data.where(Raw_data.Date > (current_year-1)*100+current_month-1) \
                        .groupby('ID', 'Date').agg(func.sum('Units').alias('Units')) \
                        .groupBy("ID").pivot("Date").agg(func.sum('Units')).persist()
    
    # 每家医院的月销数量在最近12期的误差范围内（mean+-1.96std），范围内的医院数量占比大于95%；
    check_6_2 = Raw_data.where((Raw_data.Date > (current_year-1)*100+current_month-1 ) & (Raw_data.Date < current_year*100+current_month)) \
                        .groupBy('ID', 'Date').agg(func.sum('Units').alias('Units')) \
                        .groupBy('ID').agg(func.mean('Units').alias('Mean_Units'), func.stddev('Units').alias('Sd_Units'))
    check_6_2 = check_6_2.join(Raw_data.where(Raw_data.Date == current_year*100+current_month).groupBy('ID').agg(func.sum('Units').alias('Units_newmonth')), 
                                            on='ID', how='left').persist()
    check_6_2 = check_6_2.withColumn('Check', func.when(check_6_2.Units_newmonth < check_6_2.Mean_Units-1.96*check_6_2.Sd_Units, func.lit('F')) \
                                                .otherwise(func.when(check_6_2.Units_newmonth > check_6_2.Mean_Units+1.96*check_6_2.Sd_Units, func.lit('F')) \
                                                                .otherwise(func.lit('T'))))
    check_6_2 = check_6_2.withColumn('Check', func.when(func.isnan(check_6_2.Mean_Units) | func.isnan(check_6_2.Sd_Units) | check_6_2.Units_newmonth.isNull(), func.lit(None)) \
                                                    .otherwise(check_6_2.Check)) 
    
    check_6 = check_6_1.join(check_6_2, on='ID', how='left').orderBy('ID')

    # %%
    #========== check_7 ==========
    
    # 最近12期每家医院每个月每个产品(Packid)的平均价格
    check_7_1 = Raw_data_1.where(Raw_data_1.Date > (current_year-1)*100+current_month-1) \
                        .groupBy('ID', 'Date', '通用名','商品名','Pack_ID') \
                        .agg(func.sum('Sales').alias('Sales'), func.sum('Units').alias('Units')).persist()
    check_7_1 = check_7_1.withColumn('Price', check_7_1.Sales/check_7_1.Units)
    check_7_1 = check_7_1.groupBy('ID', '通用名', '商品名', 'Pack_ID').pivot("Date").agg(func.sum('Price')) \
                        .orderBy('ID', '通用名', '商品名').persist()
    
    # 每家医院的每个产品单价在最近12期的误差范围内（mean+-1.96std或gap10%以内），范围内的产品数量占比大于95%；
    check_7_2 = Raw_data_1.where((Raw_data_1.Date > (current_year-1)*100+current_month-1 ) & (Raw_data_1.Date < current_year*100+current_month)) \
                        .groupBy('ID', 'Date', '通用名', '商品名', 'Pack_ID') \
                        .agg(func.sum('Sales').alias('Sales'), func.sum('Units').alias('Units'))
    check_7_2 = check_7_2.withColumn('Price', check_7_2.Sales/check_7_2.Units)
    check_7_2 = check_7_2.groupBy('ID', '通用名', '商品名', 'Pack_ID') \
                        .agg(func.mean('Price').alias('Mean_Price'), func.stddev('Price').alias('Sd_Price'))
    check_7_2 = check_7_2.withColumn('Sd_Price', func.when(func.isnan(check_7_2.Sd_Price), func.lit(0)).otherwise(check_7_2.Sd_Price))
    Raw_data_1_tmp = Raw_data_1.where(Raw_data_1.Date == current_year*100+current_month) \
                                .groupBy('ID', '通用名', '商品名', 'Pack_ID') \
                                .agg(func.sum('Sales').alias('Sales_newmonth'), func.sum('Units').alias('Units_newmonth'))
    Raw_data_1_tmp = Raw_data_1_tmp.withColumn('Price_newmonth', Raw_data_1_tmp.Sales_newmonth/Raw_data_1_tmp.Units_newmonth)
    Raw_data_1_tmp = Raw_data_1_tmp.withColumn('Pack_ID', func.when(Raw_data_1_tmp.Pack_ID.isNull(), func.lit(0)).otherwise(Raw_data_1_tmp.Pack_ID))
    check_7_2 = check_7_2.withColumn('Pack_ID', func.when(check_7_2.Pack_ID.isNull(), func.lit(0)).otherwise(check_7_2.Pack_ID))
    check_7_2 = check_7_2.join(Raw_data_1_tmp, on=['ID', '通用名', '商品名', 'Pack_ID'], how='left').persist()
    
    check_7_2 = check_7_2.withColumn('Check', \
                func.when((check_7_2.Price_newmonth < check_7_2.Mean_Price-1.96*check_7_2.Sd_Price) & (check_7_2.Price_newmonth < check_7_2.Mean_Price*0.9), func.lit('F')) \
                    .otherwise(func.when((check_7_2.Price_newmonth > check_7_2.Mean_Price+1.96*check_7_2.Sd_Price) & (check_7_2.Price_newmonth > check_7_2.Mean_Price*1.1), func.lit('F')) \
                                    .otherwise(func.lit('T'))))
    check_7_2 = check_7_2.withColumn('Check', func.when(check_7_2.Sales_newmonth.isNull() | check_7_2.Units_newmonth.isNull(), func.lit(None)) \
                                                    .otherwise(check_7_2.Check)) 
    
    check_7_1 = check_7_1.withColumn('Pack_ID', func.when(check_7_1.Pack_ID.isNull(), func.lit(0)).otherwise(check_7_1.Pack_ID))
    check_7 = check_7_1.join(check_7_2, on=['ID', '通用名', '商品名', 'Pack_ID'], how='left').orderBy('ID', '通用名', '商品名').persist()
    check_7 = check_7.withColumn('Pack_ID', func.when(check_7.Pack_ID == 0, func.lit(None)).otherwise(check_7.Pack_ID))
    
    check_7.groupby('Check').count().show()

    # %%
    #========== check_8 ==========
    
    # 每个月产品个数
    check_8 = Raw_data_1.select('Date', 'ID', 'Prod_Name').distinct() \
                        .groupBy('Date').count() \
                        .withColumnRenamed('count', '每月产品个数_min1') \
                        .orderBy('Date').persist()
    # 最近三个月全部医院的产品总数
    check_8_1 = Raw_data_1.where(Raw_data_1.Date.isin(RQMTH)) \
                        .select('Date', 'ID', 'Prod_Name').distinct() \
                        .count()/3
    # 当月月产品个数                  
    check_8_2 = Raw_data_1.where(Raw_data_1.Date.isin(MTH)) \
                        .select('Date', 'ID', 'Prod_Name').distinct() \
                        .count()
    
    check_result_8 = (check_8_2/check_8_1 < 0.03)
    
    check_8 = check_8.repartition(1)
    check_8.write.format("csv").option("header", "true") \
        .mode("overwrite").save(check_8_path)

    # %%
    #========== check_9 ==========
    
    # 全部医院的全部产品金额、份额、排名与历史月份对比(含缺失医院)
    check_9_1 = Raw_data_1.groupBy('Date', '商品名').agg(func.sum(Raw_data_1.Sales).alias('Sales')) \
                        .groupBy('商品名').pivot('Date').agg(func.sum('Sales')).persist()
    check_9_2 = Raw_data_1.groupBy('Date', '商品名').agg(func.sum(Raw_data_1.Sales).alias('Sales')) \
                        .join(Raw_data_1.groupBy('Date').agg(func.sum('Sales').alias('Sales_month')), on='Date', how='left')
    check_9_2 = check_9_2.withColumn('share', check_9_2.Sales/check_9_2.Sales_month) \
                        .groupBy('商品名').pivot('Date').agg(func.sum('share')).persist()
    check_9_3 = Raw_data_1.groupBy('Date', '商品名').agg(func.sum(Raw_data_1.Sales).alias('Sales'))
    check_9_3 = check_9_3.withColumn('Rank', func.row_number().over(Window.partitionBy('Date').orderBy(check_9_3['Sales'].desc())))
    check_9_3 = check_9_3.groupBy('商品名').pivot('Date').agg(func.sum('Rank')).persist()
    
    check_9_1 = check_9_1.repartition(1)
    check_9_1.write.format("csv").option("header", "true") \
        .mode("overwrite").save(check_9_1_path)
    
    check_9_2 = check_9_2.repartition(1)
    check_9_2.write.format("csv").option("header", "true") \
        .mode("overwrite").save(check_9_2_path)
    
    check_9_3 = check_9_3.repartition(1)
    check_9_3.write.format("csv").option("header", "true") \
        .mode("overwrite").save(check_9_3_path)

    # %%
    #========== check_10 ==========
    
    # group by 产品和月份，count 医院ID，省份
    # 检查是否有退市产品突然有销量；例如（17120906_2019M12,Pfizer_HTN）
    check_10 = Raw_data_1.select('Date', 'Prod_Name', 'ID').distinct() \
                        .groupBy('Date', 'Prod_Name').count() \
                        .withColumnRenamed('count', '在售产品医院个数') \
                        .groupBy('Prod_Name').pivot('Date').agg(func.sum('在售产品医院个数')).persist()
    
    check_10 = check_10.repartition(1)
    check_10.write.format("csv").option("header", "true") \
        .mode("overwrite").save(check_10_path)

    # %%
    #========== check_贡献率等级相关 ==========
    
    @udf(DoubleType())
    def mean_adj(*cols):
        # 以行为单位，去掉一个最大值和一个最小值求平均
        import numpy as np
        row_max = cols[0]
        row_min = cols[1]
        others = cols[2:]
        row_mean_list = [x for x in others if x is not None]
        if len(row_mean_list) > 3:
            row_mean = (np.sum(row_mean_list) - row_max - row_min)/(len(row_mean_list) -2)
        else:
            row_mean = 0
        return float(row_mean)
    
    @udf(DoubleType())
    def min_diff(row_max, row_min, mean_adj):
        # row_min 与 mean_adj 的差值
        import numpy as np
        if mean_adj is not None:
            # diff1 = abs(row_max - mean_adj)
            diff2 = abs(row_min - mean_adj)
            row_diff = diff2
        else:
            row_diff = 0
        return float(row_diff)
    
    def func_pandas_cumsum_level(pdf, grouplist, sumcol):
        '''
        贡献率等级计算:
        分月统计
        降序排列累加求和，占总数的比值乘10，取整   
        '''
        month = pdf['Date'][0]
        pdf = pdf.groupby(grouplist)[sumcol].agg('sum').reset_index()
        pdf = pdf.sort_values(sumcol, ascending=False)
        pdf['cumsum'] = pdf[sumcol].cumsum()
        pdf['sum'] = pdf[sumcol].sum()
        pdf['con_add'] = pdf['cumsum']/pdf['sum']
        pdf['level'] = np.where(pdf['con_add']*10 > 10, 10, np.ceil(pdf['con_add']*10))
        pdf ['month'] = str(month)
        pdf = pdf[grouplist + ['level', 'month']]
        pdf['level'].astype('int')
        return pdf
    
    def colculate_diff(check_num, grouplist):
        '''
        去掉最大值和最小值求平均
        最小值与平均值的差值min_diff
        '''
        check_num_cols = check_num.columns
        check_num_cols = list(set(check_num_cols) - set(grouplist))
        for each in check_num_cols:
            check_num = check_num.withColumn(each, check_num[each].cast(IntegerType()))
    
        # 平均值计算
        check_num = check_num.withColumn("row_max", func.greatest(*check_num_cols)) \
                        .withColumn("row_min", func.least(*check_num_cols))
        check_num = check_num.withColumn("mean_adj", 
                            mean_adj(func.col('row_max'), func.col('row_min'), *(func.col(x) for x in check_num_cols)))
        check_num = check_num.withColumn("mean_adj", func.when(func.col('mean_adj') == 0, func.lit(None)).otherwise(func.col('mean_adj')))
        # row_min 与 mean_adj 的差值
        check_num = check_num.withColumn("min_diff", min_diff(func.col('row_max'), func.col('row_min'), func.col('mean_adj')))
        check_num = check_num.withColumn("min_diff", func.when(func.col('mean_adj').isNull(), func.lit(None)).otherwise(func.col('min_diff')))
    
        # 匹配PHA    
        check_num = check_num.join(cpa_pha_mapping, on='ID', how='left')    
        # 排序
        check_num = check_num.orderBy(col('min_diff').desc(), col('mean_adj').desc())
        
        return check_num

    # %%
    Raw_data = deal_ID_length(Raw_data)
    
    #========== check_11 金额==========
    schema = StructType([
            StructField("ID", StringType(), True),
            StructField("level", IntegerType(), True),
            StructField("month", StringType(), True)
            ])
    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def pudf_cumsum_level_11(pdf):
        return func_pandas_cumsum_level(pdf, grouplist=['ID'], sumcol='Sales')
    
    check_11 = Raw_data.groupby(["Date"]).apply(pudf_cumsum_level_11)
    check_11 = check_11.groupby('ID').pivot('month').agg(func.sum('level')).persist()
    check_11 = colculate_diff(check_11, grouplist=['ID'])
    
    check_11 = check_11.repartition(1)
    check_11.write.format("csv").option("header", "true") \
        .mode("overwrite").save(check_11_path)

    # %%
    #========== check_12 金额==========
    if g_id_molecule == 'True':
        schema = StructType([
                StructField("ID", StringType(), True),
                StructField("Molecule", StringType(), True),
                StructField("level", IntegerType(), True),
                StructField("month", StringType(), True)
                ])
        @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
        def pudf_cumsum_level_12(pdf):
            return func_pandas_cumsum_level(pdf, grouplist=['ID', "Molecule"], sumcol='Sales')
    
        check_12 = Raw_data.groupby(["Date"]).apply(pudf_cumsum_level_12)
        check_12 = check_12.groupby('ID', "Molecule").pivot('month').agg(func.sum('level')).persist()
        check_12 = colculate_diff(check_12, grouplist=['ID', 'Molecule'])
    
        check_12 = check_12.repartition(1)
        check_12.write.format("csv").option("header", "true") \
            .mode("overwrite").save(check_12_path)

    # %%
    #========== check_13 数量==========
    
    schema = StructType([
            StructField("ID", StringType(), True),
            StructField("level", IntegerType(), True),
            StructField("month", StringType(), True)
            ])
    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def pudf_cumsum_level_13(pdf):
        return func_pandas_cumsum_level(pdf, grouplist=['ID'], sumcol='Units')
    
    check_13 = Raw_data.groupby(["Date"]).apply(pudf_cumsum_level_13)
    check_13 = check_13.groupby('ID').pivot('month').agg(func.sum('level')).persist()
    check_13 = colculate_diff(check_13, grouplist=['ID'])
    
    check_13 = check_13.repartition(1)
    check_13.write.format("csv").option("header", "true") \
        .mode("overwrite").save(check_13_path)

    # %%
    #========== check_14 数量==========
    if g_id_molecule == 'True':
        schema = StructType([
                StructField("ID", StringType(), True),
                StructField("Molecule", StringType(), True),
                StructField("level", IntegerType(), True),
                StructField("month", StringType(), True)
                ])
        @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
        def pudf_cumsum_level_12(pdf):
            return func_pandas_cumsum_level(pdf, grouplist=['ID', "Molecule"], sumcol='Units')
    
        check_14 = Raw_data.groupby(["Date"]).apply(pudf_cumsum_level_12)
        check_14 = check_14.groupby('ID', "Molecule").pivot('month').agg(func.sum('level')).persist()
        check_14 = colculate_diff(check_14, grouplist=['ID', 'Molecule'])
    
        check_14 = check_14.repartition(1)
        check_14.write.format("csv").option("header", "true") \
            .mode("overwrite").save(check_14_path)

    # %%
    #========== check_15 价格==========
    if g_id_molecule == 'True':
        check_15_a = Raw_data.where(Raw_data.Date > (current_year-1)*100+current_month-1) \
                            .groupBy('ID', 'Date', 'min1') \
                            .agg(func.sum('Sales').alias('Sales'), func.sum('Units').alias('Units')) \
                            .withColumn('Price', col('Sales')/col('Units'))
        check_15_count = check_15_a.groupBy('ID', 'min1').count()
        check_15_sum = check_15_a.groupBy('ID', 'min1').agg(func.sum('Price').alias('sum'), func.max('Price').alias('max'),  
                                                        func.min('Price').alias('min'))
    
        check_15_a = check_15_a.distinct() \
                            .join(check_15_count, on=['ID', 'min1'], how='left') \
                            .join(check_15_sum, on=['ID', 'min1'], how='left')
    
        check_15_a = check_15_a.withColumn('Mean_Price', func.when(col('count') == 1, col('sum')) \
                                                       .otherwise((col('sum') - col('min') - col('max'))/(col('count') - 2 )))
    
        check_15_b = check_15_a.withColumn('Mean_times', func.when(func.abs(col('Price')) > col('Mean_Price'), func.abs(col('Price'))/col('Mean_Price')) \
                                                           .otherwise(col('Mean_Price')/func.abs(col('Price'))))
    
        check_15 = check_15_b.groupby('ID', 'min1').pivot('Date').agg(func.sum('Mean_times')).persist()
    
        check_15 = check_15.withColumn('max', func.greatest(*[i for i in check_15.columns if "20" in i]))
    
        check_15 = check_15.repartition(1)
        check_15.write.format("csv").option("header", "true") \
            .mode("overwrite").save(check_15_path)

    # %%
    #========== check_16 价格==========
    check_16_a = Raw_data.join(province_city_mapping.select('ID','Province').distinct(), on='ID', how='left')
    check_16_b = check_16_a.groupBy('Date', 'min1', 'Province') \
                            .agg(func.sum('Sales').alias('Sales'), func.sum('Units').alias('Units')) \
                            .withColumn('Province_Price', col('Sales')/col('Units'))
    check_16 = check_16_a.groupBy('Date', 'ID', 'min1', 'Province') \
                            .agg(func.sum('Sales').alias('Sales'), func.sum('Units').alias('Units')) \
                            .withColumn('Price', col('Sales')/col('Units')) \
                            .join(check_16_b.select('Date','min1','Province','Province_Price'), on=['Date','Province','min1'], how='left')
    check_16 = check_16.withColumn('gap', col('Price')/col('Province_Price')-1 ) \
                            .withColumn('level', func.ceil(func.abs('gap')*10) ) \
                            .where(col('Date') > current_year*100)
    
    check_16 = check_16.repartition(1)
    check_16.write.format("csv").option("header", "true") \
        .mode("overwrite").save(check_16_path)

    # %%
    #========== 汇总检查结果 ==========
    
    # 汇总检查结果
    check_result = spark.createDataFrame(
        [('产品个数与历史相差不超过0.08', str(check_result_1)), 
        ('缺失产品销售额占比不超过0.02', str(check_result_2)), 
        ('医院个数和历史相差不超过0.01', str(check_result_3)), 
        ('缺失医院销售额占比不超过0.01', str(check_result_5)), 
        ('全部医院的全部产品总个数与最近三个月的均值相差不超过0.03', str(check_result_8))], 
        ('check', 'result'))
    
    check_result = check_result.repartition(1)
    check_result.write.format("csv").option("header", "true") \
        .mode("overwrite").save(check_result_path)

