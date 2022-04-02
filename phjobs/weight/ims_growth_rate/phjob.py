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
    minimum_product_sep = kwargs['minimum_product_sep']
    minimum_product_columns = kwargs['minimum_product_columns']
    market_city_brand = kwargs['market_city_brand']
    job_choice = kwargs['job_choice']
    year_list = kwargs['year_list']
    add_imsinfo_version = kwargs['add_imsinfo_version']
    g_input_version = kwargs['g_input_version']
    ### input args ###
    
    ### output args ###
    ### output args ###

    from pyspark.sql import SparkSession, Window
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    import os
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col
    import time
    import re
    import pandas as pd
    import numpy as np
    import json
    from copy import deepcopy
    import math
    import boto3    
    
    # %%
    # =========== 参数处理 =========== 
    
    # 是否运行此job
    if job_choice != "weight":
         raise ValueError('不运行weight')
    
    logger.debug(market_city_brand)
    
    # year_list=['2018', '2019']
    year_list = year_list.replace(" ","").split(",")
    year_min = year_list[0]
    year_max = year_list[1]
    
    minimum_product_columns = minimum_product_columns.replace(" ","").split(",")
    if minimum_product_sep == "kong":
        minimum_product_sep = ""
    
    def getMarketCityBrandDict(market_city_brand):
        market_city_brand_dict = {}
        for each in market_city_brand.replace(" ","").split(","):
            market_name = each.split(":")[0]
            if market_name not in market_city_brand_dict.keys():
                market_city_brand_dict[market_name]={}
            city_brand = each.split(":")[1]
            for each in city_brand.replace(" ","").split("|"): 
                city = each.split("_")[0]
                brand = each.split("_")[1]
                market_city_brand_dict[market_name][city]=brand
        return market_city_brand_dict   
    
    market_city_brand_dict = getMarketCityBrandDict(market_city_brand)
    logger.debug(market_city_brand_dict)
    

    # %% 
    # =========== 输入数据读取 =========== 
    def dealToNull(df):
        df = df.replace(["None", ""], None)
        return df
    
    def dealScheme(df, dict_scheme):
        # 数据类型处理
        if dict_scheme != {}:
            for i in dict_scheme.keys():
                df = df.withColumn(i, col(i).cast(dict_scheme[i]))
        return df

    def lowCol(df):
        df = df.toDF(*[c.lower() for c in df.columns])
        return df
    
    def getInputVersion(df, table_name):
        # 如果 table在g_input_version中指定了version，则读取df后筛选version，否则使用传入的df
        version = g_input_version.get(table_name, '')
        if version != '':
            version_list =  version.replace(' ','').split(',')
            df = df.where(col('version').isin(version_list))
        return df
    
    def readInFile(table_name, dict_scheme={}):
        df = kwargs[table_name]
        df = dealToNull(df)
        df = lowCol(df)
        df = dealScheme(df, dict_scheme)
        df = getInputVersion(df, table_name.replace('df_', ''))
        return df

       
    df_mkt_mapping = readInFile('df_mkt_mapping') 
       
    df_prod_mapping = readInFile('df_prod_mapping')
    
    df_raw_data = readInFile('df_max_raw_data')
    
    df_ims_sales_all = readInFile('df_cn_ims_sales_fdata')
    
    if add_imsinfo_version != 'Empty':
        df_ims_sales = df_ims_sales_all.where(col('version') != add_imsinfo_version)
        df_add_imsinfo_file = df_ims_sales_all.where(col('version') == add_imsinfo_version)
    else: 
        df_ims_sales = df_ims_sales_all
  
    
    # %% 
    # =========== 数据清洗 =============
    logger.debug('数据清洗-start')
    # 函数定义
    def getTrueCol(df, l_colnames, l_df_columns):
        # 检索出正确列名
        l_true_colname = []
        for i in l_colnames:
            if i.lower() in l_df_columns and df.where(~col(i).isNull()).count() > 0:
                l_true_colname.append(i)
        if len(l_true_colname) > 1:
           raise ValueError('有重复列名: %s' %(l_true_colname))
        if len(l_true_colname) == 0:
           raise ValueError('缺少列信息: %s' %(l_colnames)) 
        return l_true_colname[0]  
    
    def getTrueColRenamed(df, dict_cols, l_df_columns):
        # 对列名重命名
        for i in dict_cols.keys():
            true_colname = getTrueCol(df, dict_cols[i], l_df_columns)
            logger.debug(true_colname)
            if true_colname != i:
                if i in l_df_columns:
                    # 删除原表中已有的重复列名
                    df = df.drop(i)
                df = df.withColumnRenamed(true_colname, i)
        return df
    
    def dealScheme(df, dict_scheme):
        # 数据类型处理
        for i in dict_scheme.keys():
            df = df.withColumn(i, col(i).cast(dict_scheme[i]))
        return df

    # 1、prod_mapping 清洗
    dict_cols_prod_map = {"通用名":["通用名", "标准通用名", "通用名_标准", "药品名称_标准", "s_molecule_name"], 
                          "min2":["min2", "min1_标准"],
                          "pfc":['pfc', 'packcode', 'pack_id', 'packid', 'packid'],
                          "标准商品名":["标准商品名", "商品名_标准", "s_molecule_name"],
                          "标准剂型":["标准剂型", "剂型_标准", 'form_std', 's_dosage'],
                          "标准规格":["标准规格", "规格_标准", 'specifications_std', '药品规格_标准', 's_pack'],
                          "标准包装数量":["标准包装数量", "包装数量2", "包装数量_标准", 'pack_number_std', 's_packnumber', "最小包装数量"],
                          "标准生产企业":["标准生产企业", "标准企业", "生产企业_标准", 'manufacturer_std', 's_corporation', "标准生产厂家"]
                         }
    df_prod_mapping = getTrueColRenamed(df_prod_mapping, dict_cols_prod_map, df_prod_mapping.columns)
    df_prod_mapping = dealScheme(df_prod_mapping, {"标准包装数量":"int", "pfc":"int"})
    df_prod_mapping = df_prod_mapping.withColumn("min1", func.regexp_replace("min1", "&amp;", "&")) \
                                    .withColumn("min1", func.regexp_replace("min1", "&lt;", "<")) \
                                    .withColumn("min1", func.regexp_replace("min1", "&gt;", ">"))
    # c. pfc为0统一替换为null
    df_prod_mapping = df_prod_mapping.withColumn("pfc", func.when(col('pfc') == 0, None).otherwise(col('pfc')))
    df_prod_mapping = df_prod_mapping.select("min1", "pfc", "通用名", "标准商品名") \
                                    .distinct() \
                                    .withColumnRenamed("pfc", "pack_id")  
    
    # %%
    # ==========  数据执行  ============
    
    # ====  一. 函数定义  ====
    @udf(StringType())
    def city_change(name):
        # 城市名定义
        if name in ["福州市", "厦门市", "泉州市"]:
            newname = "福厦泉"
        elif name in ["珠海市", "东莞市", "中山市", "佛山市"]:
            newname = "珠三角"
        elif name in ["绍兴市", "嘉兴市", "台州市", "金华市"]:
            newname = "浙江市"
        elif name in ["苏州市", "无锡市"]:
            newname = "苏锡市"
        else:
            newname = name
        return newname
             
    # ID 的长度统一
    def dealIDLength(df, colname='ID'):
        # ID不足7位的前面补0到6位
        # 国药诚信医院编码长度是7位数字，cpa医院编码是6位数字
        # 其他来源的ID 还有包含字母的, 所以要为字符型，不能转为 数值型
        df = df.withColumn(colname, col(colname).cast(StringType()))
        # 去掉末尾的.0
        df = df.withColumn(colname, func.regexp_replace(colname, "\\.0", ""))
        df = df.withColumn(colname, func.when(func.length(col(colname)) < 7, func.lpad(col(colname), 6, "0")).otherwise(col(colname)))
        return df
    
    # ====  二. 数据准备  ====          
    # 1. mkt_mapping 文件
    df_mkt_mapping = df_mkt_mapping.select('mkt', '标准通用名').distinct() \
                            .withColumnRenamed("标准通用名", "通用名")
        
    # 2. raw_data 文件
    # min1 生成
    df_raw_data = df_raw_data.select('Date', 'ID', 'Raw_Hosp_Name', 'Brand', 'Form', 'Specifications', 'Pack_Number', 'Manufacturer', 'Molecule', 'Source', 'Corp', 'Route', 'ORG_Measure', 'Sales', 'Units', 'Units_Box', 'Path', 'Sheet')
    df_raw_data = df_raw_data.withColumn("Brand", func.when((col('Brand').isNull()) | (col('Brand') == 'NA'), col('Molecule')). \
                                             otherwise(col('Brand')))
    df_raw_data = df_raw_data.withColumn('Pack_Number', col('Pack_Number').cast(StringType()))
    df_raw_data = df_raw_data.withColumn('min1', func.concat_ws(minimum_product_sep, 
                                    *[func.when(col(i).isNull(), func.lit("NA")).otherwise(col(i)) for i in minimum_product_columns]))
    
    # b.字段类型修改
    df_raw_data = df_raw_data.withColumn('Year', func.substring(col('Date').cast(StringType()), 1, 4))
    df_raw_data = df_raw_data.withColumn('Year', col('Year').cast(StringType())) \
                            .withColumn('Date', col('Date').cast(IntegerType())) \
                            .withColumn('Sales', col('Sales').cast(DoubleType())) \
                            .withColumn('Units', col('Sales').cast(DoubleType())) \
                            .withColumn('Units_Box', col('Units_Box').cast(DoubleType()))
    
    # c.匹配信息: Pack_ID,通用名,标准商品名; mkt; PHA
    df_raw_data2 = dealIDLength(df_raw_data)
    df_raw_data2 = df_raw_data2.join(df_prod_mapping, on='min1', how='left')
    df_raw_data2 = df_raw_data2.join(df_mkt_mapping, on='通用名', how='left')    
    df_raw_data2 = df_raw_data2.withColumn('标准商品名', func.when(col('标准商品名').isNull(), col('通用名')).otherwise(col('标准商品名')))
    
    # 3. ims 数据
    geo_name = {'BJH':'北京市', 'BOI':'天津市', 'BOJ':'济南市', 'CGH':'常州市', 'CHT':'全国', 'FXQ':'福厦泉', 
                'GZH':'广州市', 'NBH':'宁波市', 'SHH':'上海市', 'WZH':'温州市', 'SXC':'苏锡市',
               'YZH':'杭州市', 'HCG':'长沙市', 'ZZH':'郑州市', 'YZW':'武汉市', 'XAH':'西安市', 'SIQ':'重庆市',
               'CTX':'浙江市', 'YZN':'南京市', 'NNH':'南宁市', 'SID':'成都市', 'HFH':'合肥市', 'HRH':'哈尔滨市',
               'SZH':'深圳市', 'WLH':'乌鲁木齐市', 'PRV':'珠三角', 'CCH':'长春市', 'NCG':'南昌市', 'SJH':'石家庄市',
               'GYH':'贵阳市', 'BOS':'沈阳市', 'LZH':'兰州市', 'BOD':'大连市', 'KMH':'昆明市', 'BOQ':'青岛市', 'TYH':'太原市'}
    df_ims_sales = df_ims_sales.select('Geography_id', 'Pack_ID', 'Period_Code', 'LC')
    
    if add_imsinfo_version != 'Empty':
        df_add_imsinfo_file = df_add_imsinfo_file.select('Geography_id', 'Pack_ID', 'Period_Code', 'LC')
        # 去掉add_imsinfo_file中有的
        df_ims_sales_keep = df_ims_sales.join(df_add_imsinfo_file, on=["Pack_ID", "Geography_id"], how='left_anti')
        df_ims_sales = df_ims_sales_keep.union(df_add_imsinfo_file.select(df_ims_sales_keep.columns))
    
    geo_dict={}
    geo_dict['Geography_id'] = list(geo_name.keys())
    geo_dict['City'] = list(geo_name.values())
    geo_df = pd.DataFrame(geo_dict)
    geo_df = spark.createDataFrame(geo_df)  
    
    df_ims_sales = df_ims_sales.withColumn('Year', (func.regexp_replace(col('Period_Code'), 'M', '')/100).cast(IntegerType())) \
                        .join(geo_df.drop_duplicates(['Geography_id']), on='Geography_id', how='inner') \
                        .join(df_raw_data2.select('Pack_ID', 'mkt', '标准商品名').dropDuplicates(['Pack_ID']), 
                                            on='Pack_ID', how='inner')
    df_ims_sales = df_ims_sales.withColumn('Year',col('Year').cast(StringType()))
    
    df_ims_sales = df_ims_sales.where(col('Year').isin(year_list)).persist()
    
    # %%
    # ====  三. 市场分析  ====     
    market_list = list(market_city_brand_dict.keys()) 
    
    '''
    这个字典最好根据IMS数据自动生成
        本例是贝达项目的宁波市调整

    若想自动优化各个城市，可以循环多重字典。字典生成需要：
        1. 处理IMS数据，计算增长率等
        2. 按 "城市 -> 类别 -> 产品" 的层级生成字典
    '''

    # ims 数据
    df_ims_sales_mkt = df_ims_sales.where(col('mkt').isin(market_list))
    df_ims_sales_brand = df_ims_sales_mkt.groupBy('mkt', 'City', '标准商品名', 'Year').agg(func.sum('LC').alias('Sales'))
    df_ims_sales_city = df_ims_sales_mkt.groupBy('mkt', 'City', 'Year').agg(func.sum('LC').alias('Sales_city'))

    df_ims_sales_gr = df_ims_sales_brand.join(df_ims_sales_city, on=['mkt', 'City', 'Year'], how='left').persist()
    df_ims_sales_gr = df_ims_sales_gr.groupBy('mkt', 'City', '标准商品名').pivot('Year') \
                            .agg(func.sum('Sales').alias('Sales'), func.sum('Sales_city').alias('Sales_city')) \
                            .fillna(0)
    df_ims_sales_gr = df_ims_sales_gr.withColumn('gr', col(year_max + '_Sales')/col(year_min + '_Sales')) \
                               .withColumn('share', col(year_max + '_Sales')/col(year_max + '_Sales_city')) \
                               .withColumn('share_ly', col(year_min + '_Sales')/col(year_min + '_Sales_city'))
    df_ims_sales_gr = df_ims_sales_gr.select('mkt', 'City', '标准商品名', 'gr', 'share', 'share_ly') \
                                .withColumnRenamed('mkt', 'DOI')

    # %%
    # =========== 数据输出 =============
    def lowerColumns(df):
        df = df.toDF(*[i.lower() for i in df.columns])
        return df
    df_ims_sales_gr = lowerColumns(df_ims_sales_gr)
    
    return {"out_df":df_ims_sales_gr}

    '''                           
    # 测试用
    ims_sales_gr = spark.read.csv('s3a://ph-max-auto/v0.0.1-2020-06-08/Test/Eisai/Share_total.csv', header=True)
    ims_sales_gr = spark.read.csv('s3a://ph-max-auto/v0.0.1-2020-06-08/Test/Merck/Share_total_merk.csv', header=True)
    ims_sales_gr = ims_sales_gr.select('IMS_gr', 'IMS_share_2019', 'IMS_share_2018', 'Brand', 'City', 'DOI') \
                                .withColumn('IMS_gr', col('IMS_gr').cast(DoubleType())) \
                                .withColumn('IMS_share_2019', col('IMS_share_2019').cast(DoubleType())) \
                                .withColumn('IMS_share_2018', col('IMS_share_2018').cast(DoubleType())) \
                                .withColumnRenamed('IMS_gr', 'gr') \
                                .withColumnRenamed('IMS_share_2019', 'share') \
                                .withColumnRenamed('IMS_share_2018', 'share_ly') \
                                .fillna(0)
    ims_sales_gr = ims_sales_gr.where(col('Brand') != 'total')
    ims_sales_gr = ims_sales_gr.where(col('DOI') == market) \
                                .withColumnRenamed('Brand', '标准商品名')
    # 测试用 over
    '''
    
    def noUse()  :
        data_count = data
        #选择要进行计算增长率的分子
        data_count = data_count.groupBy('Year','标准商品名','City').agg(func.sum('Sales').alias('Sales'))
        data_count = data_count.groupBy('City','标准商品名').pivot('Year').agg(func.sum('Sales')).fillna(0).persist()
        data_count = data_count.withColumn('gr', col(year_max)/col(year_min))
        all_sales = data_count.groupBy('City').agg(func.sum(year_min).alias(year_min + '_sales'), func.sum(year_max).alias(year_max + '_sales'))

        data_count = data_count.join(all_sales, on='City', how='inner')
        gr_city = data_count.groupBy('City').agg(func.sum(year_min + '_sales').alias(year_min), func.sum(year_min + '_sales').alias(year_max))
        gr_city = gr_city.withColumn('gr', col(year_max)/col(year_min)) \
                        .withColumnRenamed('City', 'city')

        data_count = data_count.withColumn('share', col(year_max)/col(year_max + '_sales')) \
                                .withColumn('share_ly', col(year_min)/col(year_min + '_sales'))
        data_count = data_count.select('City','标准商品名','gr','share','share_ly').distinct() \
                                .fillna(0)
