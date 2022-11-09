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
    market_city_brand = kwargs['market_city_brand']
    universe_choice = kwargs['universe_choice']
    job_choice = kwargs['job_choice']
    year_list = kwargs['year_list']
    add_imsinfo_path = kwargs['add_imsinfo_path']
    ims_sales_path = kwargs['ims_sales_path']
    ### input args ###
    
    ### output args ###
    c = kwargs['c']
    d = kwargs['d']
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
    import boto3    # %%
    '''
    # 测试
    year_list = '2019,2020'
    project_name = "灵北"
    outdir = "202012"
    market_city_brand = "精神:常州市_3"
    job_choice = "weight"
    minimum_product_sep = "|"
    #add_imsinfo_path = 's3a://ph-max-auto/v0.0.1-2020-06-08/Takeda/add_ims_info.csv'
    '''
    # %%
    # 输入
    
    # 是否运行此job
    if job_choice != "weight":
         raise ValueError('不运行weight')
    
    logger.debug(market_city_brand)
    
    # year_list=['2018', '2019']
    year_list = year_list.replace(" ","").split(",")
    year_min = year_list[0]
    year_max = year_list[1]
    
    universe_choice_dict={}
    if universe_choice != "Empty":
        for each in universe_choice.replace(" ","").split(","):
            market_name = each.split(":")[0]
            universe_name = each.split(":")[1]
            universe_choice_dict[market_name]=universe_name
    
    # market_city_brand_dict = json.loads(market_city_brand)
    minimum_product_columns = minimum_product_columns.replace(" ","").split(",")
    if minimum_product_sep == "kong":
        minimum_product_sep = ""
    
    market_city_brand_dict={}
    for each in market_city_brand.replace(" ","").split(","):
        market_name = each.split(":")[0]
        if market_name not in market_city_brand_dict.keys():
            market_city_brand_dict[market_name]={}
        city_brand = each.split(":")[1]
        for each in city_brand.replace(" ","").split("|"): 
            city = each.split("_")[0]
            brand = each.split("_")[1]
            market_city_brand_dict[market_name][city]=brand
    logger.debug(market_city_brand_dict)
    
    id_bedsize_path = max_path + '/' + '/Common_files/ID_Bedsize'
    universe_path = max_path + '/' + project_name + '/universe_base'
    city_info_path = max_path + '/' + project_name + '/province_city_mapping'
    mkt_mapping_path = max_path + '/' + project_name + '/mkt_mapping'
    cpa_pha_mapping_path = max_path + '/' + project_name + '/cpa_pha_mapping'
    product_map_path = max_path + '/' + project_name + '/' + outdir + '/prod_mapping'
    raw_data_path = max_path + '/' + project_name + '/' + outdir + '/raw_data'
    # ims_sales_path = max_path + "/Common_files/IMS_flat_files/202012/cn_IMS_Sales_Fdata_202012_1.csv"
    # raw_data = spark.read.csv('s3a://ph-max-auto/v0.0.1-2020-06-08/Test/Merck/raw_data.csv', header=True)
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
            
    def deal_ID_length(df):
        # ID不足7位的补足0到6位
        # 国药诚信医院编码长度是7位数字，cpa医院编码是6位数字。
        df = df.withColumn("ID", df["ID"].cast(StringType()))
        # 去掉末尾的.0
        df = df.withColumn("ID", func.regexp_replace("ID", "\\.0", ""))
        df = df.withColumn("ID", func.when(func.length(df.ID) < 7, func.lpad(df.ID, 6, "0")).otherwise(df.ID))
        return df
    
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
        if i in ["商品名_标准", "S_Product_Name"]:
            product_map = product_map.withColumnRenamed(i, "标准商品名")
    # b. 选取需要的列
    product_map = product_map \
                    .select("min1", "pfc", "通用名", "标准商品名") \
                    .withColumn("pfc", product_map["pfc"].cast(IntegerType())) \
                    .distinct()
    # c. pfc为0统一替换为null
    product_map = product_map.withColumn("pfc", func.when(product_map.pfc == 0, None).otherwise(product_map.pfc)).distinct()
    # d. min2处理
    product_map = product_map.withColumnRenamed("pfc", "Pack_ID") \
                    .withColumn("min1", func.regexp_replace("min1", "&amp;", "&")) \
                    .withColumn("min1", func.regexp_replace("min1", "&lt;", "<")) \
                    .withColumn("min1", func.regexp_replace("min1", "&gt;", ">"))
    
    # 2. universe 文件                  
    universe = spark.read.parquet(universe_path)
    universe = universe.select("Panel_ID", "Province", "City", "PANEL") \
                        .withColumnRenamed("Panel_ID", "PHA") \
                        .withColumn("PANEL", col("PANEL").cast(DoubleType())) \
                        .distinct()
    
    
    # 3. mkt_mapping 文件
    mkt_mapping = spark.read.parquet(mkt_mapping_path)
    mkt_mapping = mkt_mapping.select('mkt', '标准通用名').distinct() \
                            .withColumnRenamed("标准通用名", "通用名")
    
    # 4. cpa_pha_mapping 文件
    cpa_pha_mapping = spark.read.parquet(cpa_pha_mapping_path)
    cpa_pha_mapping = deal_ID_length(cpa_pha_mapping)
    cpa_pha_mapping = cpa_pha_mapping.where(col("推荐版本") == 1).select('ID', 'PHA').distinct()
    
    
    # 5. city_info_path
    city_info = spark.read.parquet(city_info_path)
    city_info = deal_ID_length(city_info)
    city_info = city_info.select('ID', 'Province', 'City') \
                        .withColumnRenamed('City', 'City_add').distinct()
    
    
    # 6. id_bedsize
    schema= StructType([
            StructField("ID", StringType(), True),
            StructField("Bedsize", DoubleType(), True),
            StructField("Bedsize>99", DoubleType(), True)
            ])
    id_bedsize = spark.read.parquet(id_bedsize_path, schema=schema)
    id_bedsize = deal_ID_length(id_bedsize)
    id_bedsize = id_bedsize.join(cpa_pha_mapping, on='ID', how='left')
    
    # 7. raw_data 文件
    raw_data = spark.read.parquet(raw_data_path)
    # a. 生成min1
    if project_name != "Mylan":
        raw_data = raw_data.withColumn("Brand", func.when((raw_data.Brand.isNull()) | (raw_data.Brand == 'NA'), raw_data.Molecule).
                                   otherwise(raw_data.Brand))
    for colname, coltype in raw_data.dtypes:
        if coltype == "logical":
            raw_data = raw_data.withColumn(colname, raw_data[colname].cast(StringType()))
    
    raw_data = raw_data.withColumn("tmp", func.when(func.isnull(raw_data[minimum_product_columns[0]]), func.lit("NA")).
                                   otherwise(raw_data[minimum_product_columns[0]]))
    
    for i in minimum_product_columns[1:]:
        raw_data = raw_data.withColumn(i, raw_data[i].cast(StringType()))
        raw_data = raw_data.withColumn("tmp", func.concat(
            raw_data["tmp"],
            func.lit(minimum_product_sep),
            func.when(func.isnull(raw_data[i]), func.lit("NA")).otherwise(raw_data[i])))
    # Mylan不重新生成minimum_product_newname: min1，其他项目生成min1
    if project_name == "Mylan":
        raw_data = raw_data.drop("tmp")
    else:
        if 'min1' in raw_data.columns:
            raw_data = raw_data.drop('min1')
        raw_data = raw_data.withColumnRenamed('tmp', 'min1')
    
    # b.字段类型修改
    raw_data = raw_data.withColumn('Year', func.substring(col('Date').cast(StringType()), 1, 4))
    raw_data = raw_data.withColumn('Year', col('Year').cast(StringType())) \
                            .withColumn('Date', col('Date').cast(IntegerType())) \
                            .withColumn('Sales', col('Sales').cast(DoubleType())) \
                            .withColumn('Units', col('Sales').cast(DoubleType())) \
                            .withColumn('Units_Box', col('Units_Box').cast(DoubleType()))
    
    # c.匹配信息: Pack_ID,通用名,标准商品名; mkt; PHA
    raw_data2 = deal_ID_length(raw_data)
    raw_data2 = raw_data2.join(product_map, on='min1', how='left')
    raw_data2 = raw_data2.join(mkt_mapping, on='通用名', how='left')
    raw_data2 = raw_data2.join(cpa_pha_mapping, on='ID', how='left')
    raw_data2 = raw_data2.join(universe.select("PHA", "Province", "City").distinct(), on='PHA', how='left')
    
    raw_data2 = raw_data2.withColumn('标准商品名', func.when(col('标准商品名').isNull(), col('通用名')).otherwise(col('标准商品名')))
    
    # 8. ims 数据
    geo_name = {'BJH':'北京市', 'BOI':'天津市', 'BOJ':'济南市', 'CGH':'常州市', 'CHT':'全国', 'FXQ':'福厦泉', 
                'GZH':'广州市', 'NBH':'宁波市', 'SHH':'上海市', 'WZH':'温州市', 'SXC':'苏锡市',
               'YZH':'杭州市', 'HCG':'长沙市', 'ZZH':'郑州市', 'YZW':'武汉市', 'XAH':'西安市', 'SIQ':'重庆市',
               'CTX':'浙江市', 'YZN':'南京市', 'NNH':'南宁市', 'SID':'成都市', 'HFH':'合肥市', 'HRH':'哈尔滨市',
               'SZH':'深圳市', 'WLH':'乌鲁木齐市', 'PRV':'珠三角', 'CCH':'长春市', 'NCG':'南昌市', 'SJH':'石家庄市',
               'GYH':'贵阳市', 'BOS':'沈阳市', 'LZH':'兰州市', 'BOD':'大连市', 'KMH':'昆明市', 'BOQ':'青岛市', 'TYH':'太原市'}
    
    
    ims_sales = spark.read.csv(ims_sales_path, header=True)
    ims_sales = ims_sales.select('Geography_id', 'Pack_ID', 'Period_Code', 'LC')
    
    if add_imsinfo_path != 'Empty':
        add_imsinfo_file = spark.read.csv(add_imsinfo_path, header=True)
        add_imsinfo_file = add_imsinfo_file.select('Geography_id', 'Pack_ID', 'Period_Code', 'LC')
        # 去掉add_imsinfo_file中有的
        ims_sales_keep = ims_sales.join(add_imsinfo_file, on=["Pack_ID", "Geography_id"], how='left_anti')
        ims_sales = ims_sales_keep.union(add_imsinfo_file.select(ims_sales_keep.columns))
    
    geo_dict={}
    geo_dict['Geography_id'] = list(geo_name.keys())
    geo_dict['City'] = list(geo_name.values())
    geo_df = pd.DataFrame(geo_dict)
    geo_df = spark.createDataFrame(geo_df)  
    
    ims_sales = ims_sales.withColumn('Year', (func.regexp_replace(col('Period_Code'), 'M', '')/100).cast(IntegerType())) \
                        .join(geo_df.drop_duplicates(['Geography_id']), on='Geography_id', how='inner') \
                        .join(raw_data2.select('Pack_ID', 'mkt', '标准商品名').dropDuplicates(['Pack_ID']), 
                                            on='Pack_ID', how='inner')
    ims_sales = ims_sales.withColumn('Year',col('Year').cast(StringType()))
    
    ims_sales = ims_sales.where(col('Year').isin(year_list)).persist()
    # %%
    # ====  三. 每个市场分析  ==== 
    
    market_list = list(market_city_brand_dict.keys())
    
    for market in market_list:
        # 输入文件
        logger.debug(market)
        factor_path = max_path + '/' + project_name + '/factor/factor_' + market
        universe_ot_path = max_path + '/' + project_name + '/universe/universe_ot_' + market
    
        # 输出文件
        data_target_path = max_path + '/' + project_name + '/weight/' + market + '_data_target'
        ims_gr_path = max_path + '/' + project_name + '/weight/' + market + '_ims_gr'
    
        # 1. 数据准备
        # universe_ot 文件        
        universe_ot = spark.read.parquet(universe_ot_path)
        universe_ot = universe_ot.withColumnRenamed('Panel_ID', 'PHA')
        # universe 文件: 如果有指定的universe则重新读取，否则就用已有的 universe_base
        if market in universe_choice_dict.keys():
            universe_path = max_path + '/' + project_name + '/' + universe_choice_dict[market]
            universe = spark.read.parquet(universe_path)
            universe = universe.select("Panel_ID", "Province", "City", "PANEL") \
                                .withColumnRenamed("Panel_ID", "PHA") \
                                .withColumn("PANEL", col("PANEL").cast(DoubleType())) \
                                .distinct()
    
        # factor 文件    
        factor = spark.read.parquet(factor_path)
        if "factor" not in factor.columns:
            factor = factor.withColumnRenamed("factor_new", "factor")
        factor = factor.select('City', 'factor').distinct()
    
        # raw_data
        data = raw_data2.where(col('mkt') == market)
    
        # 2. 权重计算
        '''
        权重计算：
            利用医药收入和城市Factor
            从universe中去掉outliers以及其他Panel == 0的样本
        '''
        # %% 目的是实现 PANEL = 0 的样本回填
        pha_list = universe.where(col('PANEL') == 0).where(~col('PHA').isNull()).select('PHA').distinct().toPandas()['PHA'].tolist()
        pha_list2 = data.where(col('Date') > 202000).where(~col('PHA').isNull()).select('PHA').distinct().toPandas()['PHA'].tolist()
    
        universe_ot_rm = universe_ot.where((col('PANEL') == 1) | (col('PHA').isin(pha_list))) \
                                .where( ~((col('PANEL') == 0) & (col('PHA').isin(pha_list2)) )) \
                                .fillna(0, subset=['Est_DrugIncome_RMB', 'BEDSIZE']) \
                                .withColumn('Panel_income', col('Est_DrugIncome_RMB') * col('PANEL')) \
                                .withColumn('Bedmark', func.when(col('BEDSIZE') >= 100, func.lit(1)).otherwise(func.lit(0)))
        universe_ot_rm = universe_ot_rm.withColumn('non_Panel_income', col('Est_DrugIncome_RMB') * (1-col('PANEL')) * col('Bedmark'))
    
        Seg_Panel_income = universe_ot_rm.groupBy('Seg').agg(func.sum('Panel_income').alias('Seg_Panel_income'))
        Seg_non_Panel_income = universe_ot_rm.groupBy('Seg', 'City') \
                                    .agg(func.sum('non_Panel_income').alias('Seg_non_Panel_income'))
        universe_ot_rm = universe_ot_rm.join(Seg_Panel_income, on='Seg', how='left') \
                                    .join(Seg_non_Panel_income, on=['Seg', 'City'], how='left').persist()
    
        # %% 按Segment+City拆分样本
        seg_city = universe_ot_rm.select('Seg','City','Seg_non_Panel_income','Seg_Panel_income').distinct()
    
        seg_pha = universe_ot_rm.where(col('PANEL') == 1).select('Seg','City','PHA','BEDSIZE').distinct() \
                                .withColumnRenamed('City', 'City_Sample')
    
        weight_table = seg_city.join(seg_pha, on='Seg', how='left') \
                                .join(factor, on='City', how='left').persist()
    
        # 只给在城市内100床位以上的样本，添加 1 
        weight_table = weight_table.withColumn('tmp', func.when((col('City') == col('City_Sample')) & (col('BEDSIZE') > 99) , \
                                                            func.lit(1)).otherwise(func.lit(0)))
        weight_table = weight_table.withColumn('weight', col('tmp') + \
                                                    col('factor') * col('Seg_non_Panel_income') / col('Seg_Panel_income'))
    
        # pandas当 Seg_non_Panel_income和Seg_Panel_income都为0结果是 null，只有 Seg_Panel_income 是 0 结果为 inf 
        # pyspark 都会是null, 用-999代表无穷大
        weight_table = weight_table.withColumn('weight', func.when((col('Seg_Panel_income') == 0) & (col('Seg_non_Panel_income') != 0), \
                                                               func.lit(-999)).otherwise(col('weight')))
        weight_init = weight_table.select('Seg','City','PHA','weight','City_Sample','BEDSIZE').distinct()
    
        # %% 权重与PANEL的连接
        # 置零之和 不等于 和的置零，因此要汇总到Date层面
        data_sub = data.groupBy('PHA','ID','Date', 'Year').agg(func.sum('Sales').alias('Sales'))
    
        tmp_weight = weight_init.join(data_sub, on='PHA', how='outer').persist()
        tmp_weight = tmp_weight.fillna({'weight':1, 'Sales':0})
        # 无穷大仍然为 null 
        tmp_weight = tmp_weight.withColumn('weight', func.when(col('weight') == -999, func.lit(None)).otherwise(col('weight')))
    
        # %% 匹配省份城市
        tmp_city = tmp_weight.join(city_info, on='ID', how='left')
        tmp_city = tmp_city.withColumn('City', func.when(col('City').isNull(), col('City_add')).otherwise(col('City'))) \
                    .withColumn('City_Sample', func.when(col('City_Sample').isNull(), col('City_add')) \
                                                    .otherwise(col('City_Sample')))
    
        # %% 床位数匹配以及权重修正
        tmp_bed =  tmp_city.join(id_bedsize.select('ID', 'Bedsize>99').distinct(), on='ID', how='left')
    
        # 不在大全的床位数小于1的，权重减1，等同于剔除
        tmp_bed = tmp_bed.withColumn('weight', func.when((col('Bedsize>99') == 0) & (col('PHA').isNull()), \
                                                    col('weight')-1).otherwise(col('weight')))
    
        # %% MAX结果计算
        tmp_bed = tmp_bed.withColumn('MAX_' + year_max, col('Sales') * col('weight'))
        tmp_bed_seg = tmp_bed.groupBy('Seg','Date').agg(func.sum('MAX_' + year_max).alias('MAX_' + year_max)).persist()
        tmp_bed_seg = tmp_bed_seg.withColumn('Positive', func.when(col('MAX_' + year_max) >= 0, func.lit(1)).otherwise(func.lit(0)))
        tmp_bed = tmp_bed.join(tmp_bed_seg.select('Seg','Positive').distinct(), on='Seg', how='left')
        tmp_bed = tmp_bed.fillna({'Positive':1})
    
        #tmp_max_city = tmp_bed.where(col('Positive') == 1).where(~col('Year').isNull()) \
        #                    .groupBy('City','Year') \
        #                    .agg(func.sum('MAX_2019').alias('MAX_2019'), func.sum('Sales').alias('Sales')).persist()
    
        # %% 权重初始值
        weight_0 = tmp_bed.select('City','PHA','Positive','City_Sample','Seg','weight') \
                            .withColumn('City', city_change(col('City'))) \
                            .withColumn('City_Sample', city_change(col('City_Sample'))).distinct()
    
    
        # 3. 需要优化的城市数据
        # 城市列表
        city_brand_dict = market_city_brand_dict[market]
        city_list = list(city_brand_dict.keys())
    
        # %% 观察放大结果
        data_target = data.where(col('Year').isin(year_list)) \
                            .select('Date','PHA','标准商品名','Sales', 'Year').distinct() \
                            .join(weight_0, on='PHA', how='inner').persist()
        data_target = data_target.where(col('City').isin(city_list))
    
        data_target = data_target.withColumn('MAX_weighted', col('weight')*col('Sales')) \
                                .withColumn('tmp', func.concat(col('Year'), func.lit('_'), col('标准商品名'))) \
                                .join(id_bedsize.select('PHA','Bedsize>99').dropDuplicates(['PHA']), on='PHA', how='left')
    
        # 输出
        data_target = data_target.repartition(1)
        data_target.write.format("parquet") \
            .mode("overwrite").save(data_target_path)
    
        # 4. ims数据
        '''
        这个字典最好根据IMS数据自动生成
            本例是贝达项目的宁波市调整
    
        若想自动优化各个城市，可以循环多重字典。字典生成需要：
            1. 处理IMS数据，计算增长率等
            2. 按 "城市 -> 类别 -> 产品" 的层级生成字典
        '''
        if False:
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
    
        # ims 数据
        ims_sales_mkt = ims_sales.where(col('mkt') == market)
        ims_sales_brand = ims_sales_mkt.groupBy('mkt', 'City', '标准商品名', 'Year').agg(func.sum('LC').alias('Sales'))
        ims_sales_city = ims_sales_mkt.groupBy('mkt', 'City', 'Year').agg(func.sum('LC').alias('Sales_city'))
    
        ims_sales_gr = ims_sales_brand.join(ims_sales_city, on=['mkt', 'City', 'Year'], how='left').persist()
        ims_sales_gr = ims_sales_gr.groupBy('mkt', 'City', '标准商品名').pivot('Year') \
                                .agg(func.sum('Sales').alias('Sales'), func.sum('Sales_city').alias('Sales_city')) \
                                .fillna(0)
        ims_sales_gr = ims_sales_gr.withColumn('gr', col(year_max + '_Sales')/col(year_min + '_Sales')) \
                                   .withColumn('share', col(year_max + '_Sales')/col(year_max + '_Sales_city')) \
                                   .withColumn('share_ly', col(year_min + '_Sales')/col(year_min + '_Sales_city'))
        ims_sales_gr = ims_sales_gr.select('mkt', 'City', '标准商品名', 'gr', 'share', 'share_ly') \
                                    .withColumnRenamed('mkt', 'DOI')
    
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
    
        ims_sales_gr = ims_sales_gr.repartition(1)
        ims_sales_gr.write.format("parquet") \
            .mode("overwrite").save(ims_gr_path)
