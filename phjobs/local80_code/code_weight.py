# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

# from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL


def default(kwargs):
    # ### input args ###
    max_path = kwargs.get('max_path', 'D:/Auto_MAX/MAX/')
    project_name = kwargs['project_name']
    universe_choice = kwargs.get('universe_choice', 'Empty')
    all_models = kwargs['all_models']
    weight_upper = kwargs.get('weight_upper', '1.25')
    job_choice = kwargs['job_choice']
    test = kwargs.get('test', 'False')
    # ### input args ###
    '''
    max_path = '../MAX'
    project_name = "Gilead"
    universe_choice = "乙肝:universe_传染,乙肝_2:universe_传染,乙肝_3:universe_传染"
    all_models = "乙肝,乙肝_2,乙肝_3"
    weight_upper = "1.25"
    job_choice = "weight_default"
    test = "False"
    '''


    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    import os
    from pyspark.sql.functions import pandas_udf, PandasUDFType, col, udf
    
    from scipy.stats import ranksums, mannwhitneyu
    import pandas as pd
    import numpy as np
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    # %%
    # project_name = "Gilead"
    # universe_choice = "乙肝:universe_传染,乙肝_2:universe_传染,乙肝_3:universe_传染,安必素:universe_传染"
    # all_models = "乙肝,乙肝_2,乙肝_3,安必素"
    # weight_upper = "1.25"
    # job_choice = "weight_default"
    # test = "True"

    # %%
    # 是否运行此job
    if test != "False" and test != "True":
        # logger.info('wrong input: test, False or True')
        raise ValueError('wrong input: test, False or True')
    
    if job_choice != "weight_default":
         raise ValueError('不运行weight_default')

    # 输入
    universe_choice_dict={}
    if universe_choice != "Empty":
        for each in universe_choice.replace(" ","").split(","):
            market_name = each.split(":")[0]
            universe_name = each.split(":")[1]
            universe_choice_dict[market_name]=universe_name
    
    all_models = all_models.replace(", ",",").split(",")
    weight_upper = float(weight_upper)
    
    # 输出
    project_path = project_path = max_path + "/" + project_name
    if test == "True":
        weight_default_path = max_path + "/" + project_name + '/test/PHA_weight_default'
    else:
        weight_default_path = max_path + "/" + project_name + '/PHA_weight_default'
    

    # %%
    # ====  数据分析  ====
    print('default')
    for index, market in enumerate(all_models):
        if market in universe_choice_dict.keys():
            universe_path = project_path + '/' + universe_choice_dict[market]
        else:
            universe_path = project_path + '/universe_base'
    
        universe = spark.read.parquet(universe_path)
        universe = universe.fillna(0, 'Est_DrugIncome_RMB') \
                            .withColumn('Est_DrugIncome_RMB', func.when(func.isnan('Est_DrugIncome_RMB'), 0).otherwise(col('Est_DrugIncome_RMB')))
        
        # 数据处理
        universe_panel = universe.where(col('PANEL') == 1).select('Panel_ID', 'Est_DrugIncome_RMB', 'Seg')
        universe_non_panel = universe.where(col('PANEL') == 0).select('Est_DrugIncome_RMB', 'Seg', 'City', 'Province')
        
        seg_multi_cities = universe.select('Seg', 'City', 'Province').distinct() \
                                .groupby('Seg').count()
        seg_multi_cities = seg_multi_cities.where(col('count') > 1).select('Seg').toPandas()['Seg'].tolist()
    
        universe_m = universe_panel.where(col('Seg').isin(seg_multi_cities)) \
                                    .withColumnRenamed('Est_DrugIncome_RMB', 'Est_DrugIncome_RMB_x') \
                                    .join(universe_non_panel, on='Seg', how='inner')
        
        # 秩和检验获得p值
        schema = StructType([
            StructField("Panel_ID", StringType(), True),
            StructField("City", StringType(), True),
            StructField("Province", StringType(), True),
            StructField("pvalue", DoubleType(), True)
            ])
    
        @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
        def wilcoxtest(pdf):
            # 秩和检验
            Panel_ID = pdf['Panel_ID'][0]
            City = pdf['City'][0]
            Province = pdf['Province'][0]
            a = pdf['Est_DrugIncome_RMB_x'].drop_duplicates().values.astype(float)
            b = pdf['Est_DrugIncome_RMB'].values.astype(float)
            pvalue = round(mannwhitneyu(a, b, alternative="two-sided")[1],6) # 等同于R中的wilcox.test()
            return pd.DataFrame([[Panel_ID] + [City] + [Province] + [pvalue]], columns=["Panel_ID", "City", "Province", "pvalue"])
    
        universe_m_wilcox = universe_m.groupby('Panel_ID', 'City', 'Province') \
                                    .apply(wilcoxtest)
        
        universe_m_maxmin = universe_m_wilcox.groupby('Panel_ID') \
                                            .agg(func.min('pvalue').alias('min'), func.max('pvalue').alias('max'))
        
        # 计算weight
        universe_m_weight = universe_m_wilcox.join(universe_m_maxmin, on='Panel_ID', how='left') \
                                            .withColumn('Weight', 
                        (col('pvalue') - col('min'))/(col('max') - col('min'))*(weight_upper-1/weight_upper) + 1/weight_upper)
    
        universe_m_weight = universe_m_weight.fillna(1, 'Weight')
        
        weight_out = universe_m_weight.withColumn('DOI', func.lit(market)) \
                                    .withColumnRenamed('Panel_ID', 'PHA') \
                                    .select('Province', 'City', 'DOI', 'Weight', 'PHA')
        
        # 结果输出
        if index ==0:
            weight_out = weight_out.repartition(1)
            weight_out.write.format("parquet") \
                .mode("overwrite").save(weight_default_path)
        else:
            weight_out = weight_out.repartition(1)
            weight_out.write.format("parquet") \
                .mode("append").save(weight_default_path)
            

def get_weight_gr(kwargs):
    # ### input args ###
    max_path = kwargs.get('max_path', 'D:/Auto_MAX/MAX/')
    project_name = kwargs['project_name']
    outdir = kwargs['outdir']
    minimum_product_sep = kwargs.get('minimum_product_sep', '|')
    minimum_product_columns = kwargs.get('minimum_product_columns', 'Brand, Form, Specifications, Pack_Number, Manufacturer')
    market_city_brand = kwargs['market_city_brand']
    universe_choice = kwargs.get('universe_choice', 'Empty')
    job_choice = kwargs['job_choice']
    year_list = kwargs['year_list']
    add_imsinfo_path = kwargs.get('add_imsinfo_path', 'Empty')
    ims_sales_path = kwargs['ims_sales_path']
    # ### input args ###

    ### input args ###
    '''
    max_path = '../MAX'
    project_name = 'Gilead'
    outdir = '202101'
    minimum_product_sep = '|'
    minimum_product_columns = 'Brand,Form,Specifications, Pack_Number, Manufacturer'
    market_city_brand = '乙肝:北京市_3|上海市_3'
    universe_choice = '乙肝:universe_传染,乙肝_2:universe_传染,乙肝_3:universe_传染'
    job_choice = 'weight'
    year_list = '2018,2019'
    add_imsinfo_path = 'Empty'
    ims_sales_path = 'cn_IMS_Sales_Fdata_202301'
    '''
    ### input args ###


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
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

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
    
    # print(market_city_brand)
    
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
    # print(market_city_brand_dict)
    
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
    print('get_weight_gr')
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
        print(market)
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



def gradient_descent(kwargs):

    ### input args ###
    max_path = kwargs.get('max_path', 'D:/Auto_MAX/MAX/')
    project_name = kwargs['project_name']
    market_city_brand = kwargs['market_city_brand']
    lmda = kwargs.get('lmda', '0.001')
    learning_rate = kwargs.get('learning_rate', '100')
    max_iteration = kwargs.get('max_iteration', '10000')
    gradient_type = kwargs.get('gradient_type', 'both')
    test = kwargs.get('test', 'False')
    year_list = kwargs['year_list']
    ### input args ###
    

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
    spark = SparkSession.builder.getOrCreate()
    
    print('gradient_descent')
	
    if test != "False" and test != "True":
        logger.info('wrong input: test, False or True') 
        raise ValueError('wrong input: test, False or True')
    
    year_list = year_list.replace(" ","").split(",")
    year_min = year_list[0]
    year_max = year_list[1]
    
    lmda = float(lmda)
    learning_rate = int(learning_rate)
    max_iteration = int(max_iteration)
    # market_city_brand = json.loads(market_city_brand)
    universe_path = max_path + '/' + project_name + '/universe_base'
    
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
    print(market_city_brand_dict)
    
    # 输出
    weight_tmp_path = max_path + '/' + project_name + '/weight/PHA_weight_tmp'
    tmp_path = max_path + '/' + project_name + '/weight/tmp'
    if test == "False":
        weight_path = max_path + '/' + project_name + '/PHA_weight'
    else:
        weight_path = max_path + '/' + project_name + '/weight/PHA_weight'

    # %%
    # ==========  数据执行  ============
    
    # ====  一. 数据准备  ==== 
    # 1. universe 文件                  
    universe = spark.read.parquet(universe_path)
    universe = universe.select("Province", "City") \
                        .distinct()
    
    
    # ====  二. 函数定义  ====
    
    # 1. 利用 ims_sales_gr，生成城市 top 产品的 'gr','share','share_ly' 字典
    def func_target_brand(pdf, city_brand_dict):
        import json
        city_name = pdf['City'][0]
        brand_number = city_brand_dict[city_name]
        pdf = pdf.sort_values(by='share', ascending=False).reset_index()[0:int(brand_number)]
        dict_share = pdf.groupby(['City'])[['标准商品名','gr','share','share_ly']].apply(lambda x : x.set_index('标准商品名').to_dict()).to_dict()
        dict_share = json.dumps(dict_share)
        return pd.DataFrame([[city_name] + [dict_share]], columns=['City', 'dict'])
    
    schema= StructType([
            StructField("City", StringType(), True),
            StructField("dict", StringType(), True)
            ])
    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def udf_target_brand(pdf):
        return func_target_brand(pdf, city_brand_dict)
    
    # 2. 算法函数 辅助函数
    # 计算增长率或者份额
    def r(W, h_n, h_d):
        return h_n.T.dot(W)/h_d.T.dot(W)
    
    # 计算和IMS的差距
    def delta_gr(W, h_n, h_d, g):
        return (r(W, h_n, h_d) - g)
    
    # 控制W离散
    def w_discrete_ctrl(W, lmda):
        return 2*lmda*(W-np.average(W))
    
    # Loss-func求导
    '''
    loss function是差距（份额或增长）的平方
    '''
    def gradient(W, h_n, h_d, g, lmda):
        CrossDiff = (h_d.sum() - W.reshape(-1)*h_d) * h_n - \
                  (h_n.sum() - W.reshape(-1)*h_n) * h_d    
        dW = delta_gr(W, h_n, h_d, g) * np.power((h_d.T.dot(W)), -2) * CrossDiff \
          + w_discrete_ctrl(W, lmda).reshape(-1, )
        return dW.reshape(-1,1)
    
    # 梯度下降
    '''
    这是一个双任务优化求解，目标是寻找满足IMS Share and Growth的医院参数线性组合 ～ w
    可能的待优化方向：
        1. 自适应学习率
        2. 不同任务重要性，不同收敛速度，不同学习率
        3. 避免落在局部最优
        4. 初始值的选择
    '''
    
    def gradient_descent(W_0, S, G, S_ly, learning_rate, max_iteration, brand_number, H_n, H_share, H_gr, H_ly, lmda, gradient_type):
        X = np.array([]); Growth = np.array([]); Share = np.array([])
        W = W_0.copy()
        W_init = W_0.copy()
        w_gd = np.array([0]*H_n.shape[1])
        for i in range(max_iteration):
            gradient_sum = np.zeros((len(H_n),1))
            for k in range(H_n.shape[1]):
                gd_share = gradient(W, H_n[:,k], H_share, S[k], lmda)
                gd_growth = gradient(W, H_n[:,k], H_gr[:,k], G[k], lmda)
                if gradient_type == 'both':
                    gradient_sum += gd_growth + gd_share
                elif gradient_type == 'share':
                    # 只优化Share
                    gradient_sum += gd_share
                # 优化Share和增长
                elif gradient_type == 'gr':
                    gradient_sum += gd_growth
    
            gradient_sum[(W_init==0)|(W_init==1)] = 0
            W -= learning_rate * gradient_sum
            W[W<0] = 0
            #print ('iteration : ', i, '\n GR : ', r(W, H_n, H_gr),
            #    '\n Share : ', r(W, H_n, H_share))
            X = np.append(X, [i])
            Growth = np.append(Growth, r(W, H_n, H_gr))
            Share = np.append(Share, r(W, H_n, H_share))
            share_ly_hat = W.T.dot(H_gr).reshape(-1)/W.T.dot(H_ly)
            w_gd = abs(share_ly_hat/S_ly -1)
        return W, X, Growth.reshape((i+1,-1)), Share.reshape((i+1,-1))
    
    # 3. pandas_udf 执行算法，分城市进行优化
    def func_target_weight(pdf, dict_target_share, l, m, lmda, gradient_type, year_min, year_max):
        city_name = pdf['City'][0]
        target = list(dict_target_share[city_name]['gr'].keys())
        brand_number = len(target)
    
        H = pdf.pivot_table(index=['City','PHA','City_Sample','weight', 'Bedsize>99'], columns=['tmp'], 
                            values='Sales', fill_value=0, aggfunc='sum')
        H = H.reset_index()
    
        # 判断target是否在data中，如果不在给值为0(会有小城市，ims的top—n产品在data中不存在)
        for each in target:
            if year_min+'_'+each not in H.columns:
                H[year_min+'_'+each]=0
            if year_max+'_'+each not in H.columns:
                H[year_max+'_'+each]=0
                
        H[year_min+'_total'] = H.loc[:,H.columns.str.contains(year_min)].sum(axis=1)
        H[year_max+'_total'] = H.loc[:,H.columns.str.contains(year_max)].sum(axis=1)
        H[year_min+'_others'] = H[year_min+'_total'] - H.loc[:,[year_min+'_'+col for col in target]].sum(axis=1)
        H[year_max+'_others'] = H[year_max+'_total'] - H.loc[:,[year_max + '_'+col for col in target]].sum(axis=1)
    
        H_18 = H.loc[:,[year_min+'_'+col for col in target]].values
        H_19 = H.loc[:,[year_max+'_'+col for col in target]].values
        Ht_18 = H.loc[:,year_min+'_total'].values
        Ht_19 = H.loc[:,year_max+'_total'].values
        G = list(dict_target_share[city_name]['gr'].values())
        S = list(dict_target_share[city_name]['share'].values())
        S_ly = np.array(list(dict_target_share[city_name]['share_ly'].values()))
        W_0 = np.array(H['weight']).reshape(-1,1)
    
        # 梯度下降
        # H_n=H_19, H_share=Ht_19, H_gr=H_18, H_ly=Ht_18
        W_result, X, Growth, Share= gradient_descent(W_0, S, G, S_ly, l, m, brand_number, H_19, Ht_19, H_18, Ht_18, 
                                                    lmda, gradient_type)
        # 首先标准化，使w优化前后的点积相等
        W_norm = W_result * Ht_19.T.dot(W_0)[0]/Ht_19.T.dot(W_result)[0]
    
        H2 = deepcopy(H)
        H2['weight_factor'] = (W_norm-1)/(np.array(H2['weight']).reshape(-1,1)-1)
        H2['weight_factor1'] = (W_norm)/(np.array(H2['weight']).reshape(-1,1))
        H2.loc[(H2['Bedsize>99'] == 0), 'weight_factor'] = H2.loc[(H2['Bedsize>99'] == 0), 'weight_factor1']
        H2['weight_factor'].fillna(0, inplace=True)
        # H2.loc[(H2['weight_factor'] < 0), 'weight_factor'] = 0
        H2['W'] = W_norm
        # 整理H2便于输出
        H2['weight_factor'] = [x if math.isinf(x)==False else 0 for x in H2['weight_factor']]
        H2['weight_factor1'] = [x if math.isinf(x)==False else 0 for x in H2['weight_factor1']]
        H2[H2.columns] = H2[H2.columns].astype("str")
        H2 = H2[['City', 'City_Sample', 'PHA', 'weight', 'Bedsize>99','weight_factor', 'weight_factor1', 'W']]
        return H2
    
    schema= StructType([
            StructField("City", StringType(), True),
            StructField("PHA", StringType(), True),
            StructField("City_Sample", StringType(), True),
            StructField("weight", StringType(), True),
            StructField("Bedsize>99", StringType(), True),
            StructField("weight_factor", StringType(), True),
            StructField("weight_factor1", StringType(), True),
            StructField("W", StringType(), True)
            ])
    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def udf_target_weight(pdf):
        return func_target_weight(pdf, dict_target_share, l=learning_rate, m=max_iteration, lmda=lmda, gradient_type=gradient_type, 
                                  year_min=year_min, year_max=year_max)
    

    # %%
    # ====  三. 数据分析  ====
    
    # 每个市场 进行分析
    market_list = list(market_city_brand_dict.keys())
    index_file = 0
    for market in market_list:
        # 输入文件
        # market = '固力康'
        data_target_path = max_path + '/' + project_name + '/weight/' + market + '_data_target'
        ims_gr_path = max_path + '/' + project_name + '/weight/' + market + '_ims_gr'
    
        # 输出
        df_sum_path = max_path + '/' + project_name + '/weight/' + market + '_share_gr_out'
        df_weight_path = max_path + '/' + project_name + '/weight/' + market + '_weight_raw_out'
    
        # 1. 该市场所需要的分析的城市
        city_brand_dict = market_city_brand_dict[market]
        city_list = list(city_brand_dict.keys())
    
        # 2. 利用ims_sales_gr，生成每个城市 top 产品的 'gr','share','share_ly' 字典
        ims_sales_gr = spark.read.parquet(ims_gr_path)
        ims_sales_gr_city = ims_sales_gr.where(col('City').isin(city_list))
        target_share = ims_sales_gr_city.groupBy('City').apply(udf_target_brand)
    
        # 转化为字典格式
        df_target_share = target_share.agg(func.collect_list('dict').alias('dict_all')).select("dict_all").toPandas()
        df_target_share = df_target_share["dict_all"].values[0]
        length_dict = len(df_target_share)
        str_target_share = ""
        for index, each in enumerate(df_target_share):
            if index == 0:
                str_target_share += "{" + each[1:-1]
            elif index == length_dict - 1:
                str_target_share += "," + each[1:-1] + "}"
            else:
                str_target_share += "," + each[1:-1]
    
            if length_dict == 1:
                str_target_share += "}"
        dict_target_share  = json.loads(str_target_share)
    
        # 3. 对 data_target 进行weight分析
        data_target = spark.read.parquet(data_target_path).where(col('City').isin(city_list))
        df_weight_out = data_target.groupBy('City').apply(udf_target_weight).persist()
        df_weight_out = df_weight_out.withColumn('weight', col('weight').cast(DoubleType())) \
                           .withColumn('weight_factor', col('weight_factor').cast(DoubleType())) \
                           .withColumn('weight_factor1', col('weight_factor1').cast(DoubleType())) \
                           .withColumn('Bedsize>99', col('Bedsize>99').cast(DoubleType())) \
                           .withColumn('W', col('W').cast(DoubleType())) 
    
        # 4. 输出结果整理
        # 4.1 原始的weight结果
        df_weight_out = df_weight_out.repartition(1)
        df_weight_out.write.format("parquet") \
            .mode("overwrite").save(df_weight_path)
    
        # 4.2 用于生产的weight结果
        df_weight_final = df_weight_out.withColumn('weight_factor', func.when(col('Bedsize>99')==0, col('weight_factor1')) \
                                                                        .otherwise((col('W')-1)/(col('weight')-1)))
    
        df_weight_final = df_weight_final.fillna(0, 'weight_factor') \
                                        .withColumn('DOI', func.lit(market)) \
                                        .select('PHA', 'weight_factor', 'DOI', 'City') \
                                        .join(universe, on='City', how='left') \
                                        .withColumnRenamed('weight_factor', 'weight')
    
        df_weight_final = df_weight_final.withColumn('Province', func.when(col('City')=='福厦泉', func.lit('福建省')) \
                                                                    .otherwise(col('Province'))) \
                                         .withColumn('Province', func.when(col('City')=='珠三角', func.lit('广东省')) \
                                                                    .otherwise(col('Province'))) \
                                         .withColumn('Province', func.when(col('City')=='浙江市', func.lit('浙江省')) \
                                                                    .otherwise(col('Province'))) \
                                        .withColumn('Province', func.when(col('City')=='苏锡市', func.lit('江苏省')) \
                                                                    .otherwise(col('Province'))) 
    
        if index_file == 0:
            df_weight_final = df_weight_final.repartition(1)
            df_weight_final.write.format("parquet") \
                .mode("overwrite").save(weight_tmp_path)
        else:
            df_weight_final = df_weight_final.repartition(1)
            df_weight_final.write.format("parquet") \
                .mode("append").save(weight_tmp_path)
    
    
        # 4.3 share 和 gr 结果
        data_final = data_target.join(df_weight_out.select('PHA','W'), on='PHA', how='left')
        data_final = data_final.withColumn('MAX_new', col('Sales')*col('W'))
    
        df_sum = data_final.groupBy('City','标准商品名','Year').agg(func.sum('MAX_new').alias('MAX_new')).persist()
        df_sum = df_sum.groupBy('City', '标准商品名').pivot('Year').agg(func.sum('MAX_new')).fillna(0).persist()
        
        df_sum_city = df_sum.groupBy('City').agg(func.sum(year_min).alias('str_sum_'+year_min), func.sum(year_max).alias('str_sum_'+year_max))
    
        df_sum = df_sum.join(df_sum_city, on='City', how='left')
        #str_sum_2018 = df_sum.agg(func.sum('2018').alias('sum')).collect()[0][0]
        #str_sum_2019 = df_sum.agg(func.sum('2019').alias('sum')).collect()[0][0]
        df_sum = df_sum.withColumn('Share_'+year_min, func.bround(col(year_min)/col('str_sum_'+year_min), 3)) \
                       .withColumn('Share_'+year_max, func.bround(col(year_max)/col('str_sum_'+year_max), 3)) \
                       .withColumn('GR', func.bround(col(year_max)/col(year_min)-1, 3)) \
                        .drop('str_sum_'+year_min, 'str_sum_'+year_max)
    
        df_sum = df_sum.repartition(1)
        df_sum.write.format("parquet") \
            .mode("overwrite").save(df_sum_path)
    
        index_file += 1
        
    # %%
    # ====  四. 数据处理  ====
    
    # 1、福夏泉，珠三角 城市展开
    df_weight_final = spark.read.parquet(weight_tmp_path)
    citys = df_weight_final.select('City').distinct().toPandas()['City'].tolist()
    
    if '福厦泉' or '珠三角' or "浙江市" or "苏锡市" in citys:
        df_keep = df_weight_final.where(~col('City').isin('福厦泉', '珠三角', "浙江市", "苏锡市"))
        
        if '福厦泉' in citys:
            df1 = df_weight_final.where(col('City') == '福厦泉')
            df1_1 = df1.withColumn('City', func.lit('福州市'))
            df1_2 = df1.withColumn('City', func.lit('厦门市'))
            df1_3 = df1.withColumn('City', func.lit('泉州市'))
            df1_new = df1_1.union(df1_2).union(df1_3)
            # 合并
            df_keep = df_keep.union(df1_new)
    
        if '珠三角' in citys:
            df2 = df_weight_final.where(col('City') == '珠三角')
            df2_1 = df2.withColumn('City', func.lit('珠海市'))
            df2_2 = df2.withColumn('City', func.lit('东莞市'))
            df2_3 = df2.withColumn('City', func.lit('中山市'))
            df2_4 = df1.withColumn('City', func.lit('佛山市'))
            df2_new = df2_1.union(df2_2).union(df2_3).union(df2_4)
            # 合并
            df_keep = df_keep.union(df2_new)
            
        if '浙江市' in citys:
            df3 = df_weight_final.where(col('City') == '浙江市')
            df3_1 = df3.withColumn('City', func.lit('绍兴市'))
            df3_2 = df3.withColumn('City', func.lit('嘉兴市'))
            df3_3 = df3.withColumn('City', func.lit('台州市'))
            df3_4 = df3.withColumn('City', func.lit('金华市'))
            df3_new = df3_1.union(df3_2).union(df3_3).union(df3_4)
            # 合并
            df_keep = df_keep.union(df3_new)
    
        if '苏锡市' in citys:
            df4 = df_weight_final.where(col('City') == '苏锡市')
            df4_1 = df4.withColumn('City', func.lit('苏州市'))
            df4_2 = df4.withColumn('City', func.lit('无锡市'))
            df4_new = df4_1.union(df4_2)
            # 合并
            df_keep = df_keep.union(df4_new)
    
        df_weight_final = df_keep     
    

    # %%
    # 2、输出判断是否已有 weight_path 结果，对已有 weight_path 结果替换或者补充
    '''
    如果已经存在 weight_path 则用新的结果对已有结果进行(Province,City,DOI)替换和补充
    
    
    file_name = weight_path.replace('//', '/').split('s3:/ph-max-auto/')[1]
    
    s3 = boto3.resource('s3', region_name='cn-northwest-1')
    bucket = s3.Bucket('ph-max-auto')
    judge = 0
    for obj in bucket.objects.filter(Prefix = file_name):
        path, filename = os.path.split(obj.key)  
        if path == file_name:
            judge += 1
    if judge > 0:
        old_out = spark.read.parquet(weight_path)   
        new_info = df_weight_final.select('Province', 'City', 'DOI').distinct()
        old_out_keep = old_out.join(new_info, on=['Province', 'City', 'DOI'], how='left_anti')
        df_weight_final = df_weight_final.union(old_out_keep.select(df_weight_final.columns))           
        # 中间文件读写一下
        df_weight_final = df_weight_final.repartition(2)
        df_weight_final.write.format("parquet") \
                            .mode("overwrite").save(tmp_path)
        df_weight_final = spark.read.parquet(tmp_path)   
    '''
    # 3、输出到 weight_path
    df_weight_final = df_weight_final.repartition(2)
    df_weight_final.write.format("parquet") \
        .mode("overwrite").save(weight_path)

    
