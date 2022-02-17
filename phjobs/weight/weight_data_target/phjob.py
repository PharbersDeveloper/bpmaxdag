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
    universe_choice = kwargs['universe_choice']
    job_choice = kwargs['job_choice']
    factor_choice = kwargs['factor_choice']
    universe_outlier_choice = kwargs['universe_outlier_choice']

    year_list = kwargs['year_list']
    add_imsinfo_version = kwargs['add_imsinfo_version']
    ### input args ###
    
    ### output args ###
    p_out = kwargs['p_out']
    out_mode = kwargs['out_mode']
    run_id = kwargs['run_id'].replace(":","_")
    owner = kwargs['owner']
    project_name = kwargs['project_name']
    g_database_temp = kwargs['g_database_temp']
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
    from phcli.ph_tools.addTable.addTableToGlue import AddTableToGlue
    
    # %%
    # =========== 参数处理 =========== 
    
    # 是否运行此job
    if job_choice != "weight":
         raise ValueError('不运行weight')
    
    logger.debug(market_city_brand)
    
    year_list = year_list.replace(" ","").split(",")
    year_min = year_list[0]
    year_max = year_list[1]
    
    def getVersionDict(str_choice):
        dict_choice = {}
        if str_choice != "Empty":
            for each in str_choice.replace(", ",",").split(","):
                market_name = each.split(":")[0]
                version_name = each.split(":")[1]
                dict_choice[market_name]=version_name
        return dict_choice 
    dict_universe_choice = getVersionDict(universe_choice)
    dict_factor = getVersionDict(factor_choice)
    dict_universe_outlier = getVersionDict(universe_outlier_choice)
    
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
    
    # ============== 删除已有的s3中间文件 =============
    
    g_table_result = 'weight_data_target'
        
    import boto3
    def deletePath(path_dir):
        file_name = path_dir.replace('//', '/').split('s3:/ph-platform/')[1]
        s3 = boto3.resource('s3', region_name='cn-northwest-1',
                            aws_access_key_id="AKIAWPBDTVEAEU44ZAGT",
                            aws_secret_access_key="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")
        bucket = s3.Bucket('ph-platform')
        bucket.objects.filter(Prefix=file_name).delete()
    deletePath(path_dir=f"{p_out + g_table_result}/version={run_id}/provider={project_name}/owner={owner}/")
        
    
    # %% 
    # =========== 输入数据读取 =========== 
    def dealToNull(df):
        df = df.replace(["None", ""], None)
        return df
    def dealScheme(df, dict_scheme):
        # 数据类型处理
        for i in dict_scheme.keys():
            df = df.withColumn(i, col(i).cast(dict_scheme[i]))
        return df
    def lowCol(df):
        df = df.toDF(*[c.lower() for c in df.columns])
        return df
    
    def getUniverse(market, dict_universe_choice):
        if market in dict_universe_choice.keys():
            df_universe =  kwargs['df_universe_other'].where(col('version')==dict_universe_choice[market])
            df_universe = dealToNull(df_universe) 
        else:
            df_universe =  kwargs['df_universe_base']
            df_universe = dealToNull(df_universe)
        return df_universe
    
    df_id_bedsize = kwargs['df_ID_Bedsize']
    df_id_bedsize = dealToNull(df_id_bedsize)
    df_id_bedsize = lowCol(df_id_bedsize)

    df_city_info = kwargs['df_province_city_mapping']
    df_city_info = dealToNull(df_city_info)
    df_city_info = lowCol(df_city_info)
    
    df_mkt_mapping = kwargs['df_mkt_mapping']
    df_mkt_mapping = dealToNull(df_mkt_mapping)    
    df_mkt_mapping = lowCol(df_mkt_mapping)
    
    df_cpa_pha_mapping = kwargs['df_cpa_pha_mapping']
    df_cpa_pha_mapping = dealToNull(df_cpa_pha_mapping)    
    df_cpa_pha_mapping = lowCol(df_cpa_pha_mapping)
    
    df_prod_mapping = kwargs['df_prod_mapping']
    df_prod_mapping = dealToNull(df_prod_mapping)    
    df_prod_mapping = lowCol(df_prod_mapping)
    
    df_raw_data = kwargs['df_raw_data']
    df_raw_data = dealToNull(df_raw_data)    
    df_raw_data = lowCol(df_raw_data)
          
    df_universe_outlier_all = kwargs['df_universe_outlier']
    df_universe_outlier_all = dealToNull(df_universe_outlier_all)
    df_universe_outlier_all = lowCol(df_universe_outlier_all)
    
    df_universe = kwargs['df_universe_base']
    df_universe = dealToNull(df_universe)    
    df_universe = lowCol(df_universe)
    
    df_factor_all = kwargs['df_factor']
    df_factor_all = dealToNull(df_factor_all)
    df_factor_all = lowCol(df_factor_all)
    
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
    # 2、universe
    dict_cols_universe = {"City_Tier_2010":["City_Tier", "CITYGROUP", "City_Tier_2010"], "PHA":["Panel_ID", "PHA"]}
    df_universe = getTrueColRenamed(df_universe, dict_cols_universe, df_universe.columns) 
    
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
    # 2. universe 文件                  
    df_universe = df_universe.select("PHA", "Province", "City", "PANEL") \
                        .withColumn("PANEL", col("PANEL").cast(DoubleType())) \
                        .distinct()
    
    # 3. mkt_mapping 文件
    df_mkt_mapping = df_mkt_mapping.select('mkt', '标准通用名').distinct() \
                            .withColumnRenamed("标准通用名", "通用名")
    
    # 4. cpa_pha_mapping 文件
    df_cpa_pha_mapping = dealIDLength(df_cpa_pha_mapping)
    df_cpa_pha_mapping = df_cpa_pha_mapping.where(col("推荐版本") == 1).select('ID', 'PHA').distinct()
    
    
    # 5. city_info_path
    df_city_info = dealIDLength(df_city_info)
    df_city_info = df_city_info.select('ID', 'Province', 'City') \
                        .withColumnRenamed('City', 'City_add').distinct()
    
    
    # 6. id_bedsize
    schema= StructType([
            StructField("ID", StringType(), True),
            StructField("Bedsize", DoubleType(), True),
            StructField("Bedsize>99", DoubleType(), True)
            ])
    df_id_bedsize = dealIDLength(df_id_bedsize)
    df_id_bedsize = df_id_bedsize.join(df_cpa_pha_mapping, on='ID', how='left')
    
    # 7. raw_data 文件
    # min1 生成
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
    df_raw_data2 = df_raw_data2.join(df_cpa_pha_mapping, on='ID', how='left')
    df_raw_data2 = df_raw_data2.join(df_universe.select("PHA", "Province", "City").distinct(), on='PHA', how='left')
    
    df_raw_data2 = df_raw_data2.withColumn('标准商品名', func.when(col('标准商品名').isNull(), col('通用名')).otherwise(col('标准商品名')))
    
    
    # %%
    # ====  三. 每个市场分析  ==== 
    
    market_list = list(market_city_brand_dict.keys()) 
    
    for market in market_list:
        # 输入文件
        logger.debug(market)

        # 1. 数据准备
        # universe_ot 文件        
        df_universe_ot = df_universe_outlier_all.where(col('version')==dict_universe_outlier[market])
        df_universe_ot = df_universe_ot.withColumnRenamed('Panel_ID', 'PHA')
        # universe 文件: 如果有指定的universe则重新读取，否则就用已有的 universe_base
        
        df_universe = getUniverse(market, dict_universe_choice)
        df_universe = df_universe.select("Panel_ID", "Province", "City", "PANEL") \
                            .withColumnRenamed("Panel_ID", "PHA") \
                            .withColumn("PANEL", col("PANEL").cast(DoubleType())) \
                            .distinct()
    
        # factor 文件    
        df_factor = df_factor_all.where(col('version')==dict_factor[market])
        if "factor" not in df_factor.columns:
            df_factor = df_factor.withColumnRenamed("factor_new", "factor")
        df_factor = df_factor.select('City', 'factor').distinct()
    
        # df_raw_data
        data = df_raw_data2.where(col('mkt') == market)
    
        # 2. 权重计算
        '''
        权重计算：
            利用医药收入和城市Factor
            从universe中去掉outliers以及其他Panel == 0的样本
        '''
        # %% 目的是实现 PANEL = 0 的样本回填
        pha_list = df_universe.where(col('PANEL') == 0).where(~col('PHA').isNull()).select('PHA').distinct().toPandas()['PHA'].tolist()
        pha_list2 = data.where(col('Date') > 202000).where(~col('PHA').isNull()).select('PHA').distinct().toPandas()['PHA'].tolist()
    
        df_universe_ot_rm = df_universe_ot.where((col('PANEL') == 1) | (col('PHA').isin(pha_list))) \
                                .where( ~((col('PANEL') == 0) & (col('PHA').isin(pha_list2)) )) \
                                .fillna(0, subset=['Est_DrugIncome_RMB', 'BEDSIZE']) \
                                .withColumn('Panel_income', col('Est_DrugIncome_RMB') * col('PANEL')) \
                                .withColumn('Bedmark', func.when(col('BEDSIZE') >= 100, func.lit(1)).otherwise(func.lit(0)))
        df_universe_ot_rm = df_universe_ot_rm.withColumn('non_Panel_income', col('Est_DrugIncome_RMB') * (1-col('PANEL')) * col('Bedmark'))
    
        Seg_Panel_income = df_universe_ot_rm.groupBy('Seg').agg(func.sum('Panel_income').alias('Seg_Panel_income'))
        Seg_non_Panel_income = df_universe_ot_rm.groupBy('Seg', 'City') \
                                    .agg(func.sum('non_Panel_income').alias('Seg_non_Panel_income'))
        df_universe_ot_rm = df_universe_ot_rm.join(Seg_Panel_income, on='Seg', how='left') \
                                    .join(Seg_non_Panel_income, on=['Seg', 'City'], how='left').persist()
    
        # %% 按Segment+City拆分样本
        seg_city = df_universe_ot_rm.select('Seg','City','Seg_non_Panel_income','Seg_Panel_income').distinct()
    
        seg_pha = df_universe_ot_rm.where(col('PANEL') == 1).select('Seg','City','PHA','BEDSIZE').distinct() \
                                .withColumnRenamed('City', 'City_Sample')
    
        weight_table = seg_city.join(seg_pha, on='Seg', how='left') \
                                .join(df_factor, on='City', how='left').persist()
    
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
        tmp_city = tmp_weight.join(df_city_info, on='ID', how='left')
        tmp_city = tmp_city.withColumn('City', func.when(col('City').isNull(), col('City_add')).otherwise(col('City'))) \
                    .withColumn('City_Sample', func.when(col('City_Sample').isNull(), col('City_add')) \
                                                    .otherwise(col('City_Sample')))
    
        # %% 床位数匹配以及权重修正
        tmp_bed =  tmp_city.join(df_id_bedsize.select('ID', 'Bedsize>99').distinct(), on='ID', how='left')
    
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
        df_data_target = data.where(col('Year').isin(year_list)) \
                            .select('Date','PHA','标准商品名','Sales', 'Year').distinct() \
                            .join(weight_0, on='PHA', how='inner').persist()
        df_data_target = df_data_target.where(col('City').isin(city_list))
    
        df_data_target = df_data_target.withColumn('MAX_weighted', col('weight')*col('Sales')) \
                                .withColumn('tmp', func.concat(col('Year'), func.lit('_'), col('标准商品名'))) \
                                .join(df_id_bedsize.select('PHA','Bedsize>99').dropDuplicates(['PHA']), on='PHA', how='left')
        
        df_data_target = df_data_target.withColumn('doi', func.lit(market))
        
        # 结果输出
        def lowerColumns(df):
            df = df.toDF(*[i.lower() for i in df.columns])
            return df
        df_data_target = lowerColumns(df_data_target)
        
        AddTableToGlue(df=df_data_target, database_name_of_output=g_database_temp, table_name_of_output=g_table_result, 
                           path_of_output_file=p_out, mode=out_mode) \
                    .add_info_of_partitionby({"version":run_id,"provider":project_name,"owner":owner})
    
    # %%
    # =========== 数据输出 =============
    # 读回
    df_out = spark.sql("SELECT * FROM %s.%s WHERE version='%s' AND provider='%s' AND  owner='%s'" 
                                 %(g_database_temp, g_table_result, run_id, project_name, owner))
    df_out = df_out.drop('version', 'provider', 'owner')
    
    return {"out_df":df_out}        
