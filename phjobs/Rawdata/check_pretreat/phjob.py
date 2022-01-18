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
    current_year = kwargs['current_year']
    current_month = kwargs['current_month']
    three = kwargs['three']
    twelve = kwargs['twelve']
    g_id_molecule = kwargs['g_id_molecule']
    ### input args ###
    
    ### output args ###
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
    # 输入
    current_year = int(current_year)
    current_month = int(current_month)
    three = int(three)
    twelve = int(twelve)
    
    if minimum_product_sep == "kong":
        minimum_product_sep = ""
    minimum_product_columns = minimum_product_columns.replace(" ","").split(",")
    
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
    
    df_prod_mapping = kwargs['df_prod_mapping']
    df_prod_mapping = dealToNull(df_prod_mapping)
       
    df_raw_data = kwargs['df_union_raw_data']
    df_raw_data = dealToNull(df_raw_data)
    df_raw_data = dealScheme(df_raw_data, {"Pack_Number":"int", "Date":"int"})
    
    df_cpa_pha_mapping = kwargs['df_cpa_pha_mapping']
    df_cpa_pha_mapping = dealToNull(df_cpa_pha_mapping)
    
    df_province_city_mapping =  kwargs["df_province_city_mapping"]
    df_province_city_mapping = dealToNull(df_province_city_mapping)
    
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
    
    def dealIDLength(df, colname='ID'):
        # ID不足7位的前面补0到6位
        # 国药诚信医院编码长度是7位数字，cpa医院编码是6位数字
        # 其他来源的ID 还有包含字母的, 所以要为字符型，不能转为 数值型
        df = df.withColumn(colname, col(colname).cast(StringType()))
        # 去掉末尾的.0
        df = df.withColumn(colname, func.regexp_replace(colname, "\\.0", ""))
        df = df.withColumn(colname, func.when(func.length(col(colname)) < 7, func.lpad(col(colname), 6, "0")).otherwise(col(colname)))
        return df
    
    # prod_mappin 清洗
    dict_cols_prod_map = {"通用名":["通用名", "标准通用名", "通用名_标准", "药品名称_标准", "S_Molecule_Name"], 
                          "min2":["min2", "min1_标准"],
                          "pfc":["pfc", "packcode", "Pack_ID", "PackID", "packid"],
                          "标准商品名":["标准商品名", "商品名_标准", "S_Product_Name"],
                          "标准剂型":["标准剂型", "剂型_标准", "Form_std", "S_Dosage"],
                          "标准规格":["标准规格", "规格_标准", "Specifications_std", "药品规格_标准", "S_Pack"],
                          "标准包装数量":["标准包装数量", "包装数量2", "包装数量_标准", "Pack_Number_std", "S_PackNumber", "最小包装数量"],
                          "标准生产企业":["标准生产企业", "标准企业", "生产企业_标准", "Manufacturer_std", "S_CORPORATION", "标准生产厂家"]
                         }
    
    df_prod_mapping = getTrueColRenamed(df_prod_mapping, dict_cols_prod_map, df_prod_mapping.columns)
    df_prod_mapping = dealScheme(df_prod_mapping, {"标准包装数量":"int", "pfc":"int"})
    df_prod_mapping = df_prod_mapping.select("min1", "min2", "pfc", "通用名", "标准商品名", "标准剂型", "标准规格", "标准包装数量", "标准生产企业").distinct() \
                                .withColumnRenamed("标准商品名", "商品名") \
                                .withColumnRenamed("标准剂型", "剂型") \
                                .withColumnRenamed("标准规格", "规格") \
                                .withColumnRenamed("标准包装数量", "包装数量") \
                                .withColumnRenamed("标准生产企业", "生产企业") \
                                .withColumnRenamed("pfc", "Pack_ID")
    
    # cpa_pha_mapping 清洗
    df_cpa_pha_mapping = dealIDLength(df_cpa_pha_mapping)
    df_cpa_pha_mapping = df_cpa_pha_mapping.where(col("推荐版本") == 1).select('ID', 'PHA').distinct()
    
    # province_city_mapping 清洗
    df_province_city_mapping = dealIDLength(df_province_city_mapping)
    city_list = ['北京市','常州市','广州市','济南市','宁波市','上海市','苏州市','天津市','温州市','无锡市']
    df_province_city_mapping = df_province_city_mapping.withColumn('Province', func.when(col('City').isin(['福州市','厦门市','泉州市']), func.lit('福厦泉')) \
                                                                    .otherwise(func.when(col('City').isin(city_list), col('City')) \
                                                                               .otherwise(col('Province')))) \
                                                    
    
    # Raw_data 
    df_raw_data = dealScheme(df_raw_data, {"Pack_Number":"int","Date":"int", "Units":"double", "Sales":"double"})
    df_raw_data = df_raw_data.drop('Pack_ID')
    
    # min1 生成
    df_raw_data = df_raw_data.withColumn('Brand_bak', col('Brand')) \
                            .withColumn("Brand", func.when((col('Brand').isNull()) | (col('Brand') == 'NA'), col('Molecule')). \
                                             otherwise(col('Brand')))
    df_raw_data = df_raw_data.withColumn("min1", func.concat_ws(minimum_product_sep, 
                                    *[func.when(col(i).isNull(), func.lit("NA")).otherwise(col(i).cast('string')) for i in minimum_product_columns]))
    df_raw_data = df_raw_data.withColumn('Brand', col('Brand_bak')).drop('Brand_bak')

    
    # %%
    # =========== 数据执行 =============
    df_raw_data_out = df_raw_data.join(df_prod_mapping.select('min1','min2','通用名','商品名','Pack_ID').dropDuplicates(['min1']), on='min1', how='left') \
                                .join(df_cpa_pha_mapping.select('ID', 'PHA'), on='ID', how='left') \
                                .join(df_province_city_mapping.select('ID', 'Province').distinct(), on='ID', how='left')

    
    # %%
    # =========== 数据输出 =============
    def lowerColumns(df):
        df = df.toDF(*[i.lower() for i in df.columns])
        return df
    
    df_raw_data_out = lowerColumns(df_raw_data_out)
    
    logger.debug('数据执行-Finish')
    
    return {'out_df':df_raw_data_out}