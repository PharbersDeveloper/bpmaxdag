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
    g_input_version = kwargs['g_input_version']
    ### input args ###
    
    ### output args ###
    ### output args ###

    
    
    from pyspark.sql import SparkSession, Window
    from pyspark.sql.types import StringType, IntegerType, DoubleType
    from pyspark.sql import functions as func
    import os
    from pyspark.sql.functions import pandas_udf, PandasUDFType, col
    import time    
    
    # %%
    # 输入
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
        if dict_scheme != {}:
            for i in dict_scheme.keys():
                df = df.withColumn(i, col(i).cast(dict_scheme[i]))
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
        df = dealScheme(df, dict_scheme)
        df = getInputVersion(df, table_name.replace('df_', ''))
        return df
    
    df_prod_mapping = readInFile('df_prod_mapping')
       
    all_raw_data = readInFile('df_union_raw_data', dict_scheme={"Pack_Number":"int"})

    
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
    
    def cleanProdMap(df_prod_mapping):   
        # 1、列名清洗
        # 待清洗列名
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
        # 2、数据类型处理
        df_prod_mapping = dealScheme(df_prod_mapping, dict_scheme = {"标准包装数量":"int", "pfc":"int"})
        # 3、min1和min2处理
        df_prod_mapping = df_prod_mapping.withColumn("min1", func.regexp_replace("min1", "&amp;", "&")) \
                                    .withColumn("min1", func.regexp_replace("min1", "&lt;", "<")) \
                                    .withColumn("min1", func.regexp_replace("min1", "&gt;", ">")) \
                                    .withColumn("min2", func.regexp_replace("min2", "&amp;", "&")) \
                                    .withColumn("min2", func.regexp_replace("min2", "&lt;", "<")) \
                                    .withColumn("min2", func.regexp_replace("min2", "&gt;", ">"))
        # 4、选择标准列
        df_prod_mapping = df_prod_mapping.select(['min1', 'min2', 'pfc', '通用名', "标准商品名", "标准剂型", "标准规格", "标准包装数量", "标准生产企业"]).distinct()
        return df_prod_mapping
        
    df_prod_mapping = cleanProdMap(df_prod_mapping)
    
    # %%
    # =========  数据执行  =============    
    clean = all_raw_data.select('Brand','Form','Specifications','Pack_Number','Manufacturer','Molecule','Corp','Route','Path','Sheet').distinct()
    
    # 生成min1
    clean = clean.withColumn('Brand', func.when((col('Brand').isNull()) | (clean.Brand == 'NA'), col('Molecule')).otherwise(col('Brand')))
    clean = clean.withColumn('Pack_Number', col('Pack_Number').cast(StringType()))
    clean = clean.withColumn("min1", func.concat_ws(minimum_product_sep, 
                                    *[func.when(col(i).isNull(), func.lit("NA")).otherwise(col(i)) for i in minimum_product_columns]))
    
    
    # min1不在product_map中的为需要清洗的条目                 
    need_clean = clean.join(df_prod_mapping.select('min1').distinct(), on='min1', how='left_anti')

    # %%
    # =========== 数据输出 =============
    def lowerColumns(df):
        df = df.toDF(*[i.lower() for i in df.columns])
        return df
    need_clean = lowerColumns(need_clean)   
    return {"out_df":need_clean}

