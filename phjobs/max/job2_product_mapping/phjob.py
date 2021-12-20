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
    minimum_product_columns = kwargs['minimum_product_columns']
    minimum_product_sep = kwargs['minimum_product_sep']
    minimum_product_newname = kwargs['minimum_product_newname']
    need_cleaning_cols = kwargs['need_cleaning_cols']
    out_path = kwargs['out_path']
    run_id = kwargs['run_id'].replace(":","_")
    owner = kwargs['owner']
    # g_input_version = kwargs['g_input_version']
    g_database_temp = kwargs['g_database_temp']
    g_database_input = kwargs['g_database_input']
    ### input args ###
    
    ### output args ###
    #g_out_table = kwargs['g_out_table']
    #g_need_clean_table = kwargs['g_need_clean_table']
    ### output args ###

    
    
    from pyspark.sql import SparkSession, Window
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    import os
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col
    import json
    import boto3    # %% 
    # 输入参数设置
    g_out_table = 'product_mapping_out'
    logger.debug('job2_product_mapping')
    
    # dict_input_version = json.loads(g_input_version)
    # logger.debug(dict_input_version)
    
    need_cleaning_cols = need_cleaning_cols.replace(" ","").split(",")
    minimum_product_columns = minimum_product_columns.replace(" ","").split(",")
    if minimum_product_sep == "kong":
        minimum_product_sep = ""
        
    # 输出
    p_out_path = out_path + g_out_table
    p_out_need_clean = out_path + g_need_clean_table
    # %% 
    # 输入数据读取
    df_raw_data = kwargs['df_hospital_mapping_out']
    # print(df_raw_data)
    # print(df_raw_data.count())
    
    df_prod_mapping =  kwargs['df_prod_mapping']
    # print(df_prod_mapping)
    # print(df_prod_mapping.count())

    # %% 
    # =========== 数据清洗 =============
    logger.debug('数据清洗-start')
    # 函数定义
    def getTrueCol(df, l_colnames, l_df_columns):
        # 检索出正确列名
        l_true_colname = []
        for i in l_colnames:
            if i.lower() in l_df_columns and df.where(col(i) != 'None').count() > 0:
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
    
    #  执行
    df_prod_mapping = getTrueColRenamed(df_prod_mapping, dict_cols_prod_map, df_prod_mapping.columns)
    
    # 2、选择标准列
    # 标准列名
    std_cols_prod_mapping = ['min1', 'min2', 'pfc', '通用名', "标准商品名", "标准剂型", "标准规格", "标准包装数量", "标准生产企业"]
    
    #  执行
    df_prod_mapping = df_prod_mapping.select(std_cols_prod_mapping)
    
    # 3、数据类型处理
    dict_scheme_prod_mapping = {"标准包装数量":"int", "pfc":"int"}
    
    df_prod_mapping = dealScheme(df_prod_mapping, dict_scheme_prod_mapping)

    # %%
    # =========== 数据执行 =============
    logger.debug('数据执行-start')
    df_raw_data = df_raw_data.withColumn("Brand", func.when((col('Brand')=='None') | (col('Brand') == 'NA'), col('Molecule')). \
                                             otherwise(col('Brand')))
    
    
    # min1 生成
    df_raw_data = df_raw_data.withColumn('Pack_Number', col('Pack_Number').cast(StringType()))
    df_raw_data = df_raw_data.withColumn(minimum_product_newname, func.concat_ws(minimum_product_sep, 
                                    *[func.when(col(i)=='None', func.lit("NA")).otherwise(col(i)) for i in minimum_product_columns]))
    
    
    
    # 产品信息匹配
    df_product_map_for_needclean = df_prod_mapping.select("min1").distinct()
    df_product_map_for_rawdata = df_prod_mapping.select("min1", "min2", "通用名", "标准商品名").distinct()
    
    df_raw_data = df_raw_data.join(df_product_map_for_rawdata, on="min1", how="left") \
                                    .withColumnRenamed("通用名", "S_Molecule")
    
    # 待清洗
    df_need_cleaning = df_raw_data.join(df_product_map_for_needclean, on="min1", how="left_anti") \
                            .select(need_cleaning_cols) \
                            .distinct()
    logger.debug('待清洗行数: ' + str(df_need_cleaning.count()))

    # %%
    # =========== 数据输出 =============
    # 1、待清洗
    # if df_need_cleaning.count() > 0:
    #     outResult(df_need_cleaning, p_out_need_clean)        
    #     logger.debug("已输出待清洗文件至:  " + p_out_need_clean)
    #     createPartition(p_out_need_clean)
    
    def lowerColumns(df):
        df = df.toDF(*[i.lower() for i in df.columns])
        return df

    df_raw_data = lowerColumns(df_raw_data)

    return {'out_df':df_raw_data}   
    

    
    

