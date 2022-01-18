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
        for i in dict_scheme.keys():
            df = df.withColumn(i, col(i).cast(dict_scheme[i]))
        return df
    
    product_map = kwargs['df_prod_mapping']
    product_map = dealToNull(product_map)
       
    all_raw_data = kwargs['df_union_raw_data']
    all_raw_data = dealToNull(all_raw_data)
    all_raw_data = dealScheme(all_raw_data, {"Pack_Number":"int"})

    
    # %%
    # =========  数据执行  =============    
    clean = all_raw_data.select('Brand','Form','Specifications','Pack_Number','Manufacturer','Molecule','Corp','Route','Path','Sheet').distinct()
    
    # 生成min1
    clean = clean.withColumn('Brand', func.when((col('Brand').isNull()) | (clean.Brand == 'NA'), col('Molecule')).otherwise(col('Brand')))
    clean = clean.withColumn("min1", func.when(clean[minimum_product_columns[0]].isNull(), func.lit("NA")).
                                       otherwise(clean[minimum_product_columns[0]]))
    for col in minimum_product_columns[1:]:
        clean = clean.withColumn(col, clean[col].cast(StringType()))
        clean = clean.withColumn("min1", func.concat(
            clean["min1"],
            func.lit(minimum_product_sep),
            func.when(func.isnull(clean[col]), func.lit("NA")).otherwise(clean[col])))
    
    # 已有的product_map文件
    product_map = product_map.distinct() \
                        .withColumn("min1", func.regexp_replace("min1", "&amp;", "&")) \
                        .withColumn("min1", func.regexp_replace("min1", "&lt;", "<")) \
                        .withColumn("min1", func.regexp_replace("min1", "&gt;", ">"))
    
    # min1不在product_map中的为需要清洗的条目                 
    need_clean = clean.join(product_map.select('min1').distinct(), on='min1', how='left_anti')

    # %%
    # =========== 数据输出 =============
    def lowerColumns(df):
        df = df.toDF(*[i.lower() for i in df.columns])
        return df
    need_clean = lowerColumns(need_clean)   
    return {"out_df":need_clean}

