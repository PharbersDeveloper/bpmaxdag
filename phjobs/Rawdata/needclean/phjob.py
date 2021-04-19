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
    test = kwargs['test']
    ### input args ###
    
    ### output args ###
    c = kwargs['c']
    d = kwargs['d']
    ### output args ###

    from pyspark.sql import SparkSession, Window
    from pyspark.sql.types import StringType, IntegerType, DoubleType
    from pyspark.sql import functions as func
    import os
    from pyspark.sql.functions import pandas_udf, PandasUDFType
    import time    # %%
    # 输入
    if minimum_product_sep == "kong":
        minimum_product_sep = ""
    minimum_product_columns = minimum_product_columns.replace(" ","").split(",")
    product_map_path = max_path + '/' + project_name + '/' + outdir + '/prod_mapping'
    
    if test == 'True':
        all_raw_data_path = max_path + '/' + project_name + '/' + outdir + '/raw_data_check/raw_data'
    else:
        all_raw_data_path = max_path + '/' + project_name + '/' + outdir + '/raw_data'
    
    # 输出
    need_clean_path = max_path + '/' + project_name + '/' + outdir + '/raw_data_check/need_cleaning_raw.csv'
    # %%
    # =========  数据执行  =============
    all_raw_data = spark.read.parquet(all_raw_data_path)
    
    clean = all_raw_data.select('Brand','Form','Specifications','Pack_Number','Manufacturer','Molecule','Corp','Route','Path','Sheet').distinct()
    
    # 生成min1
    clean = clean.withColumn('Brand', func.when((clean.Brand.isNull()) | (clean.Brand == 'NA'), clean.Molecule).otherwise(clean.Brand))
    clean = clean.withColumn("min1", func.when(clean[minimum_product_columns[0]].isNull(), func.lit("NA")).
                                       otherwise(clean[minimum_product_columns[0]]))
    for col in minimum_product_columns[1:]:
        clean = clean.withColumn(col, clean[col].cast(StringType()))
        clean = clean.withColumn("min1", func.concat(
            clean["min1"],
            func.lit(minimum_product_sep),
            func.when(func.isnull(clean[col]), func.lit("NA")).otherwise(clean[col])))
    
    # 已有的product_map文件
    product_map = spark.read.parquet(product_map_path)
    product_map = product_map.distinct() \
                        .withColumn("min1", func.regexp_replace("min1", "&amp;", "&")) \
                        .withColumn("min1", func.regexp_replace("min1", "&lt;", "<")) \
                        .withColumn("min1", func.regexp_replace("min1", "&gt;", ">"))
    
    # min1不在product_map中的为需要清洗的条目                 
    need_clean = clean.join(product_map.select('min1').distinct(), on='min1', how='left_anti')
    if need_clean.count() > 0:
        need_clean = need_clean.repartition(1)
        need_clean.write.format("csv").option("header", "true") \
            .mode("overwrite").save(need_clean_path) 

