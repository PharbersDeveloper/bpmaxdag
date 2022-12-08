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
    test = kwargs['test']
    
    path_change_file = kwargs['path_change_file']
    
    if path_change_file == 'Empty':
        minimum_product_sep = kwargs['minimum_product_sep']
        minimum_product_columns = kwargs['minimum_product_columns']
    else:
        if_two_source = kwargs['if_two_source']
       
    ### input args ###
    
    ### output args ###

    ### output args ###

    
    
    import os
    import pandas as pd
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    from pyspark.sql.functions import col     
    import json
    import boto3
    from pyspark.sql.functions import lit, col, struct, to_json, json_tuple
    from functools import reduce
    import json

    # %%
    # 输入

    if path_change_file != 'Empty':
        # %% 
        # =========== 数据分析 =========== 
        def dealIDLength(df, colname='ID'):
            # ID不足7位的前面补0到6位
            # 国药诚信医院编码长度是7位数字，cpa医院编码是6位数字
            # 其他来源的ID 还有包含字母的, 所以要为字符型，不能转为 数值型
            df = df.withColumn(colname, col(colname).cast(StringType()))
            # 去掉末尾的.0
            df = df.withColumn(colname, func.regexp_replace(colname, "\\.0", ""))
            df = df.withColumn(colname, func.when(func.length(col(colname)) < 7, func.lpad(col(colname), 6, "0")).otherwise(col(colname)))
            return df

        def dealChangeFile(df):
            df = df.withColumn('Date', col('Date').cast('int')) \
                                .select(*[col(i).astype("string") for i in df.columns])
            df = dealIDLength(df, colname='Hospital_ID')
            df = df.replace('nan', None).fillna('NA', subset=["Brand", "Form", "Specifications", "Pack_Number", "Manufacturer"])
            return df

        def productLevel(df):
            change_file_1 = df.where(col('错误类型') == '产品层面') \
                            .withColumn('all_info', func.concat('Molecule', 'Brand', 'Form', 'Specifications', 'Pack_Number', 'Manufacturer', 'Date', 'Hospital_ID')) \
                            .select('all_info', 'Sales_old', 'Sales_new', 'Units_old', 'Units_new').distinct()
            return change_file_1

        def change_raw(raw_data_old, change_file_product):
            raw_data_old = raw_data_old.withColumn("ID", func.when(func.length(col('ID')) < 7, func.lpad(col('ID'), 6, "0")).otherwise(col('ID')))
            # a. 产品层面
            raw_data_old = raw_data_old.withColumn('Brand_new', func.when(col('Brand').isNull(), func.lit('NA')).otherwise(col('Brand')))
            raw_data_old = raw_data_old.withColumn('all_info', func.concat(func.col('Molecule'), func.col('Brand_new'), func.col('Form'), func.col('Specifications'),
                                     func.col('Pack_Number'), func.col('Manufacturer'), func.col('Date'), func.col('ID')))
            raw_data_new = raw_data_old.join(change_file_product, on='all_info', how='left')
            print("产品层面替换的条目：", change_file_product.count())
            raw_data_new = raw_data_new.withColumn('Sales', func.when(~col('Sales_old').isNull(), col('Sales_new')) \
                                                                    .otherwise(col('Sales'))) \
                                        .withColumn('Units', func.when(~col('Units_old').isNull(), col('Units_new')) \
                                                                    .otherwise(col('Units')))
            raw_data_new = raw_data_new.drop('all_info', 'Sales_old', 'Sales_new', 'Units_old', 'Units_new', 'Brand_new')
            print('修改前后raw_data行数是否一致：', (raw_data_new.count() == raw_data_old.count()))
            return raw_data_new
        
        
        def processPipe(path_raw_data, change_file_product):
            # raw_data 读取和备份
            raw_data_old = spark.read.parquet(path_raw_data, header=True)
            raw_data_old.write.format("parquet").mode("overwrite").save(f"{path_raw_data}_bk") 
            raw_data_old = spark.read.parquet(f"{path_raw_data}_bk", header=True)

            # raw_data 处理和输出
            raw_data_new = change_raw(raw_data_old, change_file_product)
            raw_data_new.write.format("parquet").mode("overwrite").save(path_raw_data)        

        # =========== 执行 =========== 
        
        # change_file 处理
        change_file_spark = spark.read.csv(path_change_file, header=True, encoding='GBK')
        change_file = dealChangeFile(change_file_spark)
        change_file_product = productLevel(change_file)
        
        # raw_data
        path_raw_data = max_path + "/" + project_name + "/" + outdir + "/raw_data"
        processPipe(path_raw_data, change_file_product)

        if if_two_source == 'True':
            # raw_data_std
            path_raw_data_std = max_path + "/" + project_name + "/" + outdir + "/raw_data_std"
            processPipe(path_raw_data_std, change_file_product)


    else:
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


