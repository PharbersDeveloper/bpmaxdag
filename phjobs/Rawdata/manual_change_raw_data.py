def execute(**kwargs):

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
    # =========== 输入数据读取 =========== 
    g_input_version={}
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
    
    # =========== 执行 =========== 
    change_file_spark = readInFile('df_change_rawdata_file')
    raw_data_old = readInFile('df_union_raw_data')
    
    change_file = dealChangeFile(change_file_spark)
    change_file_product = productLevel(change_file)
    raw_data_new = change_raw(raw_data_old, change_file_product)
    
    # %%
    # =========== 数据输出 =============
    def lowerColumns(df):
        df = df.toDF(*[i.lower() for i in df.columns])
        return df
    
    raw_data_new = lowerColumns(raw_data_new)


    return {"out_df": raw_data_new}
