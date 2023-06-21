# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""



def pretreat(kwargs):
    ### input args ###
    max_path = kwargs.get('max_path', 'D:/Auto_MAX/MAX/')
    project_name = kwargs['project_name']
    outdir = kwargs['outdir']
    history_outdir = kwargs.get('history_outdir', 'Empty')
    raw_data_path = kwargs.get('raw_data_path', 'Empty')
    if_two_source = kwargs.get('if_two_source', 'False')
    cut_time_left = kwargs.get('cut_time_left', 'Empty')
    cut_time_right = kwargs['cut_time_right']
    if_union = kwargs.get('if_union', 'True')
    test = kwargs.get('test', 'False')
    auto_max = kwargs.get('auto_max', 'True')
    ### input args ###
    

    from pyspark.sql import SparkSession, Window
    from pyspark.sql.types import StringType, IntegerType, DoubleType
    from pyspark.sql import functions as func
    import os
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col
    import time
    import re        

    spark = SparkSession.builder.getOrCreate()
    
    print('pretreat')
    # %%
    # project_name = 'Gilead'
    # outdir = '202101'
    # if_two_source = 'True'
    # cut_time_left = '202101'
    # cut_time_right = '202101'
    # test = 'True'
    # %%
    # 输入
    if if_two_source != "False" and if_two_source != "True":
        print('wrong input: if_two_source, False or True') 
        raise ValueError('wrong input: if_two_source, False or True')
        
    if if_union == 'True':
        cut_time_left = int(cut_time_left)
        cut_time_right = int(cut_time_right)
    
    molecule_adjust_path = max_path + "/Common_files/新老通用名转换.csv"
    
    if raw_data_path == 'Empty':
        raw_data_path = max_path + '/' + project_name + '/' + outdir + '/raw_data_upload.csv'
    
    if history_outdir == 'Empty':
        if outdir[4:] == '01':
            history_outdir = str((int(outdir[0:4]) -1 )*100 + 12)
        else:
            history_outdir = str(int(outdir) - 1)
    
    history_raw_data_path = max_path + '/' + project_name + '/' + history_outdir + '/raw_data'
    history_raw_data_delivery_path = max_path + '/' + project_name + '/' + history_outdir + '/raw_data_delivery'    
    
    cpa_pha_mapping_path = max_path + '/' + project_name + '/cpa_pha_mapping'
    if if_two_source == 'True':
        history_raw_data_std_path = max_path + '/' + project_name + '/' + history_outdir + '/raw_data_std'
        cpa_pha_mapping_common_path = max_path + '/Common_files/cpa_pha_mapping'    
    
    
    std_names = ["Date", "ID", "Raw_Hosp_Name", "Brand", "Form", "Specifications", "Pack_Number", "Manufacturer", 
    "Molecule", "Source", "Corp", "Route", "ORG_Measure"]
    
    # if project_name == 'Mylan':
    #    std_names = ["Date", "ID", "Raw_Hosp_Name", "Brand", "Form", "Specifications", "Pack_Number", "Manufacturer", 
    #    "Molecule", "Source", "Corp", "Route", "ORG_Measure", "min1", "Pack_ID"]
    
    # 输出
    same_sheet_dup_path = max_path + '/' + project_name + '/' + outdir + '/raw_data_check/same_sheet_dup.csv'
    across_sheet_dup_path = max_path + '/' + project_name + '/' + outdir + '/raw_data_check/across_sheet_dup.csv'
    across_sheet_dup_bymonth_path = max_path + '/' + project_name + '/' + outdir + '/raw_data_check/across_sheet_dup_bymonth.csv'
    
    if test != "False" and test != "True":
        print('wrong input: test, False or True') 
        raise ValueError('wrong input: test, False or True')
    
    if test == 'False':
        all_raw_data_path = max_path + '/' + project_name + '/' + outdir + '/raw_data'
        tmp_path_raw_data_dedup  = max_path + '/' + project_name + '/' + outdir + '/Raw_tmp/raw_data_dedup'
        raw_data_delivery_path = max_path + '/' + project_name + '/' + outdir + '/raw_data_delivery'
        if if_two_source == 'True':
            all_raw_data_std_path = max_path + '/' + project_name + '/' + outdir + '/raw_data_std'
            tmp_path_raw_data_dedup_std = max_path + '/' + project_name + '/' + outdir + '/Raw_tmp/raw_data_dedup_std'
    else:
        all_raw_data_path = max_path + '/' + project_name + '/' + outdir + '/raw_data_check/raw_data'
        raw_data_delivery_path = max_path + '/' + project_name + '/' + outdir + '/raw_data_check/raw_data_delivery'
        if if_two_source == 'True':
            all_raw_data_std_path = max_path + '/' + project_name + '/' + outdir + '/raw_data_check/raw_data_std'
    
    # 备份原始未修改数据
    #if if_two_source == 'True':
    #    history_raw_data_delivery_path = max_path + '/' + project_name + '/201912/raw_data_std'
    #else:
    #    history_raw_data_delivery_path = max_path + '/' + project_name + '/201912/raw_data'

    # %%
    # =============  数据执行 ==============
    raw_data = spark.read.csv(raw_data_path, header=True)    
    if 'Corp' not in raw_data.columns:
        raw_data = raw_data.withColumn('Corp', func.lit(''))
    if 'Route' not in raw_data.columns:
        raw_data = raw_data.withColumn('Route', func.lit(''))
    for colname, coltype in raw_data.dtypes:
        if coltype == "boolean":
            raw_data = raw_data.withColumn(colname, raw_data[colname].cast(StringType()))
    
    # 1. 同sheet去重(两行完全一样的)
    raw_data = raw_data.groupby(raw_data.columns).count()
    same_sheet_dup = raw_data.where(raw_data['count'] > 1)
    
    # 重复条目数情况
    describe = same_sheet_dup.groupby('Sheet', 'Path').count() \
                        .withColumnRenamed('count', 'dup_count') \
                        .join(raw_data.groupby('Sheet', 'Path').count(), on=['Sheet', 'Path'], how='left')
    describe = describe.withColumn('ratio', describe['dup_count']/describe['count'])
    print("同Sheet中重复条目数:", describe.show())
    
    
    # 同sheet重复条目输出  
    if same_sheet_dup.count() > 0:
        same_sheet_dup = same_sheet_dup.repartition(1)
        same_sheet_dup.write.format("csv").option("header", "true") \
            .mode("overwrite").save(same_sheet_dup_path)
    
    # group计算金额和数量（std_names + ['Sheet', 'Path']）
    raw_data = raw_data.withColumn('Sales', raw_data['Sales'].cast(DoubleType())) \
                    .withColumn('Units', raw_data['Units'].cast(DoubleType())) \
                    .withColumn('Units_Box', raw_data['Units_Box'].cast(DoubleType()))
    raw_data = raw_data.groupby(std_names + ['Sheet', 'Path']).agg(func.sum(raw_data.Sales).alias('Sales'), 
                                                func.sum(raw_data.Units).alias('Units'), 
                                                func.sum(raw_data.Units_Box).alias('Units_Box')).persist()
    
    # 分子名新旧转换
    molecule_adjust = spark.read.csv(molecule_adjust_path, header=True)
    molecule_adjust = molecule_adjust.dropDuplicates(["Mole_Old"])
    
    raw_data = raw_data.join(molecule_adjust, raw_data['Molecule'] == molecule_adjust['Mole_Old'], how='left').persist()
    raw_data = raw_data.withColumn("S_Molecule", func.when(raw_data.Mole_New.isNull(), raw_data.Molecule).otherwise(raw_data.Mole_New)) \
                        .drop('Mole_Old', 'Mole_New')

    # %%
    # 2. 跨sheet去重
    # 2.1 path来自不同月份文件夹：'Date, S_Molecule' 优先最大月份文件夹来源的数据，生成去重后结果 raw_data_dedup_bymonth
    
    # 获取path的日期文件夹名, 纯数字的为文件夹名，只要包含字符就不是月份名
    @udf(StringType())
    def path_split(path):
        path_month = path.replace('//', '/').split('/')
        month = ''
        for each in path_month:
            if len(re.findall('\D+', each)) == 0:
                month = each
        return month
    
    dedup_check_bymonth = raw_data.select('Date', 'S_Molecule', 'Path', 'Sheet', 'Source').distinct() \
                                .withColumn('month_dir', path_split(raw_data.Path))
    dedup_check_bymonth = dedup_check_bymonth.withColumn('month_dir', dedup_check_bymonth.month_dir.cast(IntegerType()))
    # func.rank() 排名重复占位
    dedup_check_bymonth = dedup_check_bymonth.withColumn('rank', func.rank().over(Window.partitionBy('Date', 'S_Molecule', 'Source') \
                                                                    .orderBy(dedup_check_bymonth['month_dir'].desc()))).persist()
    dedup_check_bymonth = dedup_check_bymonth.where(dedup_check_bymonth['rank'] == 1).select('Date', 'S_Molecule', 'Path', 'Sheet','Source')
    
    # 去重后数据
    raw_data_dedup_bymonth = raw_data.join(dedup_check_bymonth, on=['Date', 'S_Molecule', 'Path', 'Sheet','Source'], how='inner').persist()
    # 重复数据
    across_sheet_dup_bymonth = raw_data.join(dedup_check_bymonth, on=['Date', 'S_Molecule', 'Path', 'Sheet', 'Source'], how='left_anti')
    
    print('跨sheet去重，不同月份文件夹来源数据去重情况：')
    print('重复条目数：', across_sheet_dup_bymonth.count())
    print('去重后条目数：', raw_data_dedup_bymonth.count())
    print('总条目数：', raw_data.count())
    
    if across_sheet_dup_bymonth.count() > 0:
        across_sheet_dup_bymonth = across_sheet_dup_bymonth.repartition(1)
        across_sheet_dup_bymonth.write.format("csv").option("header", "true") \
            .mode("overwrite").save(across_sheet_dup_bymonth_path)
    
    # 2.2  保留金额大的数据，生成去重后结果 raw_data_dedup
    dedup_check_bysales = raw_data_dedup_bymonth.groupby('Date', 'S_Molecule', 'Path', 'Sheet', 'Source') \
                            .agg(func.sum(raw_data_dedup_bymonth.Sales).alias('Sales'), func.sum(raw_data_dedup_bymonth.Sales).alias('Units'))
    # func.row_number()
    dedup_check_bysales = dedup_check_bysales.withColumn('rank', func.row_number().over(Window.partitionBy('Date', 'S_Molecule', 'Source') \
                                                                    .orderBy(dedup_check_bysales['Sales'].desc()))).persist()
    dedup_check_bysales = dedup_check_bysales.where(dedup_check_bysales['rank'] == 1).select('Date', 'S_Molecule', 'Path', 'Sheet','Source')
    
    # 去重后数据
    raw_data_dedup = raw_data_dedup_bymonth.join(dedup_check_bysales, on=['Date', 'S_Molecule', 'Path', 'Sheet','Source'], how='inner').persist()
    # 重复数据
    across_sheet_dup = raw_data_dedup_bymonth.join(dedup_check_bysales, on=['Date', 'S_Molecule', 'Path', 'Sheet','Source'], how='left_anti')
    
    print('跨sheet去重，根据金额去重情况：')
    print('重复条目数：', across_sheet_dup.count())
    print('去重后条目数：', raw_data_dedup.count())
    print('总条目数：', raw_data.count())
    
    if across_sheet_dup.count() > 0:
        across_sheet_dup = across_sheet_dup.repartition(1)
        across_sheet_dup.write.format("csv").option("header", "true") \
            .mode("overwrite").save(across_sheet_dup_path)
    
    # ID 的长度统一
    def distinguish_cpa_gyc(col, gyc_hospital_id_length):
        # gyc_hospital_id_length是国药诚信医院编码长度，一般是7位数字，cpa医院编码一般是6位数字。医院编码长度可以用来区分cpa和gyc
        return (func.length(col) < gyc_hospital_id_length)
    def deal_ID_length(df):
        # 不足6位补足
        df = df.withColumn("ID", df["ID"].cast(StringType()))
        df = df.withColumn("ID", func.regexp_replace("ID", "\\.0", ""))
        df = df.withColumn("ID", func.when(distinguish_cpa_gyc(df.ID, 7), func.lpad(df.ID, 6, "0")).otherwise(df.ID))
        return df

    # %%
    # 3. 跨源去重，跨源去重优先保留CPA医院
    cpa_pha_mapping = spark.read.parquet(cpa_pha_mapping_path)
    cpa_pha_mapping = cpa_pha_mapping.where(cpa_pha_mapping["推荐版本"] == 1).select('ID', 'PHA')
    cpa_pha_mapping = deal_ID_length(cpa_pha_mapping)
    
    if if_two_source == 'True':
        # drop_dup_hospital
        cpa_pha_mapping_common = spark.read.parquet(cpa_pha_mapping_common_path)
        cpa_pha_mapping_common = cpa_pha_mapping_common.where(cpa_pha_mapping_common["推荐版本"] == 1).select('ID', 'PHA')
        cpa_pha_mapping_common = deal_ID_length(cpa_pha_mapping_common)
        # raw_data_dedup = raw_data_dedup.withColumn('ID', raw_data_dedup.ID.cast(IntegerType()))
        raw_data_dedup = deal_ID_length(raw_data_dedup)
    
        def drop_dup_hospital(df, cpa_pha_map):
            column_names = df.columns
            df = df.join(cpa_pha_map, on='ID', how='left')
    
            Source_window = Window.partitionBy("Date", "PHA").orderBy(func.col('Source'))
            rank_window = Window.partitionBy("Date", "PHA").orderBy(func.col('Source').desc())
            Source = df.select("Date", "PHA", 'Source').distinct() \
                .select("Date", "PHA", func.collect_list(func.col('Source')).over(Source_window).alias('Source_list'),
                func.rank().over(rank_window).alias('rank')).persist()
            Source = Source.where(Source.rank == 1).drop('rank')
            Source = Source.withColumn('count', func.size('Source_list'))
    
            df = df.join(Source, on=['Date', 'PHA'], how='left')
    
            # 无重复
            df1 = df.where((df['count'] <=1) | (df['count'].isNull()))
            # 有重复
            df2 = df.where(df['count'] >1)
            df2 = df2.withColumn('Source_choice', 
                            func.when(func.array_contains('Source_list', 'CPA'), func.lit('CPA')) \
                                .otherwise(func.when(func.array_contains('Source_list', 'GYC'), func.lit('GYC')) \
                                                .otherwise(func.lit('DDD'))))
            df2 = df2.where(df2['Source'] == df2['Source_choice'])
    
            # 合并
            df_all = df1.union(df2.select(df1.columns))
    
            '''
            df_filter = df.where(~df.PHA.isNull()).select('Date', 'PHA', 'ID').distinct() \
                                    .groupby('Date', 'PHA').count()
    
            df = df.join(df_filter, on=['Date', 'PHA'], how='left')
    
            # 有重复的优先保留CPA（CPA的ID为6位，GYC的ID为7位）,其次是国药，最后是其他
            df = df.where((df['count'] <=1) | ((df['count'] > 1) & (func.length('ID')==6)) | (df['count'].isNull()))
    
            '''
    
            # 检查去重结果
            check = df_all.select('PHA','Date').distinct() \
                        .groupby('Date', 'PHA').count()
            check_dup = check.where(check['count'] >1 ).where(~check.PHA.isNull())
            if check_dup.count() > 0 :
                print('有未去重的医院')
    
            df_all = df_all.select(column_names)
            return df_all
    
        raw_data_dedup_std = drop_dup_hospital(raw_data_dedup, cpa_pha_mapping_common)                  
        raw_data_dedup = drop_dup_hospital(raw_data_dedup, cpa_pha_mapping)
        
    # %%
    # 4. 与历史数据合并
    def union_raw_data(raw_data_dedup, history_raw_data_path, all_raw_data_path, cpa_pha_map):
        # 1. 历史数据
        history_raw_data = spark.read.parquet(history_raw_data_path)
        if 'Corp' not in history_raw_data.columns:
            history_raw_data = history_raw_data.withColumn('Corp', func.lit(''))
        if 'Route' not in history_raw_data.columns:
            history_raw_data = history_raw_data.withColumn('Route', func.lit(''))
        for colname, coltype in history_raw_data.dtypes:
            if coltype == "boolean":
                history_raw_data = history_raw_data.withColumn(colname, history_raw_data[colname].cast(StringType()))
                
        history_raw_data = history_raw_data.withColumn('Date', history_raw_data.Date.cast(IntegerType()))
        history_raw_data = history_raw_data.where(history_raw_data.Date < cut_time_left)
        history_raw_data = history_raw_data.drop("Brand_new", "all_info")
        history_raw_data = deal_ID_length(history_raw_data)
        # 匹配PHA
        history_raw_data = history_raw_data.join(cpa_pha_map, on='ID', how='left')
    
        # 2. 本期数据
        raw_data_dedup = raw_data_dedup.withColumn('Date', raw_data_dedup.Date.cast(IntegerType()))
        # new_raw_data = raw_data_dedup.where((raw_data_dedup.Date >= cut_time_left) & (raw_data_dedup.Date <= cut_time_right))
        new_raw_data = deal_ID_length(raw_data_dedup)
        # 匹配PHA
        new_raw_data = new_raw_data.join(cpa_pha_map, on='ID', how='left')
        # 本期 ID
        new_date_mole_id = new_raw_data.select('Date', 'Molecule', 'ID').distinct()
        # 本期 PHA
        new_date_mole_pha = new_raw_data.where(~col('PHA').isNull()).select('Date', 'Molecule', 'PHA').distinct()
        
        # 3. 去重
        history_raw_data = history_raw_data.join(new_date_mole_id, on=['Date', 'Molecule', 'ID'], how='left_anti') \
                                            .join(new_date_mole_pha, on=['Date', 'Molecule', 'PHA'], how='left_anti')
        # 4.合并数据
        all_raw_data = new_raw_data.select(history_raw_data.columns).union(history_raw_data) \
                                    .drop('PHA')
        all_raw_data = deal_ID_length(all_raw_data)
    
        all_raw_data = all_raw_data.repartition(4)
        all_raw_data.write.format("parquet") \
            .mode("overwrite").save(all_raw_data_path)

    # %%
    # 与历史数据合并   
    if if_union == 'True':
        # 输出临时文件 
        raw_data_dedup.repartition(4).write.format("parquet") \
                .mode("overwrite").save(tmp_path_raw_data_dedup)   
        if if_two_source == 'True':
            raw_data_dedup_std.repartition(4).write.format("parquet") \
                .mode("overwrite").save(tmp_path_raw_data_dedup_std)
            
        # 用于max计算
        raw_data_dedup = spark.read.parquet(tmp_path_raw_data_dedup)
        union_raw_data(raw_data_dedup, history_raw_data_path, all_raw_data_path, cpa_pha_mapping)             
        if if_two_source == 'True':
            # 用于max计算
            raw_data_dedup_std = spark.read.parquet(tmp_path_raw_data_dedup_std)
            union_raw_data(raw_data_dedup_std, history_raw_data_std_path, all_raw_data_std_path, cpa_pha_mapping_common)
            # 备份生成手动修改前的交付结果，用于交付和提数
            raw_data_dedup_std = spark.read.parquet(tmp_path_raw_data_dedup_std)
            union_raw_data(raw_data_dedup_std, history_raw_data_delivery_path, raw_data_delivery_path, cpa_pha_mapping_common)
        else:
            # 备份生成手动修改前的交付结果，用于交付和提数
            union_raw_data(raw_data_dedup, history_raw_data_delivery_path, raw_data_delivery_path, cpa_pha_mapping)   
    # 不与历史数据合并        
    else:
        raw_data_dedup = deal_ID_length(raw_data_dedup)
    
        raw_data_dedup = raw_data_dedup.repartition(4)
        raw_data_dedup.write.format("parquet") \
            .mode("overwrite").save(all_raw_data_path)
    
        if if_two_source == 'True':
            raw_data_dedup_std = deal_ID_length(raw_data_dedup_std)
    
            raw_data_dedup_std = raw_data_dedup_std.repartition(4)
            raw_data_dedup_std.write.format("parquet") \
                .mode("overwrite").save(all_raw_data_std_path)
            # 备份生成手动修改前的交付结果
            raw_data_dedup_std.write.format("parquet") \
                .mode("overwrite").save(raw_data_delivery_path)
        else:
            # 备份生成手动修改前的交付结果
            raw_data_dedup.write.format("parquet") \
                .mode("overwrite").save(raw_data_delivery_path)



def needclean(kwargs):

    ### input args ###
    max_path = kwargs.get('max_path', 'D:/Auto_MAX/MAX/')
    project_name = kwargs['project_name']
    outdir = kwargs['outdir']
    test = kwargs.get('test', 'False')
    path_change_file = kwargs.get('path_change_file', 'Empty')
    
    if path_change_file == 'Empty':
        minimum_product_sep = kwargs.get('minimum_product_sep', '|')
        minimum_product_columns = kwargs.get('minimum_product_columns', 'Brand, Form, Specifications, Pack_Number, Manufacturer')
    else:
        if_two_source = kwargs.get('if_two_source', 'False')
       
    ### input args ###
    
    
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
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    
    print('needclean')
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





def check(kwargs):

    ### input args ###
    max_path = kwargs.get('max_path', 'D:/Auto_MAX/MAX/')
    project_name = kwargs['project_name']
    outdir = kwargs['outdir']
    minimum_product_sep = kwargs.get('minimum_product_sep', '|')
    minimum_product_columns = kwargs.get('minimum_product_columns', 'Brand, Form, Specifications, Pack_Number, Manufacturer')
    current_year = kwargs['current_year']
    current_month = kwargs['current_month']
    three = kwargs.get('three', '3')
    twelve = kwargs.get('twelve', '12')
    test = kwargs.get('test', 'False')
    g_id_molecule = kwargs.get('g_id_molecule', 'True')
    ### input args ###

    
    
    from pyspark.sql import SparkSession, Window
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    import os
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, greatest, least, col
    import time
    import pandas as pd
    import numpy as np    

    spark = SparkSession.builder.getOrCreate()
    print('check')
    
    # %%
    # project_name = 'Gilead'
    # outdir = '202101'
    # current_month = '1'
    # current_year = '2021'
    # minimum_product_sep = "|"
    # g_id_molecule = 'False'

    # %%
    # 输入
    current_year = int(current_year)
    current_month = int(current_month)
    three = int(three)
    twelve = int(twelve)
    
    if minimum_product_sep == "kong":
        minimum_product_sep = ""
    minimum_product_columns = minimum_product_columns.replace(" ","").split(",")
    product_map_path = max_path + '/' + project_name + '/' + outdir + '/prod_mapping'
    cpa_pha_mapping_path = max_path + '/' + project_name + '/cpa_pha_mapping'
    province_city_mapping_path = max_path + '/' + project_name + '/province_city_mapping'
    if test == 'True':
        raw_data_path = max_path + '/' + project_name + '/' + outdir + '/raw_data_check/raw_data'
    else:
        raw_data_path = max_path + '/' + project_name + '/' + outdir + '/raw_data'
    raw_data_check_path = max_path + '/' + project_name + '/' + outdir + '/raw_data_check/'
    
    # 输出
    check_result_path = raw_data_check_path + '/check_result.csv'
    check_1_path = raw_data_check_path + '/check_1_每个月产品个数.csv'
    check_2_path = raw_data_check_path + '/check_2_各产品历史月份销量.csv'
    check_3_path = raw_data_check_path + '/check_3_历史医院个数.csv'
    check_5_path = raw_data_check_path + '/check_5_最近12期每家医院每个月的金额规模.csv'
    check_8_path = raw_data_check_path + '/check_8_每个医院每个月产品个数.csv'
    check_9_1_path = raw_data_check_path + '/check_9_1_所有产品每个月金额.csv'
    check_9_2_path = raw_data_check_path + '/check_9_2_所有产品每个月份额.csv'
    check_9_3_path = raw_data_check_path + '/check_9_3_所有产品每个月排名.csv'
    check_10_path = raw_data_check_path + '/check_10_在售产品医院个数.csv'
    
    check_11_path = raw_data_check_path + '/check_11_金额_医院贡献率等级.csv'
    check_11_1_path = raw_data_check_path + '/check_11_1_金额_1级头部医院变化倍率.csv'
    
    check_12_path = raw_data_check_path + '/check_12_金额_医院分子贡献率等级.csv'
    check_12_1_path = raw_data_check_path + '/check_12_1_金额_1级头部医院分子变化倍率.csv'
    
    check_13_path = raw_data_check_path + '/check_13_数量_医院贡献率等级.csv'
    check_14_path = raw_data_check_path + '/check_14_数量_医院分子贡献率等级.csv'
    check_15_path = raw_data_check_path + '/check_15_最近12期每家医院每个月每个产品的价格与倍数.csv'
    check_16_path = raw_data_check_path + '/check_16_各医院各产品价格与所在地区对比.csv'
    #tmp_1_path  = raw_data_check_path + '/tmp_1'
    #tmp_2_path  = raw_data_check_path + '/tmp_2'

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
    # ==== 一. 数据准备  ==== 
    
    def deal_ID_length(df):
        # ID不足7位的补足0到6位
        # 国药诚信医院编码长度是7位数字，cpa医院编码是6位数字。
        df = df.withColumn("ID", df["ID"].cast(StringType()))
        # 去掉末尾的.0
        df = df.withColumn("ID", func.regexp_replace("ID", "\\.0", ""))
        df = df.withColumn("ID", func.when(func.length(df.ID) < 7, func.lpad(df.ID, 6, "0")).otherwise(df.ID))
        return df
    
    # 1. cpa_pha_mapping，province_city_mapping 文件
    cpa_pha_mapping = spark.read.parquet(cpa_pha_mapping_path)
    cpa_pha_mapping = cpa_pha_mapping.where(cpa_pha_mapping["推荐版本"] == 1).select('ID', 'PHA').distinct()
    cpa_pha_mapping = deal_ID_length(cpa_pha_mapping)
    
    province_city_mapping = spark.read.parquet(province_city_mapping_path)
    province_city_mapping = deal_ID_length(province_city_mapping)
    city_list = ['北京市','常州市','广州市','济南市','宁波市','上海市','苏州市','天津市','温州市','无锡市']
    province_city_mapping = province_city_mapping.withColumn('Province', func.when(col('City').isin(['福州市','厦门市','泉州市']), func.lit('福厦泉')) \
                                                                    .otherwise(func.when(col('City').isin(city_list), col('City')) \
                                                                               .otherwise(col('Province'))))
    
    # 2. Raw_data 处理
    Raw_data = spark.read.parquet(raw_data_path)
    Raw_data = Raw_data.withColumn('Date', Raw_data['Date'].cast(IntegerType())) \
                    .withColumn('Units', Raw_data['Units'].cast(DoubleType())) \
                    .withColumn('Sales', Raw_data['Sales'].cast(DoubleType()))
    if 'Pack_ID' in Raw_data.columns:
        Raw_data = Raw_data.drop('Pack_ID')
    
    # 生成min1
    # if project_name != 'Mylan':
    Raw_data = Raw_data.withColumn('Brand_bak', Raw_data.Brand)
    Raw_data = Raw_data.withColumn('Brand', func.when((Raw_data.Brand.isNull()) | (Raw_data.Brand == 'NA'), Raw_data.Molecule).
                                                otherwise(Raw_data.Brand))
    Raw_data = Raw_data.withColumn("min1", func.when(Raw_data[minimum_product_columns[0]].isNull(), func.lit("NA")).
                                       otherwise(Raw_data[minimum_product_columns[0]]))
    for i in minimum_product_columns[1:]:
        Raw_data = Raw_data.withColumn(i, Raw_data[i].cast(StringType()))
        Raw_data = Raw_data.withColumn("min1", func.concat(
            Raw_data["min1"],
            func.lit(minimum_product_sep),
            func.when(func.isnull(Raw_data[i]), func.lit("NA")).otherwise(Raw_data[i])))
    Raw_data = Raw_data.withColumn('Brand', Raw_data.Brand_bak).drop('Brand_bak')
    
    # 3. 产品匹配表处理 
    product_map = spark.read.parquet(product_map_path)
    # a. 列名清洗统一
    # if project_name == "Sanofi" or project_name == "AZ":
    #     product_map = product_map.withColumnRenamed(product_map.columns[21], "pfc")
    # if project_name == "Eisai":
    #     product_map = product_map.withColumnRenamed(product_map.columns[22], "pfc")
    for i in product_map.columns:
        if i in ["标准通用名", "通用名_标准", "药品名称_标准", "S_Molecule_Name"]:
            product_map = product_map.withColumnRenamed(i, "通用名")
        if i in ["min1_标准"]:
            product_map = product_map.withColumnRenamed(i, "min2")
        if i in ["packcode", "Pack_ID", "Pack_Id", "PackID", "packid"]:
            product_map = product_map.withColumnRenamed(i, "pfc")
        if i in ["商品名_标准", "S_Product_Name"]:
            product_map = product_map.withColumnRenamed(i, "标准商品名")
        if i in ["剂型_标准", "Form_std", "S_Dosage"]:
            product_map = product_map.withColumnRenamed(i, "标准剂型")
        if i in ["规格_标准", "Specifications_std", "药品规格_标准", "S_Pack"]:
            product_map = product_map.withColumnRenamed(i, "标准规格")
        if i in ["包装数量2", "包装数量_标准", "Pack_Number_std", "S_PackNumber", "最小包装数量"]:
            product_map = product_map.withColumnRenamed(i, "标准包装数量")
        if i in ["标准企业", "生产企业_标准", "Manufacturer_std", "S_CORPORATION", "标准生产厂家"]:
            product_map = product_map.withColumnRenamed(i, "标准生产企业")
    if project_name == "Janssen" or project_name == "NHWA":
        if "标准剂型" not in product_map.columns:
            product_map = product_map.withColumnRenamed("剂型", "标准剂型")
        if "标准规格" not in product_map.columns:
            product_map = product_map.withColumnRenamed("规格", "标准规格")
        if "标准生产企业" not in product_map.columns:
            product_map = product_map.withColumnRenamed("生产企业", "标准生产企业")
        if "标准包装数量" not in product_map.columns:
            product_map = product_map.withColumnRenamed("包装数量", "标准包装数量")
    
    # b. 选取需要的列
    product_map = product_map \
                    .select("min1", "min2", "pfc", "通用名", "标准商品名", "标准剂型", "标准规格", "标准包装数量", "标准生产企业") \
                    .distinct() \
                    .withColumnRenamed("标准商品名", "商品名") \
                    .withColumnRenamed("标准剂型", "剂型") \
                    .withColumnRenamed("标准规格", "规格") \
                    .withColumnRenamed("标准包装数量", "包装数量") \
                    .withColumnRenamed("标准生产企业", "生产企业") \
                    .withColumnRenamed("pfc", "Pack_ID")
    
    product_map = product_map.withColumn('Pack_ID', product_map.Pack_ID.cast(IntegerType()))
    
    # 4. 匹配产品匹配表，标准化min2通用名商品名
    Raw_data_1 = Raw_data.join(product_map.select('min1','min2','通用名','商品名','Pack_ID').dropDuplicates(['min1']), on='min1', how='left')
    Raw_data_1 = Raw_data_1.groupby('ID', 'Date', 'min2', '通用名','商品名','Pack_ID') \
                            .agg(func.sum('Sales').alias('Sales'), func.sum('Units').alias('Units')) \
                            .withColumnRenamed('min2', 'Prod_Name')

    # %%
    #========== 定义 ==========
    def WriteCsvOut(df, out_path, repartition_num=1):
        df = df.repartition(repartition_num)
        df.write.format("csv").option("header", "true") \
            .mode("overwrite").save(out_path)
    # %%
    #========== check_1 ==========
    
    # 每个月产品个数(min2)
    check_1 = Raw_data_1.select('Date', 'Prod_Name').distinct() \
                        .groupby('Date').count() \
                        .withColumnRenamed('count', '每月产品个数_min2') \
                        .orderBy('Date').persist()
    
    ### 判断产品个数与上月相比是否超过 8%
    MTH_product_num = check_1.where(check_1.Date == MTH).toPandas()['每月产品个数_min2'][0]
    PREMTH_product_num = check_1.where(check_1.Date == PREMTH).toPandas()['每月产品个数_min2'][0]
    check_result_1 = (MTH_product_num/PREMTH_product_num < 0.08)
    
    WriteCsvOut(check_1, check_1_path)
    # %%
    #========== check_2 ==========
    
    # 各产品历史月份销量
    check_2 = Raw_data_1.groupby('Date', 'Prod_Name').agg(func.sum('Sales').alias('Sales'))
    check_2 = check_2.groupBy("Prod_Name").pivot("Date").agg(func.sum('Sales')).persist()
    
    ### 判断缺失产品是否在上个月销售金额超过 2%
    MTH_product_Sales = check_2.where(check_2[str(MTH)].isNull()).groupBy().agg(func.sum(str(PREMTH)).alias('sum')).toPandas()['sum'][0]
    PREMTH_product_Sales = check_2.groupBy().agg(func.sum(str(PREMTH)).alias('sum')).toPandas()['sum'][0]
    if MTH_product_Sales == None:
        MTH_product_Sales = 0
    check_result_2 = (MTH_product_Sales/PREMTH_product_Sales < 0.08)
    
    WriteCsvOut(check_2, check_2_path)
    # %%
    #========== check_3 ==========
    
    # 历史医院个数
    check_3 = Raw_data.select('Date', 'ID').distinct() \
                    .groupBy('Date').count() \
                    .withColumnRenamed('count', '医院个数') \
                    .orderBy('Date').persist()
    
    ### 判断历史医院个数是否超过1%                
    MTH_hospital_num = check_3.where(check_3.Date == MTH).toPandas()['医院个数'][0]
    PREMTH_hospital_num = check_3.where(check_3.Date == PREMTH).toPandas()['医院个数'][0]
    check_result_3 = (MTH_hospital_num/PREMTH_hospital_num -1 < 0.01)
    
    WriteCsvOut(check_3, check_3_path)
    # %%
    #========== check_5 ==========
    
    # 最近12期每家医院每个月的销量规模
    check_5_1 = Raw_data.where(Raw_data.Date > (current_year - 1)*100 + current_month - 1) \
                        .groupBy('ID', 'Date').agg(func.sum('Sales').alias('Sales'))
    check_5_1 = check_5_1.groupBy("ID").pivot("Date").agg(func.sum('Sales')).persist() \
                        .orderBy('ID').persist()
    
    ### 检查当月缺失医院在上个月的销售额占比
    MTH_hospital_Sales = check_5_1.where(check_5_1[str(MTH)].isNull()).groupBy().agg(func.sum(str(PREMTH)).alias('sum')).toPandas()['sum'][0]
    PREMTH_hospital_Sales = check_5_1.groupBy().agg(func.sum(str(PREMTH)).alias('sum')).toPandas()['sum'][0]
    if MTH_hospital_Sales == None:
        MTH_hospital_Sales = 0
    check_result_5 = (MTH_hospital_Sales/PREMTH_hospital_Sales < 0.01)
    
    # 每家医院的月销金额在最近12期的误差范围内（mean+-1.96std），范围内的医院数量占比大于95%；
    check_5_2 = Raw_data.where((Raw_data.Date > (current_year-1)*100+current_month-1 ) & (Raw_data.Date < current_year*100+current_month)) \
                        .groupBy('ID', 'Date').agg(func.sum('Sales').alias('Sales')) \
                        .groupBy('ID').agg(func.mean('Sales').alias('Mean_Sales'), func.stddev('Sales').alias('Sd_Sales')).persist()
    
    check_5_2 = check_5_2.join(Raw_data.where(Raw_data.Date == current_year*100+current_month).groupBy('ID').agg(func.sum('Sales').alias('Sales_newmonth')), 
                                            on='ID', how='left').persist()
    check_5_2 = check_5_2.withColumn('Check', func.when(check_5_2.Sales_newmonth < check_5_2.Mean_Sales-1.96*check_5_2.Sd_Sales, func.lit('F')) \
                                                .otherwise(func.when(check_5_2.Sales_newmonth > check_5_2.Mean_Sales+1.96*check_5_2.Sd_Sales, func.lit('F')) \
                                                                .otherwise(func.lit('T'))))
    check_5_2 = check_5_2.withColumn('Check', func.when(func.isnan(check_5_2.Mean_Sales) | func.isnan(check_5_2.Sd_Sales) | check_5_2.Sales_newmonth.isNull(), func.lit(None)) \
                                                    .otherwise(check_5_2.Check))                            
    
    check_5 = check_5_1.join(check_5_2, on='ID', how='left').orderBy('ID').persist()
    
    WriteCsvOut(check_5, check_5_path)
    # %%
    #========== check_6 ==========
    # 目前没用
    def Check6():
        # 最近12期每家医院每个月的销量规模
        check_6_1 = Raw_data.where(Raw_data.Date > (current_year-1)*100+current_month-1) \
                            .groupby('ID', 'Date').agg(func.sum('Units').alias('Units')) \
                            .groupBy("ID").pivot("Date").agg(func.sum('Units')).persist()
    
        # 每家医院的月销数量在最近12期的误差范围内（mean+-1.96std），范围内的医院数量占比大于95%；
        check_6_2 = Raw_data.where((Raw_data.Date > (current_year-1)*100+current_month-1 ) & (Raw_data.Date < current_year*100+current_month)) \
                            .groupBy('ID', 'Date').agg(func.sum('Units').alias('Units')) \
                            .groupBy('ID').agg(func.mean('Units').alias('Mean_Units'), func.stddev('Units').alias('Sd_Units'))
        check_6_2 = check_6_2.join(Raw_data.where(Raw_data.Date == current_year*100+current_month).groupBy('ID').agg(func.sum('Units').alias('Units_newmonth')), 
                                                on='ID', how='left').persist()
        check_6_2 = check_6_2.withColumn('Check', func.when(check_6_2.Units_newmonth < check_6_2.Mean_Units-1.96*check_6_2.Sd_Units, func.lit('F')) \
                                                    .otherwise(func.when(check_6_2.Units_newmonth > check_6_2.Mean_Units+1.96*check_6_2.Sd_Units, func.lit('F')) \
                                                                    .otherwise(func.lit('T'))))
        check_6_2 = check_6_2.withColumn('Check', func.when(func.isnan(check_6_2.Mean_Units) | func.isnan(check_6_2.Sd_Units) | check_6_2.Units_newmonth.isNull(), func.lit(None)) \
                                                        .otherwise(check_6_2.Check)) 
    
        check_6 = check_6_1.join(check_6_2, on='ID', how='left').orderBy('ID')

    # %%
    #========== check_7 ==========
    # 目前没用
    def Check7():
        # 最近12期每家医院每个月每个产品(Packid)的平均价格
        check_7_1 = Raw_data_1.where(Raw_data_1.Date > (current_year-1)*100+current_month-1) \
                            .groupBy('ID', 'Date', '通用名','商品名','Pack_ID') \
                            .agg(func.sum('Sales').alias('Sales'), func.sum('Units').alias('Units')).persist()
        check_7_1 = check_7_1.withColumn('Price', check_7_1.Sales/check_7_1.Units)
        check_7_1 = check_7_1.groupBy('ID', '通用名', '商品名', 'Pack_ID').pivot("Date").agg(func.sum('Price')) \
                            .orderBy('ID', '通用名', '商品名').persist()
    
        # 每家医院的每个产品单价在最近12期的误差范围内（mean+-1.96std或gap10%以内），范围内的产品数量占比大于95%；
        check_7_2 = Raw_data_1.where((Raw_data_1.Date > (current_year-1)*100+current_month-1 ) & (Raw_data_1.Date < current_year*100+current_month)) \
                            .groupBy('ID', 'Date', '通用名', '商品名', 'Pack_ID') \
                            .agg(func.sum('Sales').alias('Sales'), func.sum('Units').alias('Units'))
        check_7_2 = check_7_2.withColumn('Price', check_7_2.Sales/check_7_2.Units)
        check_7_2 = check_7_2.groupBy('ID', '通用名', '商品名', 'Pack_ID') \
                            .agg(func.mean('Price').alias('Mean_Price'), func.stddev('Price').alias('Sd_Price'))
        check_7_2 = check_7_2.withColumn('Sd_Price', func.when(func.isnan(check_7_2.Sd_Price), func.lit(0)).otherwise(check_7_2.Sd_Price))
        Raw_data_1_tmp = Raw_data_1.where(Raw_data_1.Date == current_year*100+current_month) \
                                    .groupBy('ID', '通用名', '商品名', 'Pack_ID') \
                                    .agg(func.sum('Sales').alias('Sales_newmonth'), func.sum('Units').alias('Units_newmonth'))
        Raw_data_1_tmp = Raw_data_1_tmp.withColumn('Price_newmonth', Raw_data_1_tmp.Sales_newmonth/Raw_data_1_tmp.Units_newmonth)
        Raw_data_1_tmp = Raw_data_1_tmp.withColumn('Pack_ID', func.when(Raw_data_1_tmp.Pack_ID.isNull(), func.lit(0)).otherwise(Raw_data_1_tmp.Pack_ID))
        check_7_2 = check_7_2.withColumn('Pack_ID', func.when(check_7_2.Pack_ID.isNull(), func.lit(0)).otherwise(check_7_2.Pack_ID))
        check_7_2 = check_7_2.join(Raw_data_1_tmp, on=['ID', '通用名', '商品名', 'Pack_ID'], how='left').persist()
    
        check_7_2 = check_7_2.withColumn('Check', \
                    func.when((check_7_2.Price_newmonth < check_7_2.Mean_Price-1.96*check_7_2.Sd_Price) & (check_7_2.Price_newmonth < check_7_2.Mean_Price*0.9), func.lit('F')) \
                        .otherwise(func.when((check_7_2.Price_newmonth > check_7_2.Mean_Price+1.96*check_7_2.Sd_Price) & (check_7_2.Price_newmonth > check_7_2.Mean_Price*1.1), func.lit('F')) \
                                        .otherwise(func.lit('T'))))
        check_7_2 = check_7_2.withColumn('Check', func.when(check_7_2.Sales_newmonth.isNull() | check_7_2.Units_newmonth.isNull(), func.lit(None)) \
                                                        .otherwise(check_7_2.Check)) 
    
        check_7_1 = check_7_1.withColumn('Pack_ID', func.when(check_7_1.Pack_ID.isNull(), func.lit(0)).otherwise(check_7_1.Pack_ID))
        check_7 = check_7_1.join(check_7_2, on=['ID', '通用名', '商品名', 'Pack_ID'], how='left').orderBy('ID', '通用名', '商品名').persist()
        check_7 = check_7.withColumn('Pack_ID', func.when(check_7.Pack_ID == 0, func.lit(None)).otherwise(check_7.Pack_ID))
    
        check_7.groupby('Check').count().show()

    # %%
    #========== check_8 ==========
    
    # 每个月产品个数
    check_8 = Raw_data_1.select('Date', 'ID', 'Prod_Name').distinct() \
                        .groupBy('Date').count() \
                        .withColumnRenamed('count', '每月产品个数_min1') \
                        .orderBy('Date').persist()
    # 最近三个月全部医院的产品总数
    check_8_1 = Raw_data_1.where(Raw_data_1.Date.isin(RQMTH)) \
                        .select('Date', 'ID', 'Prod_Name').distinct() \
                        .count()/3
    # 当月月产品个数                  
    check_8_2 = Raw_data_1.where(Raw_data_1.Date.isin(MTH)) \
                        .select('Date', 'ID', 'Prod_Name').distinct() \
                        .count()
    
    check_result_8 = (check_8_2/check_8_1 < 0.03)
    
    WriteCsvOut(check_8, check_8_path)
    # %%
    #========== check_9 ==========
    
    # 全部医院的全部产品金额、份额、排名与历史月份对比(含缺失医院)
    check_9_1 = Raw_data_1.groupBy('Date', '商品名').agg(func.sum(Raw_data_1.Sales).alias('Sales')) \
                        .groupBy('商品名').pivot('Date').agg(func.sum('Sales')).persist()
    check_9_2 = Raw_data_1.groupBy('Date', '商品名').agg(func.sum(Raw_data_1.Sales).alias('Sales')) \
                        .join(Raw_data_1.groupBy('Date').agg(func.sum('Sales').alias('Sales_month')), on='Date', how='left')
    check_9_2 = check_9_2.withColumn('share', check_9_2.Sales/check_9_2.Sales_month) \
                        .groupBy('商品名').pivot('Date').agg(func.sum('share')).persist()
    check_9_3 = Raw_data_1.groupBy('Date', '商品名').agg(func.sum(Raw_data_1.Sales).alias('Sales'))
    check_9_3 = check_9_3.withColumn('Rank', func.row_number().over(Window.partitionBy('Date').orderBy(check_9_3['Sales'].desc())))
    check_9_3 = check_9_3.groupBy('商品名').pivot('Date').agg(func.sum('Rank')).persist()
    
    WriteCsvOut(check_9_1, check_9_1_path)
    WriteCsvOut(check_9_2, check_9_2_path)
    WriteCsvOut(check_9_3, check_9_3_path)
    # %%
    #========== check_10 ==========
    
    # group by 产品和月份，count 医院ID，省份
    # 检查是否有退市产品突然有销量；例如（17120906_2019M12,Pfizer_HTN）
    check_10 = Raw_data_1.select('Date', 'Prod_Name', 'ID').distinct() \
                        .groupBy('Date', 'Prod_Name').count() \
                        .withColumnRenamed('count', '在售产品医院个数') \
                        .groupBy('Prod_Name').pivot('Date').agg(func.sum('在售产品医院个数')).persist()
    WriteCsvOut(check_10, check_10_path)
    # %%
    #========== check_贡献率等级相关 ==========
    
    @udf(DoubleType())
    def mean_adj(*cols):
        # 以行为单位，去掉一个最大值和一个最小值求平均
        import numpy as np
        row_max = cols[0]
        row_min = cols[1]
        others = cols[2:]
        row_mean_list = [x for x in others if x is not None]
        if len(row_mean_list) > 3:
            row_mean = (np.sum(row_mean_list) - row_max - row_min)/(len(row_mean_list) -2)
        else:
            row_mean = 0
        return float(row_mean)
    
    @udf(DoubleType())
    def min_diff(row_max, row_min, mean_adj):
        # row_min 与 mean_adj 的差值
        import numpy as np
        if mean_adj is not None:
            # diff1 = abs(row_max - mean_adj)
            diff2 = abs(row_min - mean_adj)
            row_diff = diff2
        else:
            row_diff = 0
        return float(row_diff)
    
    def func_pandas_cumsum_level(pdf, grouplist, sumcol):
        '''
        贡献率等级计算:
        分月统计
        降序排列累加求和，占总数的比值乘10，取整   
        '''
        month = pdf['Date'][0]
        pdf = pdf.groupby(grouplist)[sumcol].agg('sum').reset_index()
        pdf = pdf.sort_values(sumcol, ascending=False)
        pdf['cumsum'] = pdf[sumcol].cumsum()
        pdf['sum'] = pdf[sumcol].sum()
        pdf['con_add'] = pdf['cumsum']/pdf['sum']
        pdf['level'] = np.where(pdf['con_add']*10 > 10, 10, np.ceil(pdf['con_add']*10))
        pdf ['month'] = str(month)
        pdf = pdf[grouplist + ['level', 'month']]
        pdf['level'].astype('int')
        return pdf
    
    def colculate_diff(check_num, grouplist):
        '''
        去掉最大值和最小值求平均
        最小值与平均值的差值min_diff
        '''
        check_num_cols = check_num.columns
        check_num_cols = list(set(check_num_cols) - set(grouplist))
        for each in check_num_cols:
            check_num = check_num.withColumn(each, check_num[each].cast(IntegerType()))
    
        # 平均值计算
        check_num = check_num.withColumn("row_max", func.greatest(*check_num_cols)) \
                        .withColumn("row_min", func.least(*check_num_cols))
        check_num = check_num.withColumn("mean_adj", 
                            mean_adj(func.col('row_max'), func.col('row_min'), *(func.col(x) for x in check_num_cols)))
        check_num = check_num.withColumn("mean_adj", func.when(func.col('mean_adj') == 0, func.lit(None)).otherwise(func.col('mean_adj')))
        # row_min 与 mean_adj 的差值
        check_num = check_num.withColumn("min_diff", min_diff(func.col('row_max'), func.col('row_min'), func.col('mean_adj')))
        check_num = check_num.withColumn("min_diff", func.when(func.col('mean_adj').isNull(), func.lit(None)).otherwise(func.col('min_diff')))
    
        # 匹配PHA    
        check_num = check_num.join(cpa_pha_mapping, on='ID', how='left')    
        # 排序
        check_num = check_num.orderBy(col('min_diff').desc(), col('mean_adj').desc())
        
        return check_num
    
    def CheckTopChange(check_path, Raw_data, grouplist):
        # 1级头部医院变化倍率检查
        check_have = spark.read.csv(check_path, header=True)
        check_top = Raw_data.join(check_have.where(col('row_min') == 1).select('ID').distinct(), on='ID', how='inner') \
                                .groupby(grouplist + ['Date']).agg(func.sum('Sales').alias('Sales'))
    
        check_top_tmp = check_top.groupby(grouplist) \
                    .agg(func.max('Sales').alias('max_Sales'), 
                         func.min('Sales').alias('min_Sales'), 
                         func.sum('Sales').alias('sum_Sales'),
                         func.count('Sales').alias('count'))
    
        check_top = check_top.join(check_top_tmp, on=grouplist, how='left') \
                                .withColumn('Mean_Sales', func.when(col('count') > 2, (col('sum_Sales') - col('max_Sales') - col('min_Sales'))/(col('count') -2) ) \
                                                        .otherwise(col('sum_Sales')/col('count') )) \
                                .withColumn('Mean_times', func.round(col('Sales')/col('Mean_Sales'),1 )) \
                                .groupBy(grouplist).pivot('Date').agg(func.sum('Mean_times'))
    
        check_top = check_top.withColumn("max", func.greatest(*list(set(check_top.columns) - set(grouplist))))
        
        return check_top
    # %%
    Raw_data = deal_ID_length(Raw_data)
    
    #========== check_11 金额==========
    schema = StructType([
            StructField("ID", StringType(), True),
            StructField("level", IntegerType(), True),
            StructField("month", StringType(), True)
            ])
    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def pudf_cumsum_level_11(pdf):
        return func_pandas_cumsum_level(pdf, grouplist=['ID'], sumcol='Sales')
    
    check_11 = Raw_data.groupby(["Date"]).apply(pudf_cumsum_level_11)
    check_11 = check_11.groupby('ID').pivot('month').agg(func.sum('level')).persist()
    check_11 = colculate_diff(check_11, grouplist=['ID'])
    WriteCsvOut(check_11, check_11_path)
    
    # 1级头部医院金额变化倍率检查
    check_11_1 = CheckTopChange(check_11_path, Raw_data, grouplist=['ID'])
    WriteCsvOut(check_11_1, check_11_1_path)
    # %%
    #========== check_12 金额==========
    if g_id_molecule == 'True':
        schema = StructType([
                StructField("ID", StringType(), True),
                StructField("Molecule", StringType(), True),
                StructField("level", IntegerType(), True),
                StructField("month", StringType(), True)
                ])
        @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
        def pudf_cumsum_level_12(pdf):
            return func_pandas_cumsum_level(pdf, grouplist=['ID', "Molecule"], sumcol='Sales')
    
        check_12 = Raw_data.groupby(["Date"]).apply(pudf_cumsum_level_12)
        check_12 = check_12.groupby('ID', "Molecule").pivot('month').agg(func.sum('level')).persist()
        check_12 = colculate_diff(check_12, grouplist=['ID', 'Molecule'])
        WriteCsvOut(check_12, check_12_path)
    
        # 1级头部医院 医院分子 变化倍率检查
        check_12_1 = CheckTopChange(check_12_path, Raw_data, grouplist=['ID', 'Molecule'])
        WriteCsvOut(check_12_1, check_12_1_path)
    # %%
    #========== check_13 数量==========
    
    schema = StructType([
            StructField("ID", StringType(), True),
            StructField("level", IntegerType(), True),
            StructField("month", StringType(), True)
            ])
    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def pudf_cumsum_level_13(pdf):
        return func_pandas_cumsum_level(pdf, grouplist=['ID'], sumcol='Units')
    
    check_13 = Raw_data.groupby(["Date"]).apply(pudf_cumsum_level_13)
    check_13 = check_13.groupby('ID').pivot('month').agg(func.sum('level')).persist()
    check_13 = colculate_diff(check_13, grouplist=['ID'])
    
    WriteCsvOut(check_13, check_13_path)
    # %%
    #========== check_14 数量==========
    if g_id_molecule == 'True':
        schema = StructType([
                StructField("ID", StringType(), True),
                StructField("Molecule", StringType(), True),
                StructField("level", IntegerType(), True),
                StructField("month", StringType(), True)
                ])
        @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
        def pudf_cumsum_level_12(pdf):
            return func_pandas_cumsum_level(pdf, grouplist=['ID', "Molecule"], sumcol='Units')
    
        check_14 = Raw_data.groupby(["Date"]).apply(pudf_cumsum_level_12)
        check_14 = check_14.groupby('ID', "Molecule").pivot('month').agg(func.sum('level')).persist()
        check_14 = colculate_diff(check_14, grouplist=['ID', 'Molecule'])
        
        WriteCsvOut(check_14, check_14_path)
    # %%
    #========== check_15 价格==========
    if g_id_molecule == 'True':
        check_15_a = Raw_data.where(Raw_data.Date > (current_year-1)*100+current_month-1) \
                            .groupBy('ID', 'Date', 'min1') \
                            .agg(func.sum('Sales').alias('Sales'), func.sum('Units').alias('Units')) \
                            .withColumn('Price', col('Sales')/col('Units'))
        check_15_count = check_15_a.groupBy('ID', 'min1').count()
        check_15_sum = check_15_a.groupBy('ID', 'min1').agg(func.sum('Price').alias('sum'), func.max('Price').alias('max'),  
                                                        func.min('Price').alias('min'))
    
        check_15_a = check_15_a.distinct() \
                            .join(check_15_count, on=['ID', 'min1'], how='left') \
                            .join(check_15_sum, on=['ID', 'min1'], how='left')
    
        check_15_a = check_15_a.withColumn('Mean_Price', func.when(col('count') == 1, col('sum')) \
                                                       .otherwise((col('sum') - col('min') - col('max'))/(col('count') - 2 )))
    
        check_15_b = check_15_a.withColumn('Mean_times', func.when(func.abs(col('Price')) > col('Mean_Price'), func.abs(col('Price'))/col('Mean_Price')) \
                                                           .otherwise(col('Mean_Price')/func.abs(col('Price'))))
    
        check_15 = check_15_b.groupby('ID', 'min1').pivot('Date').agg(func.sum('Mean_times')).persist()
    
        check_15 = check_15.withColumn('max', func.greatest(*[i for i in check_15.columns if "20" in i]))
    
        WriteCsvOut(check_15, check_15_path)
    # %%
    #========== check_16 价格==========
    check_16_a = Raw_data.join(province_city_mapping.select('ID','Province').distinct(), on='ID', how='left')
    check_16_b = check_16_a.groupBy('Date', 'min1', 'Province') \
                            .agg(func.sum('Sales').alias('Sales'), func.sum('Units').alias('Units')) \
                            .withColumn('Province_Price', col('Sales')/col('Units'))
    check_16 = check_16_a.groupBy('Date', 'ID', 'min1', 'Province') \
                            .agg(func.sum('Sales').alias('Sales'), func.sum('Units').alias('Units')) \
                            .withColumn('Price', col('Sales')/col('Units')) \
                            .join(check_16_b.select('Date','min1','Province','Province_Price'), on=['Date','Province','min1'], how='left')
    check_16 = check_16.withColumn('gap', col('Price')/col('Province_Price')-1 ) \
                            .withColumn('level', func.ceil(func.abs('gap')*10) ) \
                            .where(col('Date') > current_year*100)
    
    WriteCsvOut(check_16, check_16_path)
    # %%
    #========== 汇总检查结果 ==========
    
    # 汇总检查结果
    check_result = spark.createDataFrame(
        [('产品个数与历史相差不超过0.08', str(check_result_1)), 
        ('缺失产品销售额占比不超过0.02', str(check_result_2)), 
        ('医院个数和历史相差不超过0.01', str(check_result_3)), 
        ('缺失医院销售额占比不超过0.01', str(check_result_5)), 
        ('全部医院的全部产品总个数与最近三个月的均值相差不超过0.03', str(check_result_8))], 
        ('check', 'result'))
    
    WriteCsvOut(check_result, check_result_path)
