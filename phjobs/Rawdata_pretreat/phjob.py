# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""

from ph_logs.ph_logs import phlogger
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql import functions as func
import os
from pyspark.sql.functions import pandas_udf, PandasUDFType, udf
import time

def execute(max_path, project_name, outdir, history_outdir, if_two_source, cut_time_left, cut_time_right, 
raw_data_path, if_union, test, auto_max):
    os.environ["PYSPARK_PYTHON"] = "python3"
    
    if auto_max != "False" and auto_max != "True":
        phlogger.error('wrong input: auto_max, False or True') 
        raise ValueError('wrong input: auto_max, False or True')
        
    if auto_max == "False":
        raise ValueError('auto_max: False 非自动化')
        
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("data from s3") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.instance", "1") \
        .config("spark.executor.memory", "1g") \
        .config('spark.sql.codegen.wholeStage', False) \
        .getOrCreate()
    
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    if access_key is not None:
        spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
        spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
        spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
        spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
        # spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
        spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.cn-northwest-1.amazonaws.com.cn")
        
        
    # 输入
    if if_two_source != "False" and if_two_source != "True":
        phlogger.error('wrong input: if_two_source, False or True') 
        raise ValueError('wrong input: if_two_source, False or True')
        
    cut_time_left = int(cut_time_left)
    cut_time_right = int(cut_time_right)
    
    molecule_adjust_path = max_path + "/Common_files/新老通用名转换.csv"
    
    if raw_data_path == 'Empty':
        raw_data_path = max_path + '/' + project_name + '/' + outdir + '/raw_data.csv'
    
    if history_outdir == 'Empty':
        history_outdir = str(int(outdir) - 1)
        
    history_raw_data_path = max_path + '/' + project_name + '/' + history_outdir + '/raw_data'
    if if_two_source == 'True':
        history_raw_data_std_path = max_path + '/' + project_name + '/' + history_outdir + '/raw_data_std'
        cpa_pha_mapping_path = max_path + '/' + project_name + '/cpa_pha_mapping'
        cpa_pha_mapping_common_path = max_path + '/Common_files/cpa_pha_mapping'
    
    std_names = ["Date", "ID", "Raw_Hosp_Name", "Brand", "Form", "Specifications", "Pack_Number", "Manufacturer", 
    "Molecule", "Source", "Corp", "Route", "ORG_Measure"]
    
    if project_name == 'Mylan':
        std_names = ["Date", "ID", "Raw_Hosp_Name", "Brand", "Form", "Specifications", "Pack_Number", "Manufacturer", 
        "Molecule", "Source", "Corp", "Route", "ORG_Measure", "min1", "Pack_ID"]
        
    # 输出
    same_sheet_dup_path = max_path + '/' + project_name + '/' + outdir + '/raw_data_check/same_sheet_dup.csv'
    across_sheet_dup_path = max_path + '/' + project_name + '/' + outdir + '/raw_data_check/across_sheet_dup.csv'
    across_sheet_dup_bymonth_path = max_path + '/' + project_name + '/' + outdir + '/raw_data_check/across_sheet_dup_bymonth.csv'
    
    if test != "False" and test != "True":
        phlogger.error('wrong input: test, False or True') 
        raise ValueError('wrong input: test, False or True')
    
    if test == 'True':
        all_raw_data_path = max_path + '/' + project_name + '/' + outdir + '/raw_data_check/raw_data'
        if if_two_source == 'True':
            all_raw_data_std_path = max_path + '/' + project_name + '/' + outdir + '/raw_data_check/raw_data_std'
    else:
        all_raw_data_path = max_path + '/' + project_name + '/' + outdir + '/raw_data'
        if if_two_source == 'True':
            all_raw_data_std_path = max_path + '/' + project_name + '/' + outdir + '/raw_data_std'
    
    
    # =============  数据执行 ==============
    raw_data = spark.read.csv(raw_data_path, header=True)
    if 'Corp' not in raw_data.columns:
        raw_data = raw_data.withColumn('Corp', func.lit(''))
    if 'Route' not in raw_data.columns:
        raw_data = raw_data.withColumn('Route', func.lit(''))
    
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
    
    
    # 2. 跨sheet去重
    # 2.1 path来自不同月份文件夹：'Date, S_Molecule' 优先最大月份文件夹来源的数据，生成去重后结果 raw_data_dedup_bymonth
    
    # 获取path的日期文件夹名, 纯数字的为文件夹名，只要包含字符就不是月份名
    @udf(StringType())
    def path_split(path):
        path_month = path.replace('//', '/').split('/')
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
      
    # 3. 跨源去重，跨源去重优先保留CPA医院
    if if_two_source == 'True':
        # drop_dup_hospital
        cpa_pha_mapping = spark.read.parquet(cpa_pha_mapping_path)
        cpa_pha_mapping = cpa_pha_mapping.where(cpa_pha_mapping["推荐版本"] == 1).select('ID', 'PHA')
        cpa_pha_mapping = deal_ID_length(cpa_pha_mapping)
        
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
            check = df_all.select('PHA','ID','Date').distinct() \
                        .groupby('Date', 'PHA').count()
            check_dup = check.where(check['count'] >1 ).where(~check.PHA.isNull())
            if check_dup.count() > 0 :
                print('有未去重的医院')
                
            df_all = df_all.select(column_names)
            return df_all
    
        raw_data_dedup_std = drop_dup_hospital(raw_data_dedup, cpa_pha_mapping_common)                  
        raw_data_dedup = drop_dup_hospital(raw_data_dedup, cpa_pha_mapping)    
        
    
    # 4. 与历史数据合并
    if if_union == 'True':
        history_raw_data = spark.read.parquet(history_raw_data_path)
        if 'Corp' not in history_raw_data.columns:
            history_raw_data = history_raw_data.withColumn('Corp', func.lit(''))
        if 'Route' not in history_raw_data.columns:
            history_raw_data = history_raw_data.withColumn('Route', func.lit(''))
            
        history_raw_data = history_raw_data.withColumn('Corp', history_raw_data.Corp.cast(StringType()))
        history_raw_data = history_raw_data.withColumn('Date', history_raw_data.Date.cast(IntegerType()))
        history_raw_data = history_raw_data.where(history_raw_data.Date < cut_time_left)
        history_raw_data = history_raw_data.drop("Brand_new", "all_info")
        
        raw_data_dedup = raw_data_dedup.withColumn('Date', raw_data_dedup.Date.cast(IntegerType()))
        new_raw_data = raw_data_dedup.where((raw_data_dedup.Date >= cut_time_left) & (raw_data_dedup.Date <= cut_time_right))
        all_raw_data = new_raw_data.select(history_raw_data.columns).union(history_raw_data)
        
        all_raw_data = deal_ID_length(all_raw_data)
        
        all_raw_data = all_raw_data.repartition(2)
        all_raw_data.write.format("parquet") \
            .mode("overwrite").save(all_raw_data_path)
            
        if if_two_source == 'True':
            history_raw_data_std = spark.read.parquet(history_raw_data_std_path)
            if 'Corp' in history_raw_data_std.columns:
                history_raw_data_std = history_raw_data_std.withColumn('Corp', history_raw_data_std.Corp.cast(StringType()))
            history_raw_data_std = history_raw_data_std.withColumn('Date', history_raw_data_std.Date.cast(IntegerType()))
            history_raw_data_std = history_raw_data_std.where(history_raw_data_std.Date < cut_time_left)
            history_raw_data_std = history_raw_data_std.drop("Brand_new", "all_info")
            
            raw_data_dedup_std = raw_data_dedup_std.withColumn('Date', raw_data_dedup_std.Date.cast(IntegerType()))
            new_raw_data_std = raw_data_dedup_std.where((raw_data_dedup_std.Date >= cut_time_left) & (raw_data_dedup_std.Date <= cut_time_right))
            all_raw_data_std = new_raw_data_std.select(history_raw_data_std.columns).union(history_raw_data_std)
            
            all_raw_data_std = deal_ID_length(all_raw_data_std)
            
            all_raw_data_std = all_raw_data_std.repartition(2)
            all_raw_data_std.write.format("parquet") \
                .mode("overwrite").save(all_raw_data_std_path)
    else:
        raw_data_dedup = deal_ID_length(raw_data_dedup)
        
        raw_data_dedup = raw_data_dedup.repartition(2)
        raw_data_dedup.write.format("parquet") \
            .mode("overwrite").save(all_raw_data_path)
            
        if if_two_source == 'True':
            raw_data_dedup_std = deal_ID_length(raw_data_dedup_std)
            
            raw_data_dedup_std = raw_data_dedup_std.repartition(2)
            raw_data_dedup_std.write.format("parquet") \
                .mode("overwrite").save(all_raw_data_std_path)
        
        
    
