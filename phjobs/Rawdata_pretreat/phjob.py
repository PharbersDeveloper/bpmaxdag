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
from pyspark.sql.functions import pandas_udf, PandasUDFType
import time

#def execute(max_path, extract_path, out_path, out_suffix, extract_file, time_left, time_right, molecule, atc, project, doi):
os.environ["PYSPARK_PYTHON"] = "python3"
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
time_left = 202001
time_right = 202008
project_name = 'Gilead'
if_two_source = 'True'
minimum_product_sep = '|'
minimum_product_columns = "Brand, Form, Specifications, Pack_Number, Manufacturer"
minimum_product_columns = minimum_product_columns.replace(" ","").split(",")
# project_name = 'XLT'
# if_two_source = 'False'
max_path = 's3a://ph-max-auto/v0.0.1-2020-06-08/Test/'
outdir = '202008'
history_outdir = 'Empty'
if history_outdir == 'Empty':
    history_outdir = str(int(outdir) - 1)


raw_data_path = max_path + '/' + project_name + '/' + outdir + '/all_raw_data.csv'
molecule_adjust_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/Common_files/新老通用名转换.csv"
history_raw_data_path = max_path + project_name + '/' + history_outdir + '/raw_data'
if if_two_source == 'True':
    history_raw_data_std_path = max_path + project_name + '/' + history_outdir + '/raw_data_std'

std_names = ["Date",
  "ID",
  "Raw_Hosp_Name",
  "Brand",
  "Form",
  "Specifications",
  "Pack_Number",
  "Manufacturer",
  "Molecule",
  "Source",
  "Corp",
  "Route",
  "ORG_Measure"]

 
# 输出
same_sheet_dup_path = max_path + '/' + project_name + '/' + outdir + '/same_sheet_dup.csv'
across_sheet_dup_path = max_path + '/' + project_name + '/' + outdir + '/across_sheet_dup.csv'
all_raw_data_path = max_path + '/' + project_name + '/' + outdir + '/raw_data'
need_clean_path = max_path + '/' + project_name + '/' + outdir + '/need_cleaning_raw.csv'
if if_two_source == 'True':
    all_raw_data_std_path = max_path + '/' + project_name + '/' + outdir + '/raw_data_std'
    cpa_pha_mapping_path = 's3a://ph-max-auto/v0.0.1-2020-06-08/Gilead/cpa_pha_mapping'
    cpa_pha_mapping_common_path = 's3a://ph-max-auto/v0.0.1-2020-06-08/Common_files/cpa_pha_mapping'

# ===========  数据执行 ============
raw_data = spark.read.csv(raw_data_path, header=True)

# 1. 同sheet去重
raw_data = raw_data.groupby(raw_data.columns).count()
same_sheet_dup = raw_data.where(raw_data['count'] > 1)
print("重复条目数:", same_sheet_dup.groupby('Sheet', 'Path').count().show())
print("重复条目占比:", same_sheet_dupraw_data_duplicates.count()/raw_data.count())


# 同sheet重复条目输出	
if same_sheet_dup.count() > 0:
    same_sheet_dup = same_sheet_dup.repartition(1)
    same_sheet_dup.write.format("csv").option("header", "true") \
        .mode("overwrite").save(same_sheet_dup_path)

# group计算
raw_data = raw_data.withColumn('Sales', raw_data['Sales'].cast(DoubleType())) \
                .withColumn('Units', raw_data['Units'].cast(DoubleType())) \
                .withColumn('Units_Box', raw_data['Units_Box'].cast(DoubleType()))
raw_data = raw_data.groupby(std_names + ['Sheet', 'Path']).agg(func.sum(raw_data.Sales).alias('Sales'), 
                                            func.sum(raw_data.Units).alias('Units'), 
                                            func.sum(raw_data.Units_Box).alias('Units_Box'))

# 分子名新旧转换
molecule_adjust = spark.read.csv(molecule_adjust_path, header=True)
molecule_adjust = molecule_adjust.dropDuplicates(["Mole_Old"])

raw_data = raw_data.join(molecule_adjust, raw_data['Molecule'] == molecule_adjust['Mole_Old'], how='left')
raw_data = raw_data.withColumn("S_Molecule", func.when(raw_data.Mole_New.isNull(), raw_data.Molecule).otherwise(raw_data.Mole_New)) \
                    .drop('Mole_Old', 'Mole_New')

raw_data.groupby('Path').count().show()

# 2. 跨sheet去重
'''
保留金额大的
'''
dedup_check = raw_data.groupby('Date', 'S_Molecule', 'Path', 'Sheet', 'Source') \
                        .agg(func.sum(raw_data.Sales).alias('Sales'), func.sum(raw_data.Sales).alias('Units'))
dedup_check = dedup_check.withColumn('rank', func.row_number().over(Window.partitionBy('Date', 'S_Molecule', 'Source').orderBy(dedup_check['Sales'].desc())))
dedup_check = dedup_check.where(dedup_check['rank'] == 1).select('Date', 'S_Molecule', 'Path', 'Sheet','Source')

# 去重后数据
raw_data_dedup = raw_data.join(dedup_check, on=['Date', 'S_Molecule', 'Path', 'Sheet','Source'], how='inner')
# 重复数据
across_sheet_dup = raw_data.join(dedup_check, on=['Date', 'S_Molecule', 'Path', 'Sheet','Source'], how='left_anti')

if across_sheet_dup.count() > 0:
    across_sheet_dup = across_sheet_dup.repartition(1)
    across_sheet_dup.write.format("csv").option("header", "true") \
        .mode("overwrite").save(across_sheet_dup_path)

print(raw_data_dedup.count() + raw_data_dup.count() == raw_data.count())

# 跨源去重
if if_two_source == 'True':
    # drop_dup_hospital
    cpa_pha_mapping = spark.read.parquet(cpa_pha_mapping_path)
    cpa_pha_mapping = cpa_pha_mapping.where(cpa_pha_mapping["推荐版本"] == 1).select('ID', 'PHA')
    
    cpa_pha_mapping_common = spark.read.parquet(cpa_pha_mapping_common_path)
    cpa_pha_mapping_common = cpa_pha_mapping_common.where(cpa_pha_mapping_common["推荐版本"] == 1).select('ID', 'PHA')
    
    raw_data_dedup = raw_data_dedup.withColumn('ID', raw_data_dedup.ID.cast(IntegerType()))
    
    def drop_dup_hospital(df, cpa_pha_map):
        column_names = df.columns
        df = df.join(cpa_pha_map, on='ID', how='left')
        df_filter = df.where(~df.PHA.isNull()).select('Date', 'PHA', 'ID').distinct() \
                                .groupby('Date', 'PHA').count()
        
        df = df.join(df_filter, on=['Date', 'PHA'], how='left')
        # CPA的ID为6位，GYC的ID为7位，跨源去重优先保留CPA
        df = df.where((df['count'] <=1) | ((df['count'] > 1) & (df.ID <= 999999)) | (df['count'].isNull()))
        
        # 检查去重结果
        check = df.select('PHA','ID','Date').distinct() \
                    .groupby('Date', 'PHA').count()
        check_dup = check.where(check['count'] >1 ).where(~check.PHA.isNull())
        if check_dup.count() > 0 :
            print('有未去重的医院')
        df = df.select(column_names)
        return df

    raw_data_dedup_std = drop_dup_hospital(raw_data_dedup, cpa_pha_mapping_common)                  
    raw_data_dedup = drop_dup_hospital(raw_data_dedup, cpa_pha_mapping)    
    

# 数据合并
history_raw_data = spark.read.parquet(history_raw_data_path)
history_raw_data = history_raw_data.withColumn('Date', history_raw_data.Date.cast(IntegerType()))
history_raw_data = history_raw_data.where(history_raw_data.Date < time_left)

raw_data_dedup = raw_data_dedup.withColumn('Date', raw_data_dedup.Date.cast(IntegerType()))
new_raw_data = raw_data_dedup.where(raw_data_dedup.Date >= time_left)
all_raw_data = new_raw_data.select(history_raw_data.columns).union(history_raw_data)

all_raw_data = all_raw_data.repartition(1)
all_raw_data.write.format("csv").option("header", "true") \
    .mode("overwrite").save(all_raw_data_path)
    
if if_two_source == 'True':
    history_raw_data_std = spark.read.parquet(history_raw_data_std_path)
    history_raw_data_std = history_raw_data_std.withColumn('Date', history_raw_data_std.Date.cast(IntegerType()))
    history_raw_data_std = history_raw_data_std.where(history_raw_data_std.Date < time_left)
    
    raw_data_dedup_std = raw_data_dedup_std.withColumn('Date', raw_data_dedup_std.Date.cast(IntegerType()))
    new_raw_data_std = raw_data_dedup_std.where(raw_data_dedup_std.Date >= time_left)
    all_raw_data_std = new_raw_data_std.select(history_raw_data_std.columns).union(history_raw_data_std)
    
    all_raw_data_std = all_raw_data_std.repartition(1)
    all_raw_data_std.write.format("csv").option("header", "true") \
        .mode("overwrite").save(all_raw_data_std_path)
    
    

# 待清洗
clean = raw_data_dedup.select('Brand','Form','Specifications','Pack_Number','Manufacturer','Molecule','Corp','Route','Path','Sheet').distinct()
clean = clean.withColumn('Brand', func.when((clean.Brand.isNull()) | (clean.Brand == 'NA'), clean.Molecule).otherwise(clean.Brand))

clean = clean.withColumn("min1", func.when(clean[minimum_product_columns[0]].isNull(), func.lit("NA")).
                                   otherwise(clean[minimum_product_columns[0]]))
for col in minimum_product_columns[1:]:
    clean = clean.withColumn(col, clean[col].cast(StringType()))
    clean = clean.withColumn("min1", func.concat(
        clean["min1"],
        func.lit(minimum_product_sep),
        func.when(func.isnull(clean[col]), func.lit("NA")).otherwise(clean[col])))


# product_map = spark.read.parquet('s3a://ph-max-auto/v0.0.1-2020-06-08/XLT/202006/prod_mapping')
product_map = spark.read.parquet('s3a://ph-max-auto/v0.0.1-2020-06-08/Gilead/202008/prod_mapping')
product_map = product_map.distinct() \
                    .withColumn("min1", func.regexp_replace("min1", "&amp;", "&")) \
                    .withColumn("min1", func.regexp_replace("min1", "&lt;", "<")) \
                    .withColumn("min1", func.regexp_replace("min1", "&gt;", ">"))
                    
need_clean = clean.join(product_map.select('min1').distinct(), on='min1', how='left_anti')
if need_clean.count() > 0:
    need_clean = need_clean.repartition(1)
    need_clean.write.format("csv").option("header", "true") \
        .mode("overwrite").save(need_clean_path) 

    