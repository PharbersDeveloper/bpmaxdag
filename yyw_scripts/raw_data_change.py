from ph_logs.ph_logs import phlogger
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql import functions as func
import os
from pyspark.sql.functions import pandas_udf, PandasUDFType
import time
import pandas as pd

os.environ["PYSPARK_PYTHON"] = "python3"
spark = SparkSession.builder \
    .master("yarn") \
    .appName("data from s3") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.instance", "1") \
    .config("spark.executor.memory", "2g") \
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

'''
手动修改raw_data 错误数据
'''    
    
# 需要修改的参数
project_name = 'Bayer'
outdir = '202010'
if_two_source = 'False'
# 在c9上新建一个文件，将‘问题医院记录表’本项目要修改的条目复制粘贴（带着标题），保存的时候后缀写.csv即可
change_file_path = '/home/ywyuan/BP_Max_AutoJob/yyw_scripts//raw_data_change.csv'

max_path = 's3a://ph-max-auto/v0.0.1-2020-06-08/'


# =========  数据处理  =============
# 1. 需要修改的条目文件处理
change_file = pd.read_csv(change_file_path, sep='\t', header=0, dtype="object")
change_file[['Date']] = change_file[['Date']].astype(int)
change_file['Hospital_ID'] = change_file['Hospital_ID'].str.rjust(6,'0')
change_file = change_file.fillna({"Form":"NA","Specifications":"NA","Pack_Number":"NA","Manufacturer":"NA"})

# 产品层面
change_file_1 = change_file[change_file['错误类型'] == '产品层面']
change_file_1[['Pack_Number']] = change_file_1[['Pack_Number']].astype(int)
change_file_1[['Brand','Pack_Number', 'Date', 'Hospital_ID']] = change_file_1[['Brand','Pack_Number', 'Date', 'Hospital_ID']].astype(str)
change_file_1['all_info'] = (change_file_1['Molecule'] + change_file_1['Brand'] + change_file_1['Form'] + 
                                change_file_1['Specifications'] + change_file_1['Pack_Number'] + 
                                change_file_1['Manufacturer'] + change_file_1['Date'] + change_file_1['Hospital_ID']).str.strip()
change_file_1_spark = spark.createDataFrame(change_file_1)
change_file_1_spark = change_file_1_spark.select('all_info', 'Sales_old', 'Sales_new', 'Units_old', 'Units_new')

# 医院层面
change_file_2 = change_file[change_file['错误类型'] == '医院层面']
if len(change_file_2) >0:
    change_file_2[['医院层面替换月份']] = change_file_2[['医院层面替换月份']].astype(int)
    change_file_2 = change_file_2[['Date', 'Hospital_ID', '医院层面替换月份']]
    change_file_2_spark = spark.createDataFrame(change_file_2)
    change_file_2_spark = change_file_2_spark.withColumnRenamed('Hospital_ID', 'ID')
    change_file_2_spark.show()

# 2. 对raw_data修改并备份
def change_raw(raw_data_old_path, raw_data_new_path):
    raw_data_old = spark.read.parquet(raw_data_old_path)
    
    # a. 产品层面
    raw_data_old = raw_data_old.withColumn('Brand_new', func.when(raw_data_old.Brand.isNull(), func.lit('nan')).otherwise(raw_data_old.Brand))
    raw_data_old = raw_data_old.withColumn('all_info', func.concat(func.col('Molecule'), func.col('Brand_new'), func.col('Form'), func.col('Specifications'),
                             func.col('Pack_Number'), func.col('Manufacturer'), func.col('Date'), func.col('ID')))
    raw_data_new = raw_data_old.join(change_file_1_spark, on='all_info', how='left')
    
    # 检查替换的信息是否正确
    print("产品层面替换的条目：", change_file_1_spark.count())
    print("匹配到的替换的条目：", raw_data_new.where(~raw_data_new.Sales_old.isNull()).count())
    
    raw_data_new.where(~raw_data_new.Sales_old.isNull()) \
            .select('all_info', 'Sales', 'Sales_old', 'Sales_new', 'Units', 'Units_old', 'Units_new').show()
    
    # 替换数据
    raw_data_new = raw_data_new.withColumn('Sales', func.when(~raw_data_new.Sales_old.isNull(), raw_data_new.Sales_new) \
                                                        .otherwise(raw_data_new.Sales)) \
                            .withColumn('Units', func.when(~raw_data_new.Units_old.isNull(), raw_data_new.Units_new) \
                                                        .otherwise(raw_data_new.Units))
    
    raw_data_new.where(~raw_data_new.Sales_old.isNull()) \
            .select('all_info', 'Sales', 'Sales_old', 'Sales_new', 'Units', 'Units_old', 'Units_new').show()
            
    raw_data_new = raw_data_new.drop('all_info', 'Sales_old', 'Sales_new', 'Units_old', 'Units_new', 'Brand_new')
    
    print('修改前后raw_data行数是否一致：', (raw_data_new.count() == raw_data_old.count()))
    
    # b. 医院层面
    if len(change_file_2) >0:
        raw_data_new_drop = raw_data_old.join(change_file_2_spark, on= ['Date', 'ID'], how='left_anti')
        raw_data_new_replace = raw_data_old.join(change_file_2_spark.withColumnRenamed('Date', 'Date_raw').withColumnRenamed('医院层面替换月份', 'Date'), 
                                                on=['Date', 'ID'] ,how='inner')
        raw_data_new_replace = raw_data_new_replace.withColumn('Date', raw_data_new_replace['Date_raw']).drop('Date_raw')
        
        raw_data_new = raw_data_new_drop.union(raw_data_new_replace)
    
    # 输出结果
    raw_data_new = raw_data_new.repartition(2)
    raw_data_new.write.format("parquet") \
        .mode("overwrite").save(raw_data_new_path)
    
    raw_data_new = spark.read.parquet(raw_data_new_path)
        
    raw_data_new = raw_data_new.repartition(2)
    raw_data_new.write.format("parquet") \
        .mode("overwrite").save(raw_data_old_path)
        

# ===============

# 执行函数
raw_data_old_path = max_path + '/' + project_name + '/' + outdir + '/raw_data/'
raw_data_new_path = max_path + '/' + project_name + '/' + outdir + '/raw_data_bk/'
change_raw(raw_data_old_path, raw_data_new_path)


if if_two_source == 'True':
    raw_data_old_path = max_path + '/' + project_name + '/' + outdir + '/raw_data_std/'
    raw_data_new_path = max_path + '/' + project_name + '/' + outdir + '/raw_data_std_bk/'
    change_raw(raw_data_old_path, raw_data_new_path)

