# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""
import pandas as pd
from phlogs.phlogs import phlogger
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql import functions as func
from pyspark.sql.functions import pandas_udf, PandasUDFType

import pandas as pd
import numpy as np
from scipy.spatial import distance
import math

#def execute():
spark = SparkSession.builder \
    .master("yarn") \
    .appName("data from s3") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.instance", "1") \
    .config("spark.executor.memory", "3g") \
    .config('spark.sql.codegen.wholeStage', False) \
    .getOrCreate()

access_key = "AKIAWPBDTVEAJ6CCFVCP"
secret_key = "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"
if access_key is not None:
    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
    spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
    # spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
    spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.cn-northwest-1.amazonaws.com.cn")
    

# 输入
data_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/data"
weidao_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/2019年未到名单_v2.csv"
universe_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/universe"
cpa_pha_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/cpa_pha_mapping"

# 输出
data_missing_tmp_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/Out/data_missing_tmp"
data_missing_novbp_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/Out/data_missing_novbp"
data_missing_vbp_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/Out/data_missing_vbp"
df_sales_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/Out/df_sales"
df_units_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/Out/df_units"

# ===============
data = spark.read.parquet(data_path)

weidao = spark.read.csv(weidao_path, header=True)

data = data.join(weidao, on=["ID","Date"], how="left_anti")

universe = spark.read.parquet(universe_path)
cpa_pha = spark.read.parquet(cpa_pha_path)

hosp_info = universe.where(universe["重复"] == "0").select('新版ID', '新版名称', 'Hosp_level', 'Province', 'City')

cpa_pha = cpa_pha.where(cpa_pha["推荐版本"] == "1").select('ID','PHA')

data_pha = data.join(cpa_pha, on='ID', how = 'left')

data_info = data_pha.join(hosp_info, hosp_info['新版ID']==data_pha['PHA'], how='left')
data_info.persist()

data_info = data_info.withColumn("Province" , \
        func.when(data_info.City.isin('大连市','沈阳市','西安市','厦门市','广州市', '深圳市','成都市'), data_info.City). \
        otherwise(data_info.Province))


# =========== 

def get_niches(data, weidao, vbp = False):
    
target=np.where(vbp, 'Units', 'Sales').item()

# 一个ID对应多个data    
weidao = weidao.withColumnRenamed("Date", "Date_weidao") \
        .withColumnRenamed("ID", "ID_weidao")

# 未到id的历史数据：ID 都是在weidao中的，日期小于id 未到日期
data_all = data.join(weidao, data.ID == weidao.ID_weidao, how="inner")
# data_all.select("ID","Date","Sales","ID_weidao","Date_weidao").show()
data_his_hosp = data_all.where(data_all.Date < data_all.Date_weidao) \
            .select('ID', 'pfc', 'Province').distinct()

# 日期在未到中，Province在历史中: 有问题，Province 是要分id的 
data_all_Date = data.join(weidao, data.Date == weidao.Date_weidao, how="inner")

data_same_date = data_all_Date.join(data_his_hosp.select("ID", "Province"), on=["ID", "Province"], how="inner") \
                .select('ID', 'Date', 'pfc', 'VBP_prod').distinct()

data_missing = data_same_date.join(data_his_hosp, how='left', on='pfc')

data_missing = data_missing.where((data_missing.VBP_prod == "True") | (~data_missing.Province.isNull()))

data_missing = data_missing.withColumn(target, func.lit(3.1415926))

df = df.repartition(2)
df.write.format("parquet") \
.mode("overwrite").save("s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/Out/df_tmp")


df = data.select('ID','Date','pfc', target) \
        .union(data_missing.select('ID','Date','pfc', target)) \
        .withColumn("Date", func.concat(func.lit('Date'), data.Date))


# data_info 中 ID|Date|pfc 个别有多条Sales，目前取均值
df = df.groupBy("ID", "pfc").pivot("Date").agg(func.mean(target)).fillna(0)

# df = df.replace(3.1415926, np.nan, inplace=True)
# 将3.1415926替换为null
for eachcol in df.columns:
    df = df.withColumn(eachcol, func.when(df[eachcol] == 3.1415926, None).otherwise(df[eachcol]))


def pandas_udf_get_niches_func(data, weidao, vbp):
    examples=pd.DataFrame()
    target = np.where(vbp, 'Units', 'Sales').item()
    weidao_ID = data["ID"].drop_duplicates().values[0]
    weidao_Date = weidao.loc[weidao.ID == groupID,]
    
    # *** yyw *** 数据类型处理
    data["Date"] = data["Date"].astype("int")
    
    # todo: 不行啊，data会用到全量，未到ID的时间，取全量该时间的data
    for i in range(len(weidao_Date)):
        example=weidao_Date.loc[i,]
        
        data_his_hosp = data.loc[(data.ID == example.ID) &
                                 (data.Date <= int(example.Date)),
                                 ['ID', 'pfc', 'Province']].drop_duplicates()
        data_same_date = data.loc[(data.Date == int(example.Date)) &
                                  (data.Province.isin(data_his_hosp.Province)),
                                  ['pfc', 'VBP_prod']].drop_duplicates()
        data_missing = data_same_date.merge(data_his_hosp, how='left',
                                            on='pfc')
        data_missing = data_missing[(data_missing.VBP_prod == True) |
                                    (~data_missing.Province.isna())]
        data_missing.ID = example.ID
        data_missing.insert(1, 'Date', example.Date)
        data_missing.insert(len(data_missing.columns), target, 3.1415926)
        examples=pd.concat([examples, data_missing])
        
    examples = examples.reset_index(drop=True)
    df = pd.concat([data[['ID', 'Date', 'pfc', target]],
                    examples[['ID', 'Date', 'pfc', target]]])
    df.Date = 'Date' + df.Date.astype('str')
    #df = df.pivot_table(index=['ID', 'pfc'],
    #                    columns='Date', values=target).fillna(0).reset_index()
    #df.replace(3.1415926, np.nan, inplace=True)
    return df	

#get_niches
schema= StructType([
    StructField("ID", IntegerType(), True),
    StructField("Date", StringType(), True),
    StructField("pfc", IntegerType(), True),
    StructField("Sales", DoubleType(), True)
    ])
@pandas_udf(schema, PandasUDFType.GROUPED_MAP)    
def pandas_udf_get_niches(df):
    return pandas_udf_get_niches_func(df, weidao, vbp=False)    

# 未到ID的数据
data_info_weidao = data_info.join(weidao.select("ID"), on="ID", how="inner")
df_sales = data_info_weidao.groupby(["ID"]).apply(pandas_udf_get_niches)


