# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""
def execute(a, b):
import pandas as pd
from phlogs.phlogs import phlogger
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql import functions as func

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

def get_niches(data, weidao, vbp=False):
    examples = pd.DataFrame()
    target = np.where(vbp, 'Units', 'Sales').item()

    for i in range(len(weidao)):
        example = weidao.loc[i,]

        data_his_hosp = data.loc[(data.ID == example.ID) &
                                 (data.Date <= example.Date),
                                 ['ID', 'pfc', 'Province']].drop_duplicates()

        data_same_date = data.loc[(data.Date == example.Date) &
                                  (data.Province.isin(data_his_hosp.Province)),
                                  ['pfc', 'VBP_prod']].drop_duplicates()

        data_missing = data_same_date.merge(data_his_hosp, how='left',
                                            on='pfc')

        data_missing = data_missing[(data_missing.VBP_prod == True) |
                                    (~data_missing.Province.isna())]

        data_missing.ID = example.ID

        data_missing.insert(1, 'Date', example.Date)
        data_missing.insert(len(data_missing.columns), target, 3.1415926)

        examples = pd.concat([examples, data_missing])

    examples = examples.reset_index(drop=True)
    df = pd.concat([data[['ID', 'Date', 'pfc', target]],
                    examples[['ID', 'Date', 'pfc', target]]])

    df.Date = 'Date' + df.Date.astype('str')

    df = df.pivot_table(index=['ID', 'pfc'],
                        columns='Date', values=target).fillna(0).reset_index()

    df.replace(3.1415926, np.nan, inplace=True)

    return df

df_sales = get_niches(data_info, weidao, vbp=False)