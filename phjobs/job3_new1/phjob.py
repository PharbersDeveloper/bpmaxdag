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

import pandas as pd
import numpy as np
from scipy.spatial import distance
import math

def execute(max_path, project_name, out_path, out_dir):
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
        
    
    out_path_dir = out_path + "/" + project_name + '/' + out_dir
    
    # 输入
    data_path = out_path + "/" + project_name + '/data'
    weidao_path = out_path + "/" + project_name + u'/2019年未到名单_v2.csv'
    universe_path = out_path + "/" + project_name + '/universe'
    cpa_pha_path = out_path + "/" + project_name + '/cpa_pha_mapping'

    '''
    data_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/data"
    weidao_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/2019年未到名单_v2.csv"
    universe_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/universe"
    cpa_pha_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/cpa_pha_mapping"
    '''
    
    # 输出
    data_missing_tmp_path = out_path_dir + "/data_missing_tmp"
    data_missing_novbp_path = out_path_dir + "/data_missing_novbp"   
    data_missing_vbp_path = out_path_dir + "/data_missing_vbp"
    df_sales_path = out_path_dir + "/df_sales"
    df_units_path = out_path_dir + "/df_units"
    
    '''
    data_missing_tmp_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/Out/data_missing_tmp"
    data_missing_novbp_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/Out/data_missing_novbp"
    data_missing_vbp_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/Out/data_missing_vbp"
    df_sales_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/Out/df_sales"
    df_units_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/Out/df_units"
    '''
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
    
    def get_niches(data, weidao, vbp = False):
        
        data.persist()
        weidao = weidao.toPandas()
        
        target=np.where(vbp, 'Units', 'Sales').item()
        
        empty = 0
        for i in range(len(weidao)):
            print i
            example = weidao.loc[i,]
            
            data_his_hosp = data.where((data.ID == example.ID) & (data.Date <= example.Date)) \
                    .select('ID','pfc','Province').distinct()
            data_his_hosp.persist()        
            # data_his_hosp_Province = data_his_hosp.select("Province").distinct().toPandas()["Province"].tolist()
            
            # data_same_date = data.where((data.Date==example.Date) & (data.Province.isin(data_his_hosp_Province))) \
            #        .select('pfc','VBP_prod').distinct()
            data_same_date = data.where(data.Date==example.Date) \
                        .join(data_his_hosp.select("Province"), on="Province", how="inner") \
                        .select('pfc','VBP_prod').distinct()
                    
            data_missing = data_same_date.join(data_his_hosp, how='left', on='pfc')
            data_missing.persist()
            
            data_missing = data_missing.where((data_missing.VBP_prod == True) | (~data_missing.Province.isNull())) \
                    .withColumn("ID", func.lit(example.ID)) \
                    .withColumn("Date", func.lit(example.Date)) \
                    .withColumn(target, func.lit(3.1415926))
            
            # 写出每个循环的data_missing结果
            if empty == 0:
                data_missing = data_missing.repartition(1)
                data_missing.write.format("parquet") \
                    .mode("overwrite").save(data_missing_tmp_path)
            else:
                data_missing = data_missing.repartition(1)
                data_missing.write.format("parquet") \
                    .mode("append").save(data_missing_tmp_path)
            empty = empty + 1
        
        # 读取data_missing：examples
        examples = spark.read.parquet(data_missing_tmp_path)
        # 重新分区写出
        examples = examples.repartition(2)
        if vbp == False:
            examples.write.format("parquet") \
                .mode("overwrite").save(data_missing_novbp_path)
        else:
            examples.write.format("parquet") \
                .mode("overwrite").save(data_missing_vbp_path)
            
        
        df = data.select('ID','Date','pfc', target).union(examples.select('ID','Date','pfc', target))
        
        df = df.withColumn("Date", func.concat(func.lit("Date"), df.Date))
        
        # data_info 中 ID|Date|pfc 个别有多条Sales，目前取均值
        df = df.groupBy("ID", "pfc").pivot("Date").agg(func.mean(target)).fillna(0)
        
        # df = df.replace(3.1415926, np.nan, inplace=True)
        # 将3.1415926替换为null
        for eachcol in df.columns:
            df = df.withColumn(eachcol, func.when(df[eachcol] == 3.1415926, None).otherwise(df[eachcol]))
        
        return df            

    
    df_sales = get_niches(data_info, weidao, vbp = False)
    df_sales = df_sales.repartition(2)
    df_sales.write.format("parquet") \
    .mode("overwrite").save(df_sales_path)
    
    
    df_units = get_niches(data_info, weidao, vbp = True)
    df_units = df_units.repartition(2)
    df_units.write.format("parquet") \
    .mode("overwrite").save(df_units_path)

    

# 数据上传
'''
data_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/data"
data = spark.read.csv("s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/HTN_training_set_lu_v2.csv", header="True")
data = data.repartition(2)
data.write.format("parquet") \
    .mode("overwrite").save(data_path)
    
data_complete_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/data_complete"
data_complete = spark.read.csv(u"s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/HTN_training_set_RAW_complete_去重版.csv", header="True")
data_complete = data_complete.repartition(2)
data_complete.write.format("parquet") \
    .mode("overwrite").save(data_complete_path)

# weidao = spark.read.csv("s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/2019年未到名单_v2.csv", header="True")

universe_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/universe"
universe = spark.read.csv("s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/2020年Universe更新维护_20200717.csv", header="True")
universe = universe.repartition(2)
universe.write.format("parquet") \
    .mode("overwrite").save(universe_path)
    
cpa_pha_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/cpa_pha_mapping"
cpa_pha = spark.read.csv("s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/医院匹配_20191031.csv", header="True")
cpa_pha = cpa_pha.repartition(1)
cpa_pha.write.format("parquet") \
    .mode("overwrite").save(cpa_pha_path)
    

ProdMapPath = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/prod_mapping"
prod_map = spark.read.csv(u"s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/产品匹配表_Pfizer_20200603.csv", header="True")
prod_map = prod_map.repartition(2)
prod_map.write.format("parquet") \
    .mode("overwrite").save(ProdMapPath)

prod_ref_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/cn_prod_ref"
prod_ref = spark.read.csv("s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/cn_prod_ref_201912_1.csv", header="True")
prod_ref = prod_ref.repartition(2)
prod_ref.write.format("parquet") \
    .mode("overwrite").save(prod_ref_path)    

mnf_ref_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/cn_mnf_ref"
mnf_ref = spark.read.csv("s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/cn_mnf_ref_201912_1.csv", header="True")
mnf_ref = mnf_ref.repartition(2)
mnf_ref.write.format("parquet") \
    .mode("overwrite").save(mnf_ref_path)
    
data_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/Zhongbiao"
data = spark.read.csv("s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/中标产品清单.csv", header="True")
data = data.repartition(2)
data.write.format("parquet") \
    .mode("overwrite").save(data_path)
    
data_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/MNF_TYPE_PFC"
data = spark.read.csv("s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/MNF_TYPE_PFC.csv", header="True")
data = data.repartition(2)
data.write.format("parquet") \
    .mode("overwrite").save(data_path)
'''  