# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""
import pandas as pd
from ph_logs.ph_logs import phlogger
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

def execute(max_path, project_name, out_path, out_dir):
    os.environ["PYSPARK_PYTHON"] = "python3"
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("data from s3") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.instance", "1") \
        .config("spark.executor.memory", "3g") \
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
    
    # =============== 数据执行 =================
    
    data = spark.read.parquet(data_path)
    
    weidao = spark.read.csv(weidao_path, header=True)
    
    data = data.join(weidao, on=["ID","Date"], how="left_anti")
    
    universe = spark.read.parquet(universe_path)
    cpa_pha = spark.read.parquet(cpa_pha_path)
    
    hosp_info = universe.where(universe["重复"] == "0").select('新版ID', '新版名称', 'Hosp_level', 'Province', 'City')
    
    cpa_pha = cpa_pha.where(cpa_pha["推荐版本"] == "1").select('ID','PHA')
    
    data_pha = data.join(cpa_pha, on='ID', how = 'left')
    
    data_info = data_pha.join(hosp_info, hosp_info['新版ID']==data_pha['PHA'], how='left')
    
    data_info = data_info.withColumn("Province" , \
            func.when(data_info.City.isin('大连市','沈阳市','西安市','厦门市','广州市', '深圳市','成都市'), data_info.City). \
            otherwise(data_info.Province))
    
    # get_niches 函数进行了调整，算法替换掉for循环，加快运行效率
    def get_niches(data, weidao, vbp):    
        target=np.where(vbp, 'Units', 'Sales').item()
        
        # 一个weidaoID有多个data    
        weidao = weidao.withColumnRenamed("Date", "Date_weidao") \
                .withColumnRenamed("ID", "ID_weidao")
        
        # 未到id的历史数据：ID 在weidao中，日期小于weidao日期，得到每个weidao的历史医院
        data_all = data.join(weidao, data.ID == weidao.ID_weidao, how="inner")
        data_his_hosp = data_all.where(data_all.Date < data_all.Date_weidao) \
                    .withColumnRenamed("Province", "Province_his") \
                    .select('ID_weidao','Date_weidao' ,'pfc', 'Province_his') \
                    .distinct()
        
        # 日期在weidao中，Province在历史中
        data_all_Date = data.join(data_his_hosp.select('ID_weidao','Date_weidao','Province_his'),
                            data.Date == data_his_hosp.Date_weidao, how="inner") \
                            .select("ID_weidao", "Date_weidao", "Province", "Province_his", 'VBP_prod', 'pfc') \
                            .distinct()
        data_same_date = data_all_Date.where(data_all_Date.Province == data_all_Date.Province_his) \
                        .select("ID_weidao", "Date_weidao", 'VBP_prod', 'pfc').distinct()
        
        # data_same_date 和 data_his_hosp 合并，获得data_missing
        data_missing = data_same_date.join(data_his_hosp, on=['ID_weidao', 'Date_weidao', 'pfc'], how='left') \
                                    .distinct() \
                                    .withColumnRenamed("Province_his", "Province") \
                                    .withColumnRenamed("ID_weidao", "ID") \
                                    .withColumnRenamed("Date_weidao", "Date")
                                    
        data_missing = data_missing.where((data_missing.VBP_prod == "True") | (~data_missing.Province.isNull()))
        data_missing = data_missing.withColumn(target, func.lit(3.1415926))
        
        # 合并 data 和 data_missing
        df = data.select('ID','Date','pfc', target) \
                .union(data_missing.select('ID','Date','pfc', target)) \
                .withColumn("Date", func.concat(func.lit('Date'), data.Date))
        
        # data_info 中 ID|Date|pfc 个别有多条Sales，目前算法取均值
        df = df.groupBy("ID", "pfc").pivot("Date").agg(func.mean(target)).fillna(0)
        
        # df = df.replace(3.1415926, np.nan, inplace=True)
        # 将3.1415926替换为null
        for eachcol in df.columns:
            df = df.withColumn(eachcol, func.when(df[eachcol] == 3.1415926, None).otherwise(df[eachcol]))
            
        return df
    
    # 函数执行 
    df_sales = get_niches(data_info, weidao, vbp = False)
    
    df_sales = df_sales.repartition(2)
    df_sales.write.format("parquet") \
    .mode("overwrite").save(df_sales_path)
    
    df_units = get_niches(data_info, weidao, vbp = True)
    
    df_units = df_units.repartition(2)
    df_units.write.format("parquet") \
    .mode("overwrite").save(df_units_path)




