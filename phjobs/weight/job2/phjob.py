# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from ph_logs.ph_logs import phs3logger
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql import functions as func
import os
from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col
import time
import re
import pandas as pd
import numpy as np
import json
from copy import deepcopy
import math


def execute(**kwargs):
    """
        please input your code below
        get spark session: spark = kwargs["spark"]()
    """
    logger = phs3logger(kwargs["job_id"])
    logger.info("当前 owner 为 " + str(kwargs["owner"]))
    logger.info("当前 run_id 为 " + str(kwargs["run_id"]))
    logger.info("当前 job_id 为 " + str(kwargs["job_id"]))
    spark = kwargs["spark"]()
    #logger.info(kwargs["a"])
    #logger.info(kwargs["b"])
    logger.info(kwargs["c"])
    logger.info(kwargs["d"])
    
    '''
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

    max_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/"
    #project_name = "Eisai"
    #market_city_brand = '{"固力康":{"福厦泉":4}}'
    project_name = "Merck"
    market_city_brand = '{"思则凯":{"福厦泉":4}}'
    lmda = '0.001'
    learning_rate = '100'
    max_iteration = '10000'
    gradient_type = 'both'  # 可选'both', 'gr','share'
    '''
    # 输入
    max_path = kwargs["max_path"]
    project_name = kwargs["project_name"]
    market_city_brand = kwargs["market_city_brand"]
    lmda = kwargs["lmda"]
    learning_rate = kwargs["learning_rate"]
    max_iteration = kwargs["max_iteration"]
    gradient_type = kwargs["gradient_type"]
    
    lmda = float(lmda)
    learning_rate = int(learning_rate)
    max_iteration = int(max_iteration)
    market_city_brand = json.loads(market_city_brand)
    universe_path = max_path + '/' + project_name + '/universe_base'
    
    # 输出
    weight_path = max_path + '/' + project_name + '/weight/PHA_weight'
    
    # ==========  数据执行  ============
    
    # ====  一. 数据准备  ==== 
    # 1. universe 文件                  
    universe = spark.read.parquet(universe_path)
    universe = universe.select("Province", "City") \
                        .distinct()
    
    
    # ====  二. 函数定义  ====
    
    # 1. 利用 ims_sales_gr，生成城市 top 产品的 'gr','share','share_ly' 字典
    def func_target_brand(pdf, city_brand_dict):
        import json
        city_name = pdf['City'][0]
        brand_number = city_brand_dict[city_name]
        pdf = pdf.sort_values(by='share', ascending=False).reset_index()[0:int(brand_number)]
        dict_share = pdf.groupby(['City'])['标准商品名','gr','share','share_ly'].apply(lambda x : x.set_index('标准商品名').to_dict()).to_dict()
        dict_share = json.dumps(dict_share)
        return pd.DataFrame([[city_name] + [dict_share]], columns=['City', 'dict'])
    
    schema= StructType([
            StructField("City", StringType(), True),
            StructField("dict", StringType(), True)
            ])
    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def udf_target_brand(pdf):
        return func_target_brand(pdf, city_brand_dict)
    
    # 2. 算法函数 辅助函数
    # 计算增长率或者份额
    def r(W, h_n, h_d):
        return h_n.T.dot(W)/h_d.T.dot(W)
    
    # 计算和IMS的差距
    def delta_gr(W, h_n, h_d, g):
        return (r(W, h_n, h_d) - g)
    
    # 控制W离散
    def w_discrete_ctrl(W, lmda):
        return 2*lmda*(W-np.average(W))
    
    # Loss-func求导
    '''
    loss function是差距（份额或增长）的平方
    '''
    def gradient(W, h_n, h_d, g, lmda):
        CrossDiff = (h_d.sum() - W.reshape(-1)*h_d) * h_n - \
                  (h_n.sum() - W.reshape(-1)*h_n) * h_d    
        dW = delta_gr(W, h_n, h_d, g) * np.power((h_d.T.dot(W)), -2) * CrossDiff \
          + w_discrete_ctrl(W, lmda).reshape(-1, )
        return dW.reshape(-1,1)
    
    # 梯度下降
    '''
    这是一个双任务优化求解，目标是寻找满足IMS Share and Growth的医院参数线性组合 ～ w
    可能的待优化方向：
        1. 自适应学习率
        2. 不同任务重要性，不同收敛速度，不同学习率
        3. 避免落在局部最优
        4. 初始值的选择
    '''
    
    def gradient_descent(W_0, S, G, S_ly, learning_rate, max_iteration, brand_number, H_n, H_share, H_gr, H_ly, lmda, gradient_type):
        X = np.array([]); Growth = np.array([]); Share = np.array([])
        W = W_0.copy()
        W_init = W_0.copy()
        w_gd = np.array([0]*H_n.shape[1])
        for i in range(max_iteration):
            gradient_sum = np.zeros((len(H_n),1))
            for k in range(H_n.shape[1]):
                gd_share = gradient(W, H_n[:,k], H_share, S[k], lmda)
                gd_growth = gradient(W, H_n[:,k], H_gr[:,k], G[k], lmda)
                if gradient_type == 'both':
                    gradient_sum += gd_growth + gd_share
                elif gradient_type == 'share':
                    # 只优化Share
                    gradient_sum += gd_share
                # 优化Share和增长
                elif gradient_type == 'gr':
                    gradient_sum += gd_growth
                    
            gradient_sum[(W_init==0)|(W_init==1)] = 0
            W -= learning_rate * gradient_sum
            W[W<0] = 0
            print ('iteration : ', i, '\n GR : ', r(W, H_n, H_gr),
                '\n Share : ', r(W, H_n, H_share)) 
            X = np.append(X, [i])
            Growth = np.append(Growth, r(W, H_n, H_gr))
            Share = np.append(Share, r(W, H_n, H_share))
            share_ly_hat = W.T.dot(H_gr).reshape(-1)/W.T.dot(H_ly)
            w_gd = abs(share_ly_hat/S_ly -1)
        return W, X, Growth.reshape((i+1,-1)), Share.reshape((i+1,-1))
    
    # 3. pandas_udf 执行算法，分城市进行优化
    def func_target_weight(pdf, dict_target_share, l, m, lmda, gradient_type):
        city_name = pdf['City'][0]
        target = list(dict_target_share[city_name]['gr'].keys())
        brand_number = len(target)
        
        H = pdf.pivot_table(index=['City','PHA','City_Sample','weight', 'Bedsize>99'], columns=['tmp'], 
                            values='Sales', fill_value=0, aggfunc='sum')
        H = H.reset_index()
        
        H['2018_total'] = H.loc[:,H.columns.str.contains('2018')].sum(axis=1)
        H['2019_total'] = H.loc[:,H.columns.str.contains('2019')].sum(axis=1)
        H['2018_others'] = H['2018_total'] - H.loc[:,['2018_'+col for col in target]].sum(axis=1)
        H['2019_others'] = H['2019_total'] - H.loc[:,['2019_'+col for col in target]].sum(axis=1)
        
        H_18 = H.loc[:,['2018_'+col for col in target]].values
        H_19 = H.loc[:,['2019_'+col for col in target]].values
        Ht_18 = H.loc[:,'2018_total'].values
        Ht_19 = H.loc[:,'2019_total'].values
        G = list(dict_target_share[city_name]['gr'].values())
        S = list(dict_target_share[city_name]['share'].values())
        S_ly = np.array(list(dict_target_share[city_name]['share_ly'].values()))
        W_0 = np.array(H['weight']).reshape(-1,1)
        
        # 梯度下降
        # H_n=H_19, H_share=Ht_19, H_gr=H_18, H_ly=Ht_18
        W_result, X, Growth, Share= gradient_descent(W_0, S, G, S_ly, l, m, brand_number, H_19, Ht_19, H_18, Ht_18, 
                                                    lmda, gradient_type)
        # 首先标准化，使w优化前后的点积相等
        W_norm = W_result * Ht_19.T.dot(W_0)[0]/Ht_19.T.dot(W_result)[0]
        
        H2 = deepcopy(H)
        H2['weight_factor'] = (W_norm-1)/(np.array(H2['weight']).reshape(-1,1)-1)
        H2['weight_factor1'] = (W_norm)/(np.array(H2['weight']).reshape(-1,1))
        H2.loc[(H2['Bedsize>99'] == 0), 'weight_factor'] = H2.loc[(H2['Bedsize>99'] == 0), 'weight_factor1']
        H2['weight_factor'].fillna(0, inplace=True)
        # H2.loc[(H2['weight_factor'] < 0), 'weight_factor'] = 0
        H2['W'] = W_norm
        # 整理H2便于输出
        H2['weight_factor'] = [x if math.isinf(x)==False else 0 for x in H2['weight_factor']]
        H2['weight_factor1'] = [x if math.isinf(x)==False else 0 for x in H2['weight_factor1']]
        H2[H2.columns] = H2[H2.columns].astype("str")
        H2 = H2[['City', 'City_Sample', 'PHA', 'weight', 'Bedsize>99','weight_factor', 'weight_factor1', 'W']]
        return H2
    
    schema= StructType([
            StructField("City", StringType(), True),
            StructField("PHA", StringType(), True),
            StructField("City_Sample", StringType(), True),
            StructField("weight", StringType(), True),
            StructField("Bedsize>99", StringType(), True),
            StructField("weight_factor", StringType(), True),
            StructField("weight_factor1", StringType(), True),
            StructField("W", StringType(), True)
            ])
    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def udf_target_weight(pdf):
        return func_target_weight(pdf, dict_target_share, l=learning_rate, m=max_iteration, lmda=lmda, gradient_type=gradient_type)
    
    # ====  三. 数据分析  ====
    
    # 每个市场 进行分析
    market_list = list(market_city_brand.keys())
    index = 0
    for market in market_list:
        # 输入文件
        # market = '固力康'
        index += 1
        data_target_path = max_path + '/' + project_name + '/weight/' + market + '_data_target'
        ims_gr_path = max_path + '/' + project_name + '/weight/' + market + '_ims_gr'
        
        # 输出
        df_sum_path = max_path + '/' + project_name + '/weight/' + market + '_share_gr_out'
        df_weight_path = max_path + '/' + project_name + '/weight/' + market + '_weight_raw_out'
        
        # 1. 该市场所需要的分析的城市
        city_brand_dict = market_city_brand[market]
        city_list = list(city_brand_dict.keys())
        
        # 2. 利用ims_sales_gr，生成每个城市 top 产品的 'gr','share','share_ly' 字典
        ims_sales_gr = spark.read.parquet(ims_gr_path)
        ims_sales_gr_city = ims_sales_gr.where(col('City').isin(city_list))
        target_share = ims_sales_gr_city.groupBy('City').apply(udf_target_brand)
        
        # 转化为字典格式
        df_target_share = target_share.agg(func.collect_list('dict').alias('dict_all')).select("dict_all").toPandas()
        df_target_share = df_target_share["dict_all"].values[0]
        length_dict = len(df_target_share)
        str_target_share = ""
        for index, each in enumerate(df_target_share):
            if index == 0:
                str_target_share += "{" + each[1:-1]
            elif index == length_dict - 1:
                str_target_share += "," + each[1:-1] + "}"
            else:
                str_target_share += "," + each[1:-1]
            
            if length_dict == 1:
                str_target_share += "}"
        dict_target_share  = json.loads(str_target_share)
        
        # 3. 对 data_target 进行weight分析
        data_target = spark.read.parquet(data_target_path)
        df_weight_out = data_target.groupBy('City').apply(udf_target_weight).persist()
        df_weight_out = df_weight_out.withColumn('weight', col('weight').cast(DoubleType())) \
                           .withColumn('weight_factor', col('weight_factor').cast(DoubleType())) \
                           .withColumn('weight_factor1', col('weight_factor1').cast(DoubleType())) \
                           .withColumn('Bedsize>99', col('Bedsize>99').cast(DoubleType())) \
                           .withColumn('W', col('W').cast(DoubleType())) 
        
        # 4. 输出结果整理
        # 4.1 原始的weight结果
        df_weight_out = df_weight_out.repartition(1)
        df_weight_out.write.format("parquet") \
            .mode("append").save(df_weight_path)
            
        # 4.2 用于生产的weight结果
        df_weight_final = df_weight_out.withColumn('weight_factor', func.when(col('Bedsize>99')==0, col('weight_factor1')) \
                                                                        .otherwise((col('W')-1)/(col('weight')-1)))
                                                                        
        df_weight_final = df_weight_final.fillna(0, 'weight_factor') \
                                        .withColumn('DOI', func.lit(market)) \
                                        .select('PHA', 'weight_factor', 'DOI', 'City') \
                                        .join(universe, on='City', how='left') \
                                        .withColumnRenamed('weight_factor', 'weight')
        
        df_weight_final = df_weight_final.withColumn('Province', func.when(col('City')=='福厦泉', func.lit('福建省')) \
                                                                    .otherwise(col('Province'))) \
                                         .withColumn('Province', func.when(col('City')=='珠三角', func.lit('广东省')) \
                                                                    .otherwise(col('Province'))) 
        if index == 0:
            df_weight_final = df_weight_final.repartition(1)
            df_weight_final.write.format("parquet") \
                .mode("overwrite").save(weight_path)
        else:
            df_weight_final = df_weight_final.repartition(1)
            df_weight_final.write.format("parquet") \
                .mode("append").save(weight_path)
        
        # 4.3 share 和 gr 结果
        data_final = data_target.join(df_weight_out.select('PHA','W'), on='PHA', how='left')
        data_final = data_final.withColumn('MAX_new', col('Sales')*col('W'))
        
        df_sum = data_final.groupBy('City','标准商品名','Year').agg(func.sum('MAX_new').alias('MAX_new')).persist()
        df_sum = df_sum.groupBy('City', '标准商品名').pivot('Year').agg(func.sum('MAX_new')).fillna(0).persist()
        
        str_sum_2018 = df_sum.agg(func.sum('2018').alias('sum')).collect()[0][0]
        str_sum_2019 = df_sum.agg(func.sum('2019').alias('sum')).collect()[0][0]
        df_sum = df_sum.withColumn('Share_2018', func.bround(col('2018')/str_sum_2018, 3)) \
                       .withColumn('Share_2019', func.bround(col('2019')/str_sum_2019, 3)) \
                       .withColumn('GR', func.bround(col('2019')/col('2018')-1, 3))
                       
        df_sum = df_sum.repartition(1)
        df_sum.write.format("parquet") \
            .mode("overwrite").save(df_sum_path)

    return {}





