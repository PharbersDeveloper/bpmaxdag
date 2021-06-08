# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL


def execute(**kwargs):
    logger = phs3logger(kwargs["job_id"], LOG_DEBUG_LEVEL)
    spark = kwargs['spark']()
    result_path_prefix = kwargs["result_path_prefix"]
    depends_path = kwargs["depends_path"]
    
    ### input args ###
    max_path = kwargs['max_path']
    project_name = kwargs['project_name']
    market_city_brand = kwargs['market_city_brand']
    lmda = kwargs['lmda']
    learning_rate = kwargs['learning_rate']
    max_iteration = kwargs['max_iteration']
    gradient_type = kwargs['gradient_type']
    test = kwargs['test']
    year_list = kwargs['year_list']
    ### input args ###
    
    ### output args ###
    c = kwargs['c']
    d = kwargs['d']
    ### output args ###

    from pyspark.sql import SparkSession, Window
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
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
    import boto3    # %%
    
    # year_list = '2019,2020'
    # project_name = "Takeda"
    # outdir = "202012"
    # market_city_brand = "TK1:上海市_3"
    # # 2019_邦得清
    # #精神:天津市_3|福厦泉_3|宁波市_3|济南市_3|温州市_2|常州市_3

    # %%
    # 输入
    if test != "False" and test != "True":
        logger.info('wrong input: test, False or True') 
        raise ValueError('wrong input: test, False or True')
    
    year_list = year_list.replace(" ","").split(",")
    year_min = year_list[0]
    year_max = year_list[1]
    
    lmda = float(lmda)
    learning_rate = int(learning_rate)
    max_iteration = int(max_iteration)
    # market_city_brand = json.loads(market_city_brand)
    universe_path = max_path + '/' + project_name + '/universe_base'
    
    market_city_brand_dict={}
    for each in market_city_brand.replace(" ","").split(","):
        market_name = each.split(":")[0]
        if market_name not in market_city_brand_dict.keys():
            market_city_brand_dict[market_name]={}
        city_brand = each.split(":")[1]
        for each in city_brand.replace(" ","").split("|"): 
            city = each.split("_")[0]
            brand = each.split("_")[1]
            market_city_brand_dict[market_name][city]=brand
    logger.debug(market_city_brand_dict)
    
    # 输出
    weight_tmp_path = max_path + '/' + project_name + '/weight/PHA_weight_tmp'
    tmp_path = max_path + '/' + project_name + '/weight/tmp'
    if test == "False":
        weight_path = max_path + '/' + project_name + '/PHA_weight'
    else:
        weight_path = max_path + '/' + project_name + '/weight/PHA_weight'

    # %%
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
    def func_target_weight(pdf, dict_target_share, l, m, lmda, gradient_type, year_min, year_max):
        city_name = pdf['City'][0]
        target = list(dict_target_share[city_name]['gr'].keys())
        brand_number = len(target)
    
        H = pdf.pivot_table(index=['City','PHA','City_Sample','weight', 'Bedsize>99'], columns=['tmp'], 
                            values='Sales', fill_value=0, aggfunc='sum')
        H = H.reset_index()
    
        # 判断target是否在data中，如果不在给值为0(会有小城市，ims的top—n产品在data中不存在)
        for each in target:
            if year_min+'_'+each not in H.columns:
                H[year_min+'_'+each]=0
            if year_max+'_'+each not in H.columns:
                H[year_max+'_'+each]=0
                
        H[year_min+'_total'] = H.loc[:,H.columns.str.contains(year_min)].sum(axis=1)
        H[year_max+'_total'] = H.loc[:,H.columns.str.contains(year_max)].sum(axis=1)
        H[year_min+'_others'] = H[year_min+'_total'] - H.loc[:,[year_min+'_'+col for col in target]].sum(axis=1)
        H[year_max+'_others'] = H[year_max+'_total'] - H.loc[:,[year_max + '_'+col for col in target]].sum(axis=1)
    
        H_18 = H.loc[:,[year_min+'_'+col for col in target]].values
        H_19 = H.loc[:,[year_max+'_'+col for col in target]].values
        Ht_18 = H.loc[:,year_min+'_total'].values
        Ht_19 = H.loc[:,year_max+'_total'].values
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
        return func_target_weight(pdf, dict_target_share, l=learning_rate, m=max_iteration, lmda=lmda, gradient_type=gradient_type, 
                                  year_min=year_min, year_max=year_max)
    

    # %%
    # ====  三. 数据分析  ====
    
    # 每个市场 进行分析
    market_list = list(market_city_brand_dict.keys())
    index_file = 0
    for market in market_list:
        # 输入文件
        # market = '固力康'
        data_target_path = max_path + '/' + project_name + '/weight/' + market + '_data_target'
        ims_gr_path = max_path + '/' + project_name + '/weight/' + market + '_ims_gr'
    
        # 输出
        df_sum_path = max_path + '/' + project_name + '/weight/' + market + '_share_gr_out'
        df_weight_path = max_path + '/' + project_name + '/weight/' + market + '_weight_raw_out'
    
        # 1. 该市场所需要的分析的城市
        city_brand_dict = market_city_brand_dict[market]
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
        data_target = spark.read.parquet(data_target_path).where(col('City').isin(city_list))
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
            .mode("overwrite").save(df_weight_path)
    
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
                                                                    .otherwise(col('Province'))) \
                                         .withColumn('Province', func.when(col('City')=='浙江市', func.lit('浙江省')) \
                                                                    .otherwise(col('Province'))) \
                                        .withColumn('Province', func.when(col('City')=='苏锡市', func.lit('江苏省')) \
                                                                    .otherwise(col('Province'))) 
    
        if index_file == 0:
            df_weight_final = df_weight_final.repartition(1)
            df_weight_final.write.format("parquet") \
                .mode("overwrite").save(weight_tmp_path)
        else:
            df_weight_final = df_weight_final.repartition(1)
            df_weight_final.write.format("parquet") \
                .mode("append").save(weight_tmp_path)
    
    
        # 4.3 share 和 gr 结果
        data_final = data_target.join(df_weight_out.select('PHA','W'), on='PHA', how='left')
        data_final = data_final.withColumn('MAX_new', col('Sales')*col('W'))
    
        df_sum = data_final.groupBy('City','标准商品名','Year').agg(func.sum('MAX_new').alias('MAX_new')).persist()
        df_sum = df_sum.groupBy('City', '标准商品名').pivot('Year').agg(func.sum('MAX_new')).fillna(0).persist()
        
        df_sum_city = df_sum.groupBy('City').agg(func.sum(year_min).alias('str_sum_'+year_min), func.sum(year_max).alias('str_sum_'+year_max))
    
        df_sum = df_sum.join(df_sum_city, on='City', how='left')
        #str_sum_2018 = df_sum.agg(func.sum('2018').alias('sum')).collect()[0][0]
        #str_sum_2019 = df_sum.agg(func.sum('2019').alias('sum')).collect()[0][0]
        df_sum = df_sum.withColumn('Share_'+year_min, func.bround(col(year_min)/col('str_sum_'+year_min), 3)) \
                       .withColumn('Share_'+year_max, func.bround(col(year_max)/col('str_sum_'+year_max), 3)) \
                       .withColumn('GR', func.bround(col(year_max)/col(year_min)-1, 3)) \
                        .drop('str_sum_'+year_min, 'str_sum_'+year_max)
    
        df_sum = df_sum.repartition(1)
        df_sum.write.format("parquet") \
            .mode("overwrite").save(df_sum_path)
    
        index_file += 1
        
    # %%
    # ====  四. 数据处理  ====
    
    # 1、福夏泉，珠三角 城市展开
    df_weight_final = spark.read.parquet(weight_tmp_path)
    citys = df_weight_final.select('City').distinct().toPandas()['City'].tolist()
    
    if '福厦泉' or '珠三角' or "浙江市" or "苏锡市" in citys:
        df_keep = df_weight_final.where(~col('City').isin('福厦泉', '珠三角', "浙江市", "苏锡市"))
        
        if '福厦泉' in citys:
            df1 = df_weight_final.where(col('City') == '福厦泉')
            df1_1 = df1.withColumn('City', func.lit('福州市'))
            df1_2 = df1.withColumn('City', func.lit('厦门市'))
            df1_3 = df1.withColumn('City', func.lit('泉州市'))
            df1_new = df1_1.union(df1_2).union(df1_3)
            # 合并
            df_keep = df_keep.union(df1_new)
    
        if '珠三角' in citys:
            df2 = df_weight_final.where(col('City') == '珠三角')
            df2_1 = df2.withColumn('City', func.lit('珠海市'))
            df2_2 = df2.withColumn('City', func.lit('东莞市'))
            df2_3 = df2.withColumn('City', func.lit('中山市'))
            df2_4 = df1.withColumn('City', func.lit('佛山市'))
            df2_new = df2_1.union(df2_2).union(df2_3).union(df2_4)
            # 合并
            df_keep = df_keep.union(df2_new)
            
        if '浙江市' in citys:
            df3 = df_weight_final.where(col('City') == '浙江市')
            df3_1 = df3.withColumn('City', func.lit('绍兴市'))
            df3_2 = df3.withColumn('City', func.lit('嘉兴市'))
            df3_3 = df3.withColumn('City', func.lit('台州市'))
            df3_4 = df3.withColumn('City', func.lit('金华市'))
            df3_new = df3_1.union(df3_2).union(df3_3).union(df3_4)
            # 合并
            df_keep = df_keep.union(df3_new)
    
        if '苏锡市' in citys:
            df4 = df_weight_final.where(col('City') == '苏锡市')
            df4_1 = df4.withColumn('City', func.lit('苏州市'))
            df4_2 = df4.withColumn('City', func.lit('无锡市'))
            df4_new = df4_1.union(df4_2)
            # 合并
            df_keep = df_keep.union(df4_new)
    
        df_weight_final = df_keep     
    

    # %%
    # 2、输出判断是否已有 weight_path 结果，对已有 weight_path 结果替换或者补充
    '''
    如果已经存在 weight_path 则用新的结果对已有结果进行(Province,City,DOI)替换和补充
    '''
    
    file_name = weight_path.replace('//', '/').split('s3a:/ph-max-auto/')[1]
    
    s3 = boto3.resource('s3', region_name='cn-northwest-1',
                            aws_access_key_id="AKIAWPBDTVEAEU44ZAGT",
                            aws_secret_access_key="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")
    bucket = s3.Bucket('ph-max-auto')
    judge = 0
    for obj in bucket.objects.filter(Prefix = file_name):
        path, filename = os.path.split(obj.key)  
        if path == file_name:
            judge += 1
    if judge > 0:
        old_out = spark.read.parquet(weight_path)   
        new_info = df_weight_final.select('Province', 'City', 'DOI').distinct()
        old_out_keep = old_out.join(new_info, on=['Province', 'City', 'DOI'], how='left_anti')
        df_weight_final = df_weight_final.union(old_out_keep.select(df_weight_final.columns))           
        # 中间文件读写一下
        df_weight_final = df_weight_final.repartition(2)
        df_weight_final.write.format("parquet") \
                            .mode("overwrite").save(tmp_path)
        df_weight_final = spark.read.parquet(tmp_path)   
    
    # 3、输出到 weight_path
    df_weight_final = df_weight_final.repartition(2)
    df_weight_final.write.format("parquet") \
        .mode("overwrite").save(weight_path)
