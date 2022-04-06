
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
    market_city_brand = kwargs['market_city_brand']
    lmda = kwargs['lmda']
    learning_rate = kwargs['learning_rate']
    max_iteration = kwargs['max_iteration']
    gradient_type = kwargs['gradient_type']
    year_list = kwargs['year_list']
    g_input_version = kwargs['g_input_version']
    ### input args ###
    
    ### output args ###
    p_out = kwargs['p_out']
    out_mode = kwargs['out_mode']
    run_id = kwargs['run_id'].replace(":","_")
    owner = kwargs['owner']
    project_name = kwargs['project_name']
    g_database_temp = kwargs['g_database_temp']
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
    import boto3    
    from phcli.ph_tools.addTable.addTableToGlue import AddTableToGlue
    
    # %%
    # 输入
    year_list = year_list.replace(" ","").split(",")
    year_min = year_list[0]
    year_max = year_list[1]
    
    lmda = float(lmda)
    learning_rate = int(learning_rate)
    max_iteration = int(max_iteration)
    # market_city_brand = json.loads(market_city_brand)
    
    def getMarketCityBrandDict(market_city_brand):
        market_city_brand_dict = {}
        for each in market_city_brand.replace(" ","").split(","):
            market_name = each.split(":")[0]
            if market_name not in market_city_brand_dict.keys():
                market_city_brand_dict[market_name]={}
            city_brand = each.split(":")[1]
            for each in city_brand.replace(" ","").split("|"): 
                city = each.split("_")[0]
                brand = each.split("_")[1]
                market_city_brand_dict[market_name][city]=brand
        return market_city_brand_dict     
    market_city_brand_dict = getMarketCityBrandDict(market_city_brand)
    logger.debug(market_city_brand_dict)
      
    # ============== 删除已有的s3中间文件 =============
    g_table_weight = 'PHA_weight'
    g_table_weight_tmp = 'PHA_weight_tmp'
    g_table_share_gr = 'share_gr_out'
    g_table_weight_raw = 'weight_raw_out'
        
    import boto3
    def deletePath(path_dir):
        file_name = path_dir.replace('//', '/').split('s3:/ph-platform/')[1]
        s3 = boto3.resource('s3', region_name='cn-northwest-1',
                            aws_access_key_id="AKIAWPBDTVEAEU44ZAGT",
                            aws_secret_access_key="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")
        bucket = s3.Bucket('ph-platform')
        bucket.objects.filter(Prefix=file_name).delete()
    deletePath(path_dir=f"{p_out + g_table_weight}/version={run_id}/provider={project_name}/owner={owner}/")
    deletePath(path_dir=f"{p_out + g_table_weight_tmp}/version={run_id}/provider={project_name}/owner={owner}/")
    deletePath(path_dir=f"{p_out + g_table_share_gr}/version={run_id}/provider={project_name}/owner={owner}/")
    deletePath(path_dir=f"{p_out + g_table_weight_raw}/version={run_id}/provider={project_name}/owner={owner}/")
    
    # %% 
    # =========== 输入数据读取 =========== 
    def dealToNull(df):
        df = df.replace(["None", ""], None)
        return df
    
    def dealScheme(df, dict_scheme):
        # 数据类型处理
        if dict_scheme != {}:
            for i in dict_scheme.keys():
                df = df.withColumn(i, col(i).cast(dict_scheme[i]))
        return df

    def lowCol(df):
        df = df.toDF(*[c.lower() for c in df.columns])
        return df
    
    def getInputVersion(df, table_name):
        # 如果 table在g_input_version中指定了version，则读取df后筛选version，否则使用传入的df
        version = g_input_version.get(table_name, '')
        if version != '':
            version_list =  version.replace(' ','').split(',')
            df = df.where(col('version').isin(version_list))
        return df
    
    def readInFile(table_name, dict_scheme={}):
        df = kwargs[table_name]
        df = dealToNull(df)
        df = lowCol(df)
        df = dealScheme(df, dict_scheme)
        df = getInputVersion(df, table_name.replace('df_', ''))
        return df
       
    df_ims_sales_gr_all = readInFile('df_ims_growth_rate')
    
    df_data_target_all = readInFile('df_weight_data_target')
    
    df_universe = readInFile('df_universe_base')
    
    # %%
    # ==========  数据执行  ============
    
    # ====  一. 数据准备  ==== 
    # 1. universe 文件                  
    df_universe = df_universe.select("Province", "City").distinct()
    
    
    # ====  二. 函数定义  ====
    
    # 1. 利用 ims_sales_gr，生成城市 top 产品的 'gr','share','share_ly' 字典
    def func_target_brand(pdf, city_brand_dict):
        import json
        city_name = pdf['city'][0]
        brand_number = city_brand_dict[city_name]
        pdf = pdf.sort_values(by='share', ascending=False).reset_index()[0:int(brand_number)]
        dict_share = pdf.groupby(['city'])['标准商品名','gr','share','share_ly'].apply(lambda x : x.set_index('标准商品名').to_dict()).to_dict()
        dict_share = json.dumps(dict_share)
        return pd.DataFrame([[city_name] + [dict_share]], columns=['city', 'dict'])
    
    schema= StructType([
            StructField("city", StringType(), True),
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
        city_name = pdf['city'][0]
        target = list(dict_target_share[city_name]['gr'].keys())
        brand_number = len(target)
    
        H = pdf.pivot_table(index=['city','pha','city_sample','weight', 'bedsize>99'], columns=['tmp'], 
                            values='sales', fill_value=0, aggfunc='sum')
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
        H2.loc[(H2['bedsize>99'] == 0), 'weight_factor'] = H2.loc[(H2['bedsize>99'] == 0), 'weight_factor1']
        H2['weight_factor'].fillna(0, inplace=True)
        # H2.loc[(H2['weight_factor'] < 0), 'weight_factor'] = 0
        H2['W'] = W_norm
        # 整理H2便于输出
        H2['weight_factor'] = [x if math.isinf(x)==False else 0 for x in H2['weight_factor']]
        H2['weight_factor1'] = [x if math.isinf(x)==False else 0 for x in H2['weight_factor1']]
        H2[H2.columns] = H2[H2.columns].astype("str")
        H2 = H2[['city', 'city_sample', 'pha', 'weight', 'bedsize>99','weight_factor', 'weight_factor1', 'W']]
        return H2
    
    schema= StructType([
            StructField("city", StringType(), True),
            StructField("pha", StringType(), True),
            StructField("city_sample", StringType(), True),
            StructField("weight", StringType(), True),
            StructField("bedsize>99", StringType(), True),
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
        # 1. 该市场所需要的分析的城市
        city_brand_dict = market_city_brand_dict[market]
        city_list = list(city_brand_dict.keys())
    
        # 2. 利用ims_sales_gr，生成每个城市 top 产品的 'gr','share','share_ly' 字典
        df_ims_sales_gr = df_ims_sales_gr_all.where(col('doi') == market)
        df_ims_sales_gr_city = df_ims_sales_gr.where(col('city').isin(city_list))
        target_share = df_ims_sales_gr_city.groupBy('city').apply(udf_target_brand)
    
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
        data_target = df_data_target_all.where(col('doi') == market).where(col('city').isin(city_list))
        df_weight_out = data_target.groupBy('city').apply(udf_target_weight).persist()
        df_weight_out = df_weight_out.withColumn('weight', col('weight').cast(DoubleType())) \
                           .withColumn('weight_factor', col('weight_factor').cast(DoubleType())) \
                           .withColumn('weight_factor1', col('weight_factor1').cast(DoubleType())) \
                           .withColumn('bedsize>99', col('bedsize>99').cast(DoubleType())) \
                           .withColumn('W', col('W').cast(DoubleType())) 
    
        # 4. 输出结果整理
        # 4.1 原始的weight结果
        # ==== 输出 ====
        df_weight_out = lowCol(df_weight_out)
        AddTableToGlue(df=df_weight_out, database_name_of_output=g_database_temp, table_name_of_output=g_table_weight_raw, 
                           path_of_output_file=p_out, mode=out_mode) \
                    .add_info_of_partitionby({"version":run_id,"provider":project_name,"owner":owner})
          
        # 4.2 用于生产的weight结果
        df_weight_final = df_weight_out.withColumn('weight_factor', func.when(col('bedsize>99')==0, col('weight_factor1')) \
                                                                        .otherwise((col('W')-1)/(col('weight')-1)))
    
        df_weight_final = df_weight_final.fillna(0, 'weight_factor') \
                                        .withColumn('DOI', func.lit(market)) \
                                        .select('pha', 'weight_factor', 'DOI', 'city') \
                                        .join(df_universe, on='city', how='left') \
                                        .withColumnRenamed('weight_factor', 'weight')
    
        df_weight_final = df_weight_final.withColumn('Province', func.when(col('city')=='福厦泉', func.lit('福建省')) \
                                                                    .otherwise(col('Province'))) \
                                         .withColumn('Province', func.when(col('city')=='珠三角', func.lit('广东省')) \
                                                                    .otherwise(col('Province'))) \
                                         .withColumn('Province', func.when(col('city')=='浙江市', func.lit('浙江省')) \
                                                                    .otherwise(col('Province'))) \
                                        .withColumn('Province', func.when(col('city')=='苏锡市', func.lit('江苏省')) \
                                                                    .otherwise(col('Province'))) 
        # ==== 输出 ====
        df_weight_final = lowCol(df_weight_final)        
        AddTableToGlue(df=df_weight_final, database_name_of_output=g_database_temp, table_name_of_output=g_table_weight_tmp, 
                           path_of_output_file=p_out, mode=out_mode) \
                    .add_info_of_partitionby({"version":run_id,"provider":project_name,"owner":owner})
    
    
        # 4.3 share 和 gr 结果
        data_final = data_target.join(df_weight_out.select('pha','W'), on='pha', how='left')
        data_final = data_final.withColumn('MAX_new', col('sales')*col('W'))
    
        df_sum = data_final.groupBy('city','标准商品名','Year').agg(func.sum('MAX_new').alias('MAX_new')).persist()
        df_sum = df_sum.groupBy('city', '标准商品名').pivot('Year').agg(func.sum('MAX_new')).fillna(0).persist()
        
        df_sum_city = df_sum.groupBy('city').agg(func.sum(year_min).alias('str_sum_'+year_min), func.sum(year_max).alias('str_sum_'+year_max))
    
        df_sum = df_sum.join(df_sum_city, on='city', how='left')
        #str_sum_2018 = df_sum.agg(func.sum('2018').alias('sum')).collect()[0][0]
        #str_sum_2019 = df_sum.agg(func.sum('2019').alias('sum')).collect()[0][0]
        df_sum = df_sum.withColumn('Share_'+year_min, func.bround(col(year_min)/col('str_sum_'+year_min), 3)) \
                       .withColumn('Share_'+year_max, func.bround(col(year_max)/col('str_sum_'+year_max), 3)) \
                       .withColumn('GR', func.bround(col(year_max)/col(year_min)-1, 3)) \
                        .drop('str_sum_'+year_min, 'str_sum_'+year_max)
        
        # ==== 输出 ====
        df_sum = lowCol(df_sum)        
        AddTableToGlue(df=df_sum, database_name_of_output=g_database_temp, table_name_of_output=g_table_share_gr, 
                           path_of_output_file=p_out, mode=out_mode) \
                    .add_info_of_partitionby({"version":run_id,"provider":project_name,"owner":owner})
        
    # %%
    # ====  四. 数据处理  ====
    # 读回
    df_weight_out_all = spark.sql("SELECT * FROM %s.%s WHERE version='%s' AND provider='%s' AND  owner='%s'" 
                                 %(g_database_temp, g_table_weight_tmp, run_id, project_name, owner))
    df_weight_out_all = df_weight_out_all.drop('version', 'provider', 'owner')
    
    # 1、福夏泉，珠三角 城市展开  
    citys = df_weight_out_all.select('city').distinct().toPandas()['city'].tolist()
    
    if '福厦泉' or '珠三角' or "浙江市" or "苏锡市" in citys:
        df_keep = df_weight_out_all.where(~col('city').isin('福厦泉', '珠三角', "浙江市", "苏锡市"))
        
        if '福厦泉' in citys:
            df1 = df_weight_out_all.where(col('city') == '福厦泉')
            df1_1 = df1.withColumn('city', func.lit('福州市'))
            df1_2 = df1.withColumn('city', func.lit('厦门市'))
            df1_3 = df1.withColumn('city', func.lit('泉州市'))
            df1_new = df1_1.union(df1_2).union(df1_3)
            # 合并
            df_keep = df_keep.union(df1_new)
    
        if '珠三角' in citys:
            df2 = df_weight_out_all.where(col('city') == '珠三角')
            df2_1 = df2.withColumn('city', func.lit('珠海市'))
            df2_2 = df2.withColumn('city', func.lit('东莞市'))
            df2_3 = df2.withColumn('city', func.lit('中山市'))
            df2_4 = df1.withColumn('city', func.lit('佛山市'))
            df2_new = df2_1.union(df2_2).union(df2_3).union(df2_4)
            # 合并
            df_keep = df_keep.union(df2_new)
            
        if '浙江市' in citys:
            df3 = df_weight_out_all.where(col('city') == '浙江市')
            df3_1 = df3.withColumn('city', func.lit('绍兴市'))
            df3_2 = df3.withColumn('city', func.lit('嘉兴市'))
            df3_3 = df3.withColumn('city', func.lit('台州市'))
            df3_4 = df3.withColumn('city', func.lit('金华市'))
            df3_new = df3_1.union(df3_2).union(df3_3).union(df3_4)
            # 合并
            df_keep = df_keep.union(df3_new)
    
        if '苏锡市' in citys:
            df4 = df_weight_out_all.where(col('city') == '苏锡市')
            df4_1 = df4.withColumn('city', func.lit('苏州市'))
            df4_2 = df4.withColumn('city', func.lit('无锡市'))
            df4_new = df4_1.union(df4_2)
            # 合并
            df_keep = df_keep.union(df4_new)
    
        df_weight_out_all = df_keep     
    
    # %%
    # =========== 数据输出 =============
    # 读回
    df_weight_out_all = lowCol(df_weight_out_all)
    return {"out_df":df_weight_out_all}

    # %%
    # 2、输出判断是否已有 weight_path 结果，对已有 weight_path 结果替换或者补充
    '''
    # 如果已经存在 weight_path 则用新的结果对已有结果进行(Province,City,DOI)替换和补充
    
    
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
        new_info = df_weight_out.select('Province', 'city', 'DOI').distinct()
        old_out_keep = old_out.join(new_info, on=['Province', 'city', 'DOI'], how='left_anti')
        df_weight_out = df_weight_out.union(old_out_keep.select(df_weight_out.columns))           
        # 中间文件读写一下
        df_weight_out = df_weight_out.repartition(2)
        df_weight_out.write.format("parquet") \
                            .mode("overwrite").save(tmp_path)
        df_weight_out = spark.read.parquet(tmp_path)   
    
    # 3、输出到 weight_path
    df_weight_out = df_weight_out.repartition(2)
    df_weight_out.write.format("parquet") \
        .mode("overwrite").save(weight_path)
    '''
