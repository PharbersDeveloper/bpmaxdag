

# from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL


def job1(kwargs):
    # ### input args ###
    max_path = kwargs.get('max_path', 'D:/Auto_MAX/MAX/')
    project_name = kwargs['project_name']
    outdir = kwargs['outdir']
    model_month_right = kwargs['model_month_right']
    model_month_left = kwargs['model_month_left']
    all_models = kwargs['all_models']
    universe_choice = kwargs.get('universe_choice', 'Empty')
    rf_ntree = kwargs.get('rf_ntree', '500')
    rf_minnode = kwargs.get('rf_minnode', '5')
    # ### input args ###
    '''
    max_path = 'MAX'
    project_name = '神州'
    outdir = '202012'
    model_month_right = '201912'
    model_month_left = '201901'
    all_models = 'SZ1'
    universe_choice = 'SZ1:universe_onc'
    rf_ntree = '500'
    rf_minnode = '5'
    '''
    from pyspark.sql import SparkSession, Window
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    import os
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col
    import time
    import re
    
    from pyspark.ml import Pipeline
    from pyspark.ml.regression import RandomForestRegressor
    from pyspark.ml.feature import VectorIndexer, StringIndexer
    from pyspark.ml.linalg import DenseVector
    from pyspark.ml.evaluation import RegressionEvaluator
    from pyspark.ml.feature import VectorAssembler
    import pandas as pd

    spark = SparkSession.builder.getOrCreate()
    # %%
    '''
    project_name = 'Sanofi'
    outdir = '202012'
    model_month_right = '202012'
    model_month_left = '202001'
    all_models = 'SNY15,SNY16,SNY17'
    universe_choice = 'SNY15:universe_az_sanofi_mch,SNY16:universe_az_sanofi_mch,SNY17:universe_az_sanofi_mch'
    rf_ntree = '500'
    rf_minnode = '5'
    '''
    print("job1_randomforest")
    # %%
    rf_minnode = int(rf_minnode)
    rf_ntree = int(rf_ntree)
    model_month_right = int(model_month_right)
    model_month_left = int(model_month_left)
    all_models = all_models.replace(' ','').split(',')
    # 市场的universe文件
    universe_choice_dict={}
    if universe_choice != "Empty":
        for each in universe_choice.replace(" ","").split(","):
            market_name = each.split(":")[0]
            universe_name = each.split(":")[1]
            universe_choice_dict[market_name]=universe_name
    
    doctor_path = max_path + '/Common_files/factor_files/doctor.csv' 
    BT_PHA_path = max_path + '/Common_files/factor_files/BT_PHA.csv'
    ind_path = max_path + '/Common_files/factor_files/ind.csv'
    cpa_pha_mapping_path = max_path + '/' + project_name + '/cpa_pha_mapping'
    mkt_mapping_path = max_path + '/' + project_name + '/mkt_mapping'
    
    raw_data_path = max_path + '/' + project_name + '/' + outdir + '/raw_data'
    #raw_data_path = 's3a://ph-max-auto/v0.0.1-2020-06-08/Test/Eisai/raw_data.csv'
    product_map_path = max_path + '/' + project_name + '/' + outdir + '/prod_mapping'

    # %%
    # =======  数据执行  ============
    
    def deal_ID_length(df):
        # ID不足7位的补足0到6位
        # 国药诚信医院编码长度是7位数字，cpa医院编码是6位数字，其他还有包含字母的ID
        df = df.withColumn("ID", df["ID"].cast(StringType()))
        # 去掉末尾的.0
        df = df.withColumn("ID", func.regexp_replace("ID", "\\.0", ""))
        df = df.withColumn("ID", func.when(func.length(df.ID) < 7, func.lpad(df.ID, 6, "0")).otherwise(df.ID))
        return df
    
    # 1. == 文件准备 ==
    
    # 1.1  doctor 文件
    
    def unpivot(df, keys):
        # 功能：数据宽变长
        # 参数说明 df:dataframe,  keys 待转换表中需要保留的主键key，以list[]类型传入
        # 转换是为了避免字段类不匹配，统一将数据转换为string类型，如果保证数据类型完全一致，可以省略该句
        df = df.select(*[col(_).astype("string") for _ in df.columns])
        cols = [_ for _ in df.columns if _ not in keys]
        stack_str = ','.join(map(lambda x: "'%s', `%s`" % (x, x), cols))
        # feature, value 转换后的列名，可自定义
        df = df.selectExpr(*keys, "stack(%s, %s) as (feature, value)" % (len(cols), stack_str))
        return df
    
    doctor = spark.read.csv(doctor_path, header='True', encoding='gbk')
    doctor = doctor.where(~col('BT_Code').isNull()) \
                    .select(['Department', 'Dr_N', 'Dr_N_主任', 'Dr_N_副主任', 'Dr_N_主治', 'Dr_N_住院医', 'BT_Code'])
    doctor_g = unpivot(doctor, ['BT_Code', 'Department']).persist()
    doctor_g = doctor_g.withColumn('dr1', func.concat(col('Department'), func.lit('_'), col('feature'))) \
                        .groupBy('BT_Code', 'dr1').agg(func.sum('value'))
    doctor_g = doctor_g.groupBy('BT_Code').pivot('dr1').sum('sum(value)').persist()
    
    # 1.2 BT_PHA 文件
    BT_PHA = spark.read.csv(BT_PHA_path, header='True', encoding='gbk')
    BT_PHA = BT_PHA.select('BT', 'PHA').dropDuplicates(['PHA'])
    
    # 1.3 ind 文件
    # 列名有点的无法识别，把点换成_
    ind = spark.read.csv(ind_path, header='True', encoding='gbk')
    ind = ind.toDF(*(re.sub(r'[\.\s]+', '_', c) for c in ind.columns))
    for each in ind.columns[18:]:
        ind = ind.withColumn(each, ind[each].cast(DoubleType()))
    
    # 1.4 cpa_pha_mapping 文件
    hosp_mapping = spark.read.parquet(cpa_pha_mapping_path)
    hosp_mapping = deal_ID_length(hosp_mapping)
    
    # 1.5 product_map 文件
    molecule_map = spark.read.parquet(product_map_path)
    molecule_map = molecule_map.select('Molecule','标准通用名').distinct()
    
    # 1.6 mkt_mapping 文件
    mkt_mapping = spark.read.parquet(mkt_mapping_path)
    mkt_mapping = mkt_mapping.withColumnRenamed('mkt', 'Market') \
                        .withColumnRenamed("model", "Market") \
                        .select('Market', '标准通用名').distinct()
    molecule_mkt_map = molecule_map.join(mkt_mapping, on='标准通用名', how='left')
    # molecule_mkt_map.where(molecule_mkt_map.Market.isNull()).select('标准通用名').show()
    
    # 1.7 rawdata 数据
    rawdata = spark.read.parquet(raw_data_path)
    rawdata = deal_ID_length(rawdata)
    rawdata = rawdata.withColumn('Date', col('Date').cast(IntegerType()))
    rawdata = rawdata.where((col('Date') >= model_month_left) & (col('Date') <= model_month_right))
    rawdata = rawdata.join(molecule_mkt_map, on='Molecule', how='left') \
                        .join(hosp_mapping, on='ID', how='left').persist()

    # %%
    # 2. == 每个市场进行 randomForest 分析 ==
    
    # market = '固力康'
    
    for market in all_models:
        # logger.debug("当前market为:" + str(market))
        # 输入
        if market in universe_choice_dict.keys():
            universe_path = max_path + '/' + project_name + '/' + universe_choice_dict[market]
            hospital_range =  spark.read.parquet(universe_path)
        else:
            universe_path = max_path + '/' + project_name + '/universe_base'
            hospital_range =  spark.read.parquet(universe_path)
        # 输出
        df_importances_path = max_path + '/' + project_name + '/forest/' + market + '_importances.csv'
        df_nmse_path = max_path + '/' + project_name + '/forest/' + market + '_NMSE.csv'
        result_path = max_path + '/' + project_name + '/forest/' + market + '_rf_result'
    
        # 2.1 数据处理    
        all_hospital = hospital_range.where(hospital_range.BEDSIZE > 99 ).select('Panel_ID').distinct()
        rawdata_mkt = rawdata.where(rawdata.Market == market) \
                                .withColumnRenamed('PHA', 'PHA_ID') \
                                .withColumnRenamed('Market', 'DOI')
    
        hospital_sample = hospital_range.where(hospital_range.PANEL == 1).select('Panel_ID').distinct().toPandas()['Panel_ID'].values.tolist()
        rawdata_sample = rawdata_mkt.where(col('PHA_ID').isin(hospital_sample)) \
                                .groupBy('PHA_ID', 'DOI').agg(func.sum('Sales').alias('Sales'))
    
        ind_mkt = ind.join(hospital_range.select('Panel_ID').distinct(), ind['PHA_ID']==hospital_range['Panel_ID'], how='inner')
    
        # 2.2 计算 ind_mkt 符合条件的列
        # 去掉不用的列名
        drop_cols = ["Panel_ID", "PHA_Hosp_name", "PHA_ID", "Bayer_ID", "If_Panel", "Segment", "Segment_Description", "If_County"]
        all_cols = list(set(ind_mkt.columns)-set(drop_cols))
        new_all_cols = []
        for each in all_cols:
            if len(re.findall('机构|省|市|县|医院级别|医院等次|医院类型|性质|地址|邮编|年诊疗|总收入|门诊药品收入|住院药品收入|总支出', each)) == 0:
                new_all_cols.append(each)
    
        # 计算每列非空行数
        df_agg = ind_mkt.agg(*[func.count(func.when(~func.isnull(c), c)).alias(c) for c in new_all_cols]).persist()
        # 转置为长数据
        df_agg_col = df_agg.toPandas().T
        df_agg_col.columns = ["notNULL_Count"]
        df_agg_col_names = df_agg_col[df_agg_col.notNULL_Count >= 15000].index.tolist()
    
        '''
        from functools import reduce
        df_agg_col = reduce(
            lambda a, b: a.union(b),
            (df_agg.select(func.lit(c).alias("Column_Name"), func.col(c).alias("notNULL_Count")) 
                for c in df_agg.columns)
        ).persist()
        df_agg_col = df_agg_col.where(col('notNULL_Count') >= 15000)
        df_agg_col_names = df_agg_col.toPandas()['Column_Name'].values
        '''
    
        # 2.3 ind_mkt 文件处理
        ind2 = ind_mkt.select('PHA_ID', *df_agg_col_names, *[i for i in ind_mkt.columns if '心血管' in i ])
        ind3 = ind2.join(BT_PHA, ind2.PHA_ID==BT_PHA.PHA, how='left') \
                    .drop('PHA')
        ind4 = ind3.join(doctor_g, ind3.BT==doctor_g.BT_Code, how='left') \
                    .drop('BT_Code')
        ind5 = ind4.select(*ind2.columns, *[i for i in ind4.columns if '_Dr_N_' in i ])
    
        num_cols = list(set(ind5.columns) -set(['PHA_ID', 'Hosp_level', 'Region', 'respailty', 'Province', 'Prefecture', 
                                            'City_Tier_2010', 'Specialty_1', 'Specialty_2', 'Re_Speialty', 'Specialty_3']))
        ind5 = ind5.fillna(0, subset=num_cols) 
    
    
        # %%
        def f1(x):
            y=(x+0.001)**(1/2)
            return(y)
    
        def f2(x):
            y=x**(2)-0.001
            return(y)
    
        # 2.4 获得 modeldata 
        modeldata = ind5.join(rawdata_sample, on='PHA_ID', how='left')
        Panel_ID_list = hospital_range.where(hospital_range.PANEL == 1).select('Panel_ID').toPandas()['Panel_ID'].values.tolist()
    
        modeldata = modeldata.withColumn('flag_model', 
                                func.when(modeldata.PHA_ID.isin(Panel_ID_list), func.lit('TRUE')) \
                                    .otherwise(func.lit('FALSE'))).persist()
        modeldata = modeldata.withColumn('Sales', func.when((col('Sales').isNull()) & (col('flag_model')=='TRUE'), func.lit(0)) \
                                                     .otherwise(col('Sales')))
        modeldata = modeldata.withColumn('v', func.when(col('Sales') > 0, f1(col('Sales'))).otherwise(func.lit(0)))
    
        trn = modeldata.where(modeldata.PHA_ID.isin(Panel_ID_list))
    
        # 2.5 ===  随机森林 ===
        # 1. 数据准备
        # logger.debug("RandomForest：data prepare")
        not_features_cols = ["PHA_ID", "Province", "Prefecture", "Specialty_1", "Specialty_2", "Specialty_3", "DOI", "Sales", "flag_model"]
        features_str_cols = ['Hosp_level', 'Region', 'respailty', 'Re_Speialty', 'City_Tier_2010']
        features_cols = list(set(trn.columns) -set(not_features_cols)- set(features_str_cols) - set('v'))
    
        def data_for_forest(data):
            # 使用StringIndexer，将features中的字符型变量转为分类数值变量
            indexers = [StringIndexer(inputCol=column, outputCol=column+"_index").fit(data) for column in features_str_cols]
            pipeline = Pipeline(stages=indexers)
            data = pipeline.fit(data).transform(data)
            # 使用 VectorAssembler ，将特征合并为features
            assembler = VectorAssembler( \
                 inputCols = features_cols, \
                 outputCol = "features")
            data = assembler.transform(data)
            data = data.withColumnRenamed('v', 'label')
            # 识别哪些是分类变量，Set maxCategories so features with > 4 distinct values are treated as continuous.
            featureIndexer = VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=15).fit(data)
            data = featureIndexer.transform(data)
            return data
    
        data = data_for_forest(trn)
    
        # 2. 随机森林模型
        # logger.debug("RandomForest：model")
        rf = RandomForestRegressor(labelCol="label", featuresCol="indexedFeatures", 
                numTrees=500, minInstancesPerNode=5, maxDepth=8)
        # numTrees=100, minInstancesPerNode=2, maxDepth=8
        model = rf.fit(data)
    
        # 特征重要性
        # logger.debug("RandomForest：importances")
        dp = model.featureImportances
        dendp = DenseVector(dp)
        df_importances = pd.DataFrame(dendp.array)
        df_importances['feature'] = features_cols
        df_importances.columns=['importances','feature']  
        df_importances = df_importances.sort_values(by='importances', ascending=False)
        df_importances = spark.createDataFrame(df_importances)
    
        df_importances = df_importances.repartition(1)
        df_importances.write.format("csv").option("header", "true") \
                .mode("overwrite").save(df_importances_path)
    
    
        # 3. 对 modeldata预测
        # logger.debug("RandomForest：predict result")
        result = data_for_forest(modeldata)  
        result = result.withColumn('DOI', func.lit(market))
        result = model.transform(result)
        result = result.withColumn('sales_from_model', f2(col('prediction'))) \
                .withColumn('training_set', func.when(col('PHA_ID').isin(trn.select('PHA_ID').distinct().toPandas()['PHA_ID'].values.tolist()), 
                                                    func.lit(1)) \
                                                .otherwise(func.lit(0)))
        result = result.withColumn('final_sales', func.when(col('flag_model')== 'TRUE', col('Sales')) \
                                                    .otherwise(col('sales_from_model')))
        result = result.where(col('PHA_ID').isin(all_hospital.toPandas()['Panel_ID'].values.tolist()))
    
        result = result.repartition(1)
        result.write.format("parquet") \
                .mode("overwrite").save(result_path)
    
        # logger.debug("RandomForest：finish")
    
        # 4. 评价模型（5次随机森林，计算nmse）
        '''
        logger.debug("nmse：RandomForest")
        for i in range(1,6):
            # 模型构建
            (df_training, df_test) = data.randomSplit([0.7, 0.3])
            rf = RandomForestRegressor(labelCol="label", featuresCol="indexedFeatures", 
                    numTrees=100, minInstancesPerNode=2, maxDepth=i)
            model = rf.fit(df_training)
            # 结果预测
            # pipeline = Pipeline(stages=[rf])
            df_training_pred = model.transform(df_training)
            df_training_pred = df_training_pred.withColumn('datatype', func.lit('train'))
            df_test_pred = model.transform(df_test)
            df_test_pred = df_test_pred.withColumn('datatype', func.lit('test'))
            df_test_pred.agg(func.sum('prediction'), func.sum('label')).show()
            #rmse1 = evaluator.evaluate(df_training_pred)
            #print(rmse1)
            #rmse2 = evaluator.evaluate(df_test_pred)
            #print(rmse2)
    
            df = df_training_pred.union(df_test_pred.select(df_training_pred.columns))
            df = df.withColumn('num', func.lit(i))
    
            if i ==1:
                df_all = df
            else:
                df_all = df_all.union(df)
    
        #df_all = df_all.repartition(1)
        #df_all.write.format("csv").option("header", "true") \
        #        .mode("overwrite").save('')
    
        # 计算误差
        logger.debug("nmse：计算")
        schema = StructType([
                    StructField("Province", StringType(), True), 
                    StructField("datatype", StringType(), True),
                    StructField("NMSE", StringType(), True),
                    StructField("num", StringType(), True)
                    ])
    
        @pandas_udf(schema, PandasUDFType.GROUPED_MAP)   
        def nmse_func(pdf):
            import pandas as pd
            import numpy as np
            Province = pdf['Province'][0]
            datatype = pdf['datatype'][0]
            num = pdf['num'][0]
            pdf['tmp1'] = (pdf['y_prd'] - pdf['y_true']) **2
            tmp1_mean = pdf['tmp1'].mean()
            y_true_mean = pdf['y_true'].mean()
            pdf['tmp2'] = (pdf['y_true'] - y_true_mean) **2
            tmp2_mean = pdf['tmp2'].mean()
            if tmp2_mean == 0:
                NMSE = 'inf'
            NMSE = str(tmp1_mean/tmp2_mean)
            return pd.DataFrame([[Province] + [datatype] + [NMSE] + [str(num)]], columns=["Province", "datatype", "NMSE", "num"])
    
    
        df_all = df_all.withColumn('y_prd', f2(col('prediction'))) \
                .withColumn('y_true', f2(col('label'))).persist()
        df_nmse = df_all.select('Province', 'y_true', 'y_prd', 'datatype', 'num') \
                        .groupBy('Province', 'datatype', 'num').apply(nmse_func).persist()
        df_nmse = df_nmse.withColumn('NMSE', col('NMSE').cast(DoubleType())) \
                        .withColumn('type', func.concat(col('datatype'), col('num')))
    
        # 转置
        df_nmse = df_nmse.groupBy('Province').pivot('type').agg(func.sum('NMSE'))
    
        df_nmse = df_nmse.repartition(1)
        df_nmse.write.format("csv").option("header", "true") \
                .mode("overwrite").save(df_nmse_path)
    
        '''


def job2(kwargs):
    # ### input args ###
    max_path = kwargs.get('max_path', 'D:/Auto_MAX/MAX/')
    project_name = kwargs['project_name']
    outdir = kwargs['outdir']
    model_month_right = kwargs['model_month_right']
    model_month_left = kwargs['model_month_left']
    all_models = kwargs['all_models']
    max_file = kwargs['max_file']
    factor_optimize = kwargs.get('factor_optimize', 'True')
    for_nh_model = kwargs.get('for_nh_model', 'False')
    # ### input args ###
    '''
    max_path = 'MAX'
    project_name = '神州'
    outdir = '202012'
    model_month_right = '201912'
    model_month_left = '201901'
    all_models = 'SZ1'
    max_file = 'MAX_result_201801_201912_city_level'
    factor_optimize = 'True'
    '''
    
    
    from pyspark.sql import SparkSession, Window
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    import os
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col
    import time
    import re        # %%
    
    # project_name = 'Sanofi'
    # outdir = '202012'
    # model_month_right = '202012'
    # model_month_left = '202001'
    # all_models = 'SNY15,SNY16,SNY17'
    # max_file = 'MAX_result_201801_202012_city_level'

    spark = SparkSession.builder.getOrCreate()

    # %%
    # 输出
    model_month_right = int(model_month_right)
    model_month_left = int(model_month_left)
    all_models = all_models.replace(' ','').split(',')
    
    mkt_mapping_path = max_path + '/' + project_name + '/mkt_mapping'
    universe_path = max_path + '/' + project_name + '/universe_base'
    max_result_path = max_path + '/' + project_name + '/' + outdir + '/MAX_result/' + max_file
    #panel_result_path = max_path + '/' + project_name + '/' + outdir + '/panel_result'
    if for_nh_model == 'True':
        NH_in_old_universe_path = max_path + "/Common_files/NH_in_old_universe"
        df_NH_in_old_universe = spark.read.parquet(NH_in_old_universe_path).select('Panel_ID').withColumnRenamed('Panel_ID','PHA_ID').distinct()

    # %%
    # =========== 数据执行 ============
    print("job2_factor_raw")
    mkt_mapping = spark.read.parquet(mkt_mapping_path)
    universe = spark.read.parquet(universe_path)
    
    max_result = spark.read.parquet(max_result_path)
    max_result = max_result.where((col('Date') >= model_month_left) & (col('Date') <= model_month_right))
    
    #panel_result = spark.read.parquet(panel_result_path)
    #panel_result = panel_result.where((col('Date') >= model_month_left) & (col('Date') <= model_month_right))
    
    # 每个市场算 factor
    for market in all_models:
        # logger.debug("当前market为:" + str(market))
        #market = '固力康'
        # 输入
        rf_out_path = max_path + '/' + project_name + '/forest/' + market + '_rf_result'
        # 输出
        if factor_optimize == 'True':
            factor1_path = max_path + '/' + project_name + '/forest/' + market + '_factor_1'
        else:
            factor_out_path = max_path + '/' + project_name + '/factor/factor_' + market
    
        # 样本ID
        ID_list = universe.where(col('PANEL') == 1).select('Panel_ID').distinct().toPandas()['Panel_ID'].values.tolist()
    
        # panel 样本
        '''
        panel = panel_result.where(col('DOI') == market)
        panel1 = panel.where(col('HOSP_ID').isin(ID_list)) \
                    .drop('Province', 'City') \
                    .join(universe.select('Panel_ID', 'Province', 'City'), panel.HOSP_ID == universe.Panel_ID, how='inner')
        panel1 = panel1.groupBy('City').agg(func.sum('Sales').alias('panel_sales'))
        '''
    
        # rf 非样本
        rf_out = spark.read.parquet(rf_out_path)
        if for_nh_model == 'True':
            rf_out = rf_out.join(df_NH_in_old_universe, on='PHA_ID', how='inner')

        rf_out = rf_out.select('PHA_ID', 'final_sales') \
                        .join(universe.select('Panel_ID', 'Province', 'City').distinct(), 
                                rf_out.PHA_ID == universe.Panel_ID, how='left') \
                        .where(~col('PHA_ID').isin(ID_list))
        rf_out = rf_out.groupBy('City', 'Province').agg(func.sum('final_sales').alias('Sales_rf'))
    
        # max 非样本
        spotfire_out = max_result.where(col('DOI') == market)
        spotfire_out = spotfire_out.where(col('PANEL') != 1) \
                                .groupBy('City', 'Province').agg(func.sum('Predict_Sales').alias('Sales'))
    
        # 计算factor 城市层面 ： rf 非样本的Sales 除以  max 非样本 的Sales                
        factor_city = spotfire_out.join(rf_out, on=['City', 'Province'], how='left')
        factor_city = factor_city.withColumn('factor', col('Sales_rf')/col('Sales'))
    
        # universe join left factor_city 没有的城市factor为1
        factor_city1 = universe.select('City', 'Province').distinct() \
                                .join(factor_city, on=['City', 'Province'], how='left')
        factor_city1 = factor_city1.withColumn('factor', func.when(((col('factor').isNull()) | (col('factor') <=0)), func.lit(1)) \
                                                            .otherwise(col('factor')))
        factor_city1 = factor_city1.withColumn('factor', func.when(col('factor') >4, func.lit(4)) \
                                                            .otherwise(col('factor')))
    
        factor_out = factor_city1.select('City', 'Province', 'factor')
    
    
        if factor_optimize == 'True':
            factor_out = factor_out.repartition(1)
            factor_out.write.format("parquet") \
                .mode("overwrite").save(factor1_path)
        else:
            factor_out = factor_out.repartition(1)
            factor_out.write.format("parquet") \
                .mode("overwrite").save(factor_out_path)        
    
        
        # logger.debug("finish:" + str(market))


def job3(kwargs):
    # ### input args ###
    max_path = kwargs.get('max_path', 'D:/Auto_MAX/MAX/')
    project_name = kwargs['project_name']
    outdir = kwargs['outdir']
    model_month_right = kwargs['model_month_right']
    model_month_left = kwargs['model_month_left']
    all_models = kwargs['all_models']
    max_file = kwargs['max_file']
    test = kwargs.get('test', 'False')
    ims_info_auto = kwargs.get('ims_info_auto', 'True')
    ims_version = kwargs.get('ims_version', 'Empty')
    add_imsinfo_path = kwargs.get('add_imsinfo_path', 'Empty')
    geo_map_path = kwargs.get('geo_map_path', 'Empty')
    factor_optimize = kwargs.get('factor_optimize', 'True')
    # ### input args ###
    '''
    max_path = 'MAX'
    project_name = '神州'
    outdir = '202012'
    model_month_right = '201912'
    model_month_left = '201901'
    all_models = 'SZ1'
    max_file = 'MAX_result_201801_201912_city_level'
    test = 'False'
    ims_info_auto = 'False'
    ims_version = 'Empty'
    add_imsinfo_path = 'Empty'
    geo_map_path = 'Empty'
    factor_optimize = 'True'
    '''

    from pyspark.sql import SparkSession, Window
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col
    import os
    import time
    import re
    from copy import deepcopy    
    # %%

    spark = SparkSession.builder.getOrCreate()

    '''
    project_name = 'Takeda'
    outdir = '202012'
    model_month_right = '202012'
    model_month_left = '202001'
    all_models = 'TK1'
    max_file = 'MAX_result_201801_202012_city_level'
    test = 'True'
    ims_version = '202012'
    add_imsinfo_path = 's3a://ph-max-auto/v0.0.1-2020-06-08/Takeda/add_ims_info.csv'
    geo_map_path = 's3a://ph-max-auto/v0.0.1-2020-06-08/Takeda/geo_map.csv'
    
    project_name = '神州'
    outdir = '201912'
    model_month_right = '201912'
    model_month_left = '201901'
    all_models = 'SZ1'
    max_file = 'MAX_result_201801_201912_city_level'
    test = 'True'
    ims_version = '202010'
    '''
    # %%
    # 输入
    if factor_optimize != "True":
         raise ValueError('不进行优化')
            
    if test != "False" and test != "True":
        # logger.debug('wrong input: test, False or True')
        raise ValueError('wrong input: test, False or True')
    if ims_info_auto != "False" and ims_info_auto != "True":
        # logger.debug('wrong input: test, False or True')
        raise ValueError('wrong input: test, False or True')
    
    model_month_right = int(model_month_right)
    model_month_left = int(model_month_left)
    all_models = all_models.replace(' ','').split(',')
    
    max_result_path = max_path + '/' + project_name + '/' + outdir + '/MAX_result/' + max_file
    product_map_path = max_path + '/' + project_name + '/' + outdir + '/prod_mapping'
    mkt_map_path = max_path + '/' + project_name + '/mkt_mapping'
    
    if ims_info_auto == 'True':
        ims_mapping_path = max_path + '/' + '/Common_files/IMS_flat_files/' + ims_version + '/ims_mapping_' + ims_version + '.csv'
        ims_sales_path = max_path + '/Common_files/IMS_flat_files/' + ims_version + '/cn_IMS_Sales_Fdata_' + ims_version + '_1.csv'
        if geo_map_path == 'Empty':
            geo_map_path = max_path + '/' + '/Common_files/IMS_flat_files/' + ims_version + '/cn_geog_dimn_' + ims_version + '_1.csv'
        # 输出检查文件
        check_path = max_path + '/' + project_name + '/ims_info/ims_info_check'
        ims_sales_all_path = max_path + '/' + project_name + '/ims_info/ims_info_all'
            
    # 输出在每个循环下

    # %%
    # =========== 数据执行 ============
    print("job3_factor_optimize")
    # 1. max 文件处理
    max_result = spark.read.parquet(max_result_path)
    max_result = max_result.where((col('Date') >= model_month_left) & (col('Date') <= model_month_right))
    
    # 2. product_map 文件处理
    product_map = spark.read.parquet(product_map_path)
    #if project_name == "Sanofi" or project_name == "AZ":
    #    if "pfc" not in product_map.columns:
    #        product_map = product_map.withColumnRenamed([i for i in product_map.columns if 'pfc' in i][0], "pfc")
    #if project_name == "Eisai":
    #    if "pfc" not in product_map.columns:
    #        product_map = product_map.withColumnRenamed([i for i in product_map.columns if 'pfc' in i][0], "pfc")
    for i in product_map.columns:
        if i in ["标准通用名", "通用名_标准", "药品名称_标准", "S_Molecule_Name"]:
            product_map = product_map.withColumnRenamed(i, "通用名")
        if i in ["min1_标准"]:
            product_map = product_map.withColumnRenamed(i, "min2")
        if i in ["packcode", "Pack_ID", "Pack_Id", "PackID", "packid"]:
            product_map = product_map.withColumnRenamed(i, "pfc")
        if i in ["商品名_标准", "S_Product_Name"]:
            product_map = product_map.withColumnRenamed(i, "标准商品名")
        if i in ["剂型_标准", "Form_std", "S_Dosage"]:
            product_map = product_map.withColumnRenamed(i, "标准剂型")
        if i in ["规格_标准", "Specifications_std", "药品规格_标准", "S_Pack"]:
            product_map = product_map.withColumnRenamed(i, "标准规格")
        if i in ["包装数量2", "包装数量_标准", "Pack_Number_std", "S_PackNumber", "最小包装数量"]:
            product_map = product_map.withColumnRenamed(i, "标准包装数量")
        if i in ["标准企业", "生产企业_标准", "Manufacturer_std", "S_CORPORATION", "标准生产厂家"]:
            product_map = product_map.withColumnRenamed(i, "标准生产企业")
    if project_name == "Janssen" or project_name == "NHWA":
        if "标准剂型" not in product_map.columns:
            product_map = product_map.withColumnRenamed("剂型", "标准剂型")
        if "标准规格" not in product_map.columns:
            product_map = product_map.withColumnRenamed("规格", "标准规格")
        if "标准生产企业" not in product_map.columns:
            product_map = product_map.withColumnRenamed("生产企业", "标准生产企业")
        if "标准包装数量" not in product_map.columns:
            product_map = product_map.withColumnRenamed("包装数量", "标准包装数量")
    product_map = product_map.withColumn("min2", func.regexp_replace("min2", "&amp;", "&")) \
                            .withColumn("min2", func.regexp_replace("min2", "&lt;", "<")) \
                            .withColumn("min2", func.regexp_replace("min2", "&gt;", ">"))
    
    product_map = product_map.select('通用名', '标准商品名', '标准剂型', '标准规格', '标准包装数量', 
                                        '标准生产企业', 'min2', 'pfc') \
                            .distinct() \
                            .withColumnRenamed('通用名', 'Molecule') \
                            .withColumnRenamed('标准商品名', 'Brand') \
                            .withColumnRenamed('标准剂型', 'Form') \
                            .withColumnRenamed('标准规格', 'Specifications') \
                            .withColumnRenamed('标准包装数量', 'Pack_Number') \
                            .withColumnRenamed('标准生产企业', 'Manufacturer') \
                            .withColumn('pfc', product_map.pfc.cast(IntegerType())) \
                            .withColumnRenamed('pfc', 'Pack_ID')

    # %%
    # 3. mkt_map_path
    mkt_map = spark.read.parquet(mkt_map_path)
    mkt_map = mkt_map.withColumnRenamed("标准通用名", "通用名") \
        .withColumnRenamed("model", "mkt") \
        .select("mkt", "通用名").distinct()
    model_pfc = product_map.where(~col('Pack_ID').isNull()).select('Molecule', 'Pack_ID').distinct() \
                        .join(mkt_map, product_map['Molecule']==mkt_map['通用名'], how='left')
    model_pfc = model_pfc.select('Pack_ID', 'mkt').distinct()

    # %%
    # 4. ims文件
    if ims_info_auto == 'True':
        @udf(StringType())
        def city_change(name):
            # 城市名定义
            if name in ["苏锡城市群"]:
                newname = "苏锡市"
            elif name in ["全国"]:
                newname = "CHPA"
            else:
                newname = name + '市'
            return newname
    
        # geo_map_path 匹配城市中文名
        geo_map = spark.read.csv(geo_map_path, header=True)
        geo_map = geo_map.select('GEO_CD', 'GEO_DESC_CN').distinct() \
                        .withColumnRenamed('GEO_CD', 'Geography_id')
        # ims_mapping 匹配产品英文信息
        ims_mapping = spark.read.csv(ims_mapping_path, header=True)
        ims_mapping = ims_mapping.select('Molecule_Composition', 'Prd_desc', 'Pack_Id0').distinct() \
                                .withColumnRenamed('Pack_Id0', 'PACK_ID')
        # ims 销量数据
        ims_sales = spark.read.csv(ims_sales_path, header=True)
        ims_sales = ims_sales.select('Geography_id', 'Pack_ID', 'Period_Code', 'LC')
    
        # 是否补充ims 销量数据
        if add_imsinfo_path != 'Empty':
            add_imsinfo_file = spark.read.csv(add_imsinfo_path, header=True)
            add_imsinfo_file = add_imsinfo_file.select('Geography_id', 'Pack_ID', 'Period_Code', 'LC')
            # 去掉add_imsinfo_file中有的
            ims_sales_keep = ims_sales.join(add_imsinfo_file, on=["Pack_ID", "Geography_id"], how='left_anti')
            ims_sales = ims_sales_keep.union(add_imsinfo_file.select(ims_sales_keep.columns))
    
        # 信息匹配
        ims_sales = ims_sales.join(ims_mapping, on='Pack_ID', how='left') \
                            .join(model_pfc, on='Pack_ID', how='left') \
                            .join(geo_map, on='Geography_id', how='left')
    
        ims_sales = ims_sales.withColumn('Date', func.regexp_replace('Period_Code', 'M', '')) \
                            .withColumn('City', city_change(col('GEO_DESC_CN'))) \
                            .where(col('Date').between(model_month_left, model_month_right))
    
        # 检查文件同一个分子是否有没匹配上的packid
        check = ims_sales.where(col('City') == 'CHPA') \
                        .groupby('Molecule_Composition', 'mkt').agg(func.sum('LC').alias('Sales'))
        check_mol = check.groupby('Molecule_Composition').agg(func.sum('Sales').alias('Sales_mol'))
        check = check.join(check_mol, on='Molecule_Composition', how='left')
        check = check.withColumn('share', col('Sales')/col('Sales_mol'))
        molecules_in_model = check.where(~col('mkt').isNull()).select('Molecule_Composition').distinct()
        check = check.join(molecules_in_model, on='Molecule_Composition', how='inner')
    
        # 根据分子名重新匹配市场名，补充上缺失的数据
        ims_sales_all = ims_sales.drop('mkt') \
                                .join(check.where(~col('mkt').isNull()).select("Molecule_Composition","mkt").distinct(), 
                                      on='Molecule_Composition', how='left')
        # 计算share
        ims_sales_all = ims_sales_all.groupby('City', 'Pack_ID', 'Prd_desc', 'Molecule_Composition', 'mkt') \
                                    .agg(func.sum('LC').alias('ims_poi_vol'))
        ims_sales_tmp = ims_sales_all.groupby('City', 'mkt') \
                                    .agg(func.sum('ims_poi_vol').alias('ims_poi_vol_all'))
        ims_sales_all = ims_sales_all.join(ims_sales_tmp, on=['City', 'mkt'], how='left') \
                                    .withColumn('ims_share', col('ims_poi_vol')/col('ims_poi_vol_all'))
        # 整理
        ims_sales_all = ims_sales_all.drop("ims_poi_vol_all") \
                                    .withColumnRenamed("mkt", "model") \
                                    .where(~col('model').isNull())
    
        # 输出检查文件
        check = check.repartition(1)
        check.write.format("parquet") \
                .mode("overwrite").save(check_path)
        
        # 输出 ims_sales_all
        ims_sales_all = ims_sales_all.repartition(2)
        ims_sales_all.write.format("parquet") \
                .mode("overwrite").save(ims_sales_all_path)
        
        ims_sales_all = spark.read.parquet(ims_sales_all_path)

    # %%
    # 3. 对每个市场优化factor
    @udf(StringType())
    def city_change(name):
        # 城市名定义
        if name in ["福州市", "厦门市", "泉州市"]:
            newname = "福厦泉市"
        elif name in ["珠海市", "东莞市", "中山市", "佛山市"]:
            newname = "珠三角市"
        elif name in ["绍兴市", "嘉兴市", "台州市", "金华市"]:
            newname = "浙江市"
        elif name in ["苏州市", "无锡市"]:
            newname = "苏锡市"
        else:
            newname = name
        return newname
    
    def unpivot(df, keys):
        # 功能：数据宽变长
        # 参数说明 df:dataframe,  keys 待转换表中需要保留的主键key，以list[]类型传入
        # 转换是为了避免字段类不匹配，统一将数据转换为string类型，如果保证数据类型完全一致，可以省略该句
        df = df.select(*[col(_).astype("string") for _ in df.columns])
        cols = [_ for _ in df.columns if _ not in keys]
        stack_str = ','.join(map(lambda x: "'%s', `%s`" % (x, x), cols))
        # feature, value 转换后的列名，可自定义
        df = df.selectExpr(*keys, "stack(%s, %s) as (feature, value)" % (len(cols), stack_str))
        return df
    
    # market = '固力康'
    for market in all_models:
        # logger.debug("当前market为:" + str(market))
        # 输入
        if ims_info_auto != 'True':     
            ims_info_path = max_path + '/' + project_name + '/ims_info/' +  market + '_ims_info'
        factor_path = max_path + '/' + project_name + '/forest/' + market + '_factor_1'
        # 输出
        ims_v1_otherall_path = max_path + '/' + project_name + '/forest/' + market + '_top3_product.csv'
        ims_panel_max_out_path = max_path + '/' + project_name + '/forest/' + market + '_factor_gap.csv'
        if test == 'True':
            factor_out_path = max_path + '/' + project_name + '/forest/factor/factor_' + market
        else:
            factor_out_path = max_path + '/' + project_name + '/factor/factor_' + market
    
        # 数据执行
        if ims_info_auto != 'True':
            ims_info = spark.read.parquet(ims_info_path)
            ims_info = ims_info.withColumnRenamed('city', 'City')
        else:
            ims_info = ims_sales_all.where(col('model') == market)
    
        factor = spark.read.parquet(factor_path)
    
        # 3.1 max 数据
        max_df = max_result.where(col('DOI') == market)
        max_df = max_df.join(factor, on='City', how='left')
        max_df = max_df.withColumn('Predict_Sales', func.when(col('PANEL') == 0, col('factor')*col('Predict_Sales')) \
                                                        .otherwise(col('Predict_Sales'))) \
                        .withColumn('Citynew', col('City'))
    
        max_df = max_df.withColumn('Citynew', city_change(col('City')))
        max_df = max_df.withColumn('Citynew', func.when(~col('Citynew').isin(ims_info.select('City').distinct().toPandas()['City'].values.tolist()), 
                                                        func.lit('other')).otherwise(col('Citynew')))
    
        max_df = max_df.join(product_map.dropDuplicates(['min2']), max_df.Prod_Name==product_map.min2, how='left')
    
        # 城市产品层面
        max2 = max_df.groupBy('Brand', 'Citynew').agg(func.sum('Predict_Sales').alias('max_Prod')) \
                    .withColumn('mkt', func.lit(market)) \
                    .withColumnRenamed('Citynew', 'City')
        # 全国的市场
        max3 = max_df.groupBy('Citynew').agg(func.sum('Predict_Sales').alias('max_mkt')) \
                    .withColumn('Market', func.lit(market)) \
                    .withColumnRenamed('Citynew', 'City')
    
        # 3.2 panel数据
        bll1 = max_df.where(max_df.PANEL == 1)
        # 城市产品层面
        bll2 = bll1.groupBy('Brand', 'Citynew').agg(func.sum('Predict_Sales').alias('panel_Sales')) \
                    .withColumn('mkt', func.lit(market)) \
                    .withColumnRenamed('Citynew', 'City')
        # 全国的市场
        bll3 = bll1.groupBy('Citynew').agg(func.sum('Predict_Sales').alias('panel_mkt')) \
                    .withColumn('Market', func.lit(market)) \
                    .withColumnRenamed('Citynew', 'City')
    
        # 3.3 确定IMS三个最大的产品
        ims_df = ims_info.withColumn('Market', func.lit(market))
    
        ims_df = ims_df.withColumn('Brand_en', col('Prd_desc')) \
                            .join(product_map.select('Pack_ID', 'Brand').dropDuplicates(['Pack_ID']), on='Pack_ID', how='left')
    
        ims_df = ims_df.groupBy('Market', 'Brand', 'City').agg(func.sum('ims_poi_vol').alias('Prod')) \
                        .withColumn('Brand', func.when(col('Brand').isNull(), func.lit('none')).otherwise(col('Brand')))
    
        # 全国的商品销量
        ims_part1 = ims_df.where(col('City') == 'CHPA')
        ims_part1_v1 = ims_part1.groupBy('Market', 'Brand').agg(func.sum('Prod').alias('Prod_CHPA'))
        # 非other 省份的商品销量
        ims_part2 = ims_df.where(col('City') != 'CHPA')
        ims_part2_v1 = ims_part2.groupBy('Market', 'Brand').agg(func.sum('Prod').alias('Prod_nonCHPA'))
        # other 省份的商品销量
        ims_v1_left = ims_part1_v1.join(ims_part2_v1, on=['Market','Brand'], how='left')
        ims_v1_left = ims_v1_left.fillna(0, 'Prod_nonCHPA') \
                                .withColumn('Prod_other', col('Prod_CHPA')-col('Prod_nonCHPA')) \
                                .withColumn('City', func.lit('other'))
        ims_v1_left = ims_v1_left.withColumnRenamed('Prod_CHPA', 'CHPA') \
                                .withColumnRenamed('Prod_nonCHPA', 'IMS_City') \
                                .withColumnRenamed('Prod_other', 'Prod') \
                                .select('Market','Brand','City','Prod')
        # other + 非other省份 每个城市的销量前三名产品
        ims_v1_otherall = ims_part2.union(ims_v1_left)
        ims_v1_otherall = ims_v1_otherall.withColumn('n',
                                func.row_number().over(Window.partitionBy('Market', 'City').orderBy(col('Prod').desc())))
        ims_v1_otherall = ims_v1_otherall.where(col('n') < 4) \
                                        .orderBy(col('Market'), col('City'), col('Prod').desc()) \
                                        .withColumnRenamed('Prod', 'ims_Prod').persist()
    
        ims_v1_mkt =  ims_part2.union(ims_v1_left) \
                                .groupBy('Market','City').agg(func.sum('Prod').alias('ims_mkt'))
    
        ims_panel_max = ims_v1_otherall.join(max2.withColumnRenamed('mkt', 'Market'), 
                                            on=['Market', 'Brand', 'City'], how='left') \
                                        .join(bll2.withColumnRenamed('mkt', 'Market'),
                                            on=['Market', 'Brand', 'City'], how='left')
        ims_panel_max = unpivot(ims_panel_max, ['Market', 'Brand', 'City', 'n'])
        ims_panel_max = ims_panel_max.withColumn('value', col('value').cast(DoubleType())) \
                                    .withColumn('new', func.concat(col('feature'), func.lit('_'), col('n'))) \
                                    .drop('Brand', 'feature', 'n')
        ims_panel_max = ims_panel_max.groupBy('Market', 'City').pivot('new').agg(func.sum('value')).fillna(0).persist()
        ims_panel_max = ims_panel_max.join(ims_v1_mkt, on=['Market','City'], how='left') \
                                    .join(max3, on=['Market','City'], how='left') \
                                    .join(bll3, on=['Market','City'], how='left')
        ims_panel_max = ims_panel_max.fillna(0, ims_panel_max.columns[2:])
    
        if 'ims_Prod_2' not in ims_panel_max.columns:
            ims_panel_max = ims_panel_max.withColumn('ims_Prod_2', func.lit(0))
        if 'max_Prod_2' not in ims_panel_max.columns:
            ims_panel_max = ims_panel_max.withColumn('max_Prod_2', func.lit(0))
        if 'panel_Sales_2' not in ims_panel_max.columns:
            ims_panel_max = ims_panel_max.withColumn('panel_Sales_2', func.lit(0))
        if 'ims_Prod_3' not in ims_panel_max.columns:
            ims_panel_max = ims_panel_max.withColumn('ims_Prod_3', func.lit(0))
        if 'max_Prod_3' not in ims_panel_max.columns:
            ims_panel_max = ims_panel_max.withColumn('max_Prod_3', func.lit(0))
        if 'panel_Sales_3' not in ims_panel_max.columns:
            ims_panel_max = ims_panel_max.withColumn('panel_Sales_3', func.lit(0))
    
        # 3.4 优化 
        # logger.debug("cvxpy优化")
        schema = deepcopy(ims_panel_max.schema)
        schema.add("factor", DoubleType())
        @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
        def cvxpy_func(pdf):
            import numpy as np
            import cvxpy as cp
    
            f = cp.Variable()
            prob = cp.Problem(cp.Minimize(cp.maximum(cp.abs((f*(pdf['max_Prod_1'][0]-
                                              pdf['panel_Sales_1'][0])+
                                           pdf['panel_Sales_1'][0])/pdf['ims_Prod_1'][0]-1),
                                    cp.abs((f*(pdf['max_Prod_2'][0]-
                                              pdf['panel_Sales_2'][0])+
                                           pdf['panel_Sales_2'][0])/pdf['ims_Prod_2'][0]-1),
                                    cp.abs((f*(pdf['max_Prod_3'][0]-
                                              pdf['panel_Sales_3'][0])+
                                           pdf['panel_Sales_3'][0])/pdf['ims_Prod_3'][0]-1),
                                    cp.abs((f*(pdf['max_mkt'][0]-
                                              pdf['panel_mkt'][0])+
                                           pdf['panel_mkt'][0])/pdf['ims_mkt'][0]-1))),
                       [cp.abs((f*(pdf['max_mkt'][0]-pdf['panel_mkt'][0])+
                                   pdf['panel_mkt'][0])/pdf['ims_mkt'][0]-1) <= 0.05])
            prob.solve(solver = cp.ECOS)
    
            return pdf.assign(factor = f.value)
    
        ims_panel_max_out = ims_panel_max.groupby('Market','City').apply(cvxpy_func)
        ims_panel_max_out = ims_panel_max_out.withColumn('factor', 
                                                func.when((col('factor').isNull()) | (col('factor') < 0 ), func.lit(0)) \
                                                    .otherwise(col('factor')))
    
        ims_panel_max_out = ims_panel_max_out.withColumn('gap1', 
                (col('factor')*(col('max_Prod_1') - col('panel_Sales_1')) + col('panel_Sales_1')) / col('ims_Prod_1') -1)
        ims_panel_max_out = ims_panel_max_out.withColumn('gap2', 
                (col('factor')*(col('max_Prod_2') - col('panel_Sales_2')) + col('panel_Sales_2')) / col('ims_Prod_2') -1)
        ims_panel_max_out = ims_panel_max_out.withColumn('gap3', 
                (col('factor')*(col('max_Prod_3') - col('panel_Sales_3')) + col('panel_Sales_3')) / col('ims_Prod_3') -1)
        ims_panel_max_out = ims_panel_max_out.withColumn('gap_mkt', 
                (col('factor')*(col('max_mkt') - col('panel_mkt')) + col('panel_mkt')) / col('ims_mkt') -1)
    
        # 输出
        
        ims_v1_otherall = ims_v1_otherall.repartition(1)
        ims_v1_otherall.write.format("csv").option("header", "true") \
                .mode("overwrite").save(ims_v1_otherall_path)
    
        ims_panel_max_out = ims_panel_max_out.repartition(1)
        ims_panel_max_out.write.format("csv").option("header", "true") \
                .mode("overwrite").save(ims_panel_max_out_path)
        
        factor1 = spark.read.parquet(factor_path)
        factor1 = factor1.withColumnRenamed('factor', 'factor1')
        factor2 = ims_panel_max_out.select('City','factor').withColumnRenamed('factor', 'factor2')
        factor3 = factor1.join(factor2, on='City', how='left')
    
        factor2_city = factor2.select('City').distinct().toPandas()['City'].values.tolist()
    
        if "福厦泉市" in factor2_city:
            value = factor2.where(col('City')=='福厦泉市').select('factor2').toPandas()['factor2'][0]
            factor3 = factor3.withColumn('factor2', func.when(col('City').isin("福州市","厦门市","泉州市"), func.lit(value)) \
                                                        .otherwise(col('factor2')))
    
        if "珠三角市" in factor2_city:
            value2 = factor2.where(col('City')=='珠三角市').select('factor2').toPandas()['factor2'][0]
            factor3 = factor3.withColumn('factor2', func.when(col('City').isin("珠海市","东莞市","中山市","佛山市"), 
                                                        func.lit(value2)).otherwise(col('factor2')))
    
        if "浙江市" in factor2_city:
            value3 = factor2.where(col('City')=='浙江市').select('factor2').toPandas()['factor2'][0]
            factor3 = factor3.withColumn('factor2', func.when(col('City').isin("绍兴市","嘉兴市","台州市","金华市"), 
                                                        func.lit(value3)).otherwise(col('factor2')))
    
        if "苏锡市" in factor2_city:
            value4 = factor2.where(col('City')=='苏锡市').select('factor2').toPandas()['factor2'][0]
            factor3 = factor3.withColumn('factor2', func.when(col('City').isin("苏州市","无锡市"), 
                                                        func.lit(value4)).otherwise(col('factor2')))
    
        value_other = factor2.where(col('City')=='other').select('factor2').toPandas()['factor2'][0]    
        factor3 = factor3.withColumn('factor2', func.when(col('factor2').isNull(), func.lit(value_other)) \
                                                    .otherwise(col('factor2')))
    
        factor3 = factor3.withColumn('factor', col('factor1') * col('factor2'))
        factor3 = factor3.withColumn('factor', func.when(col('factor') > 4, func.lit(4)).otherwise(col('factor')))
        
        factor3 = factor3.repartition(1)
        factor3.write.format("parquet") \
                .mode("overwrite").save(factor_out_path)
        
        print("finish:" + str(market))
    print('数据执行-Finish')