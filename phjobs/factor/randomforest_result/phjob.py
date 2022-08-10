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
    model_month_right = kwargs['model_month_right']
    model_month_left = kwargs['model_month_left']
    all_models = kwargs['all_models']
    universe_choice = kwargs['universe_choice']
    rf_ntree = kwargs['rf_ntree']
    rf_minnode = kwargs['rf_minnode']
    g_input_version = kwargs['g_input_version']
    ### input args ###
    
    ### output args ###
    run_id = kwargs['run_id'].replace(":","_")
    owner = kwargs['owner']
    project_name = kwargs['project_name']
    
    g_database_temp = "phdatatemp"
    p_out = "s3://ph-platform/2020-11-11/etl/temporary_files/"
    out_mode = "append"
    ### output args ###

    
    
    from pyspark.sql import SparkSession, Window
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    import os
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col
    import time
    import re
    from phcli.ph_tools.addTable.addTableToGlue import AddTableToGlue
    
    from pyspark.ml import Pipeline
    from pyspark.ml.regression import RandomForestRegressor
    from pyspark.ml.feature import VectorIndexer, StringIndexer
    from pyspark.ml.linalg import DenseVector
    from pyspark.ml.evaluation import RegressionEvaluator
    from pyspark.ml.feature import VectorAssembler
    import pandas as pd

    # %%
    # =========== 参数处理 =========== 
    rf_minnode = int(rf_minnode)
    rf_ntree = int(rf_ntree)
    model_month_right = int(model_month_right)
    model_month_left = int(model_month_left)
    all_models = all_models.replace(' ','').split(',')
    
    # 市场的universe文件
    def getVersionDict(str_choice):
        dict_choice = {}
        if str_choice != "Empty":
            for each in str_choice.replace(", ",",").split(","):
                market_name = each.split(":")[0]
                version_name = each.split(":")[1]
                dict_choice[market_name]=version_name
        return dict_choice
    
    dict_universe_choice = getVersionDict(universe_choice)
            
    g_table_result = 'rf_result'
    
    # ============== 删除已有的s3中间文件 =============
    import boto3
    def deletePath(path_dir):
        file_name = path_dir.replace('//', '/').split('s3:/ph-platform/')[1]
        s3 = boto3.resource('s3', region_name='cn-northwest-1',
                            aws_access_key_id="AKIAWPBDTVEAEU44ZAGT",
                            aws_secret_access_key="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")
        bucket = s3.Bucket('ph-platform')
        bucket.objects.filter(Prefix=file_name).delete()
    deletePath(path_dir=f"{p_out + g_table_result}/version={run_id}/provider={project_name}/owner={owner}/")
    
    
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
        df = dealScheme(df, dict_scheme)
        df = getInputVersion(df, table_name.replace('df_', ''))
        return df
    
    def getUniverse(market, dict_universe_choice):
        if market in dict_universe_choice.keys():
            df_universe = readInFile('df_universe_other').where(col('version')==dict_universe_choice[market])
        else:
            df_universe = readInFile('df_universe_base')
        return df_universe
    
    df_raw_data = readInFile('df_max_raw_data')

    df_product_map = readInFile('df_prod_mapping')
    
    df_cpa_pha_mapping = readInFile('df_cpa_pha_mapping')
    
    df_mkt_mapping = readInFile('df_mkt_mapping')
    
    df_BT_PHA = readInFile('df_bt_pha')
    
    df_doctor = readInFile('df_doctor')
    
    df_ind = readInFile('df_ind')
       
    # 列名有点的无法识别，把点换成_
    # df_ind = df_ind.toDF(*(re.sub(r'[\.\s]+', '_', c) for c in df_ind.columns))
       

    # %%
    # ===========  数据执行  ============
    def deal_ID_length(df):
        # ID不足7位的补足0到6位
        # 国药诚信医院编码长度是7位数字，cpa医院编码是6位数字，其他还有包含字母的ID
        df = df.withColumn("id", df["id"].cast(StringType()))
        # 去掉末尾的.0
        df = df.withColumn("id", func.regexp_replace("id", "\\.0", ""))
        df = df.withColumn("id", func.when(func.length(col('id')) < 7, func.lpad(col('id'), 6, "0")).otherwise(col('id')))
        return df
    
    def cleanUniverse(df_universe):
        dict_cols_universe = {"city_tier_2010":['city_tier', 'citygroup', 'city_tier_2010'], "pha":['panel_id', 'pha']}
        df_universe = getTrueColRenamed(df_universe, dict_cols_universe, df_universe.columns)
        df_universe = df_universe.select('pha', 'city', 'province', 'city_tier_2010', 'hosp_name', 'panel', 'bedsize', 'seg', 'est_drugincome_rmb').distinct()
        return df_universe
    
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
    
    # 1. ============ 文件准备 ============
    
    # 1.1  doctor 文件    
    df_doctor = df_doctor.where(~col('bt_code').isNull()) \
                    .select(['department', 'dr_n', 'dr_n_主任', 'dr_n_副主任', 'dr_n_主治', 'dr_n_住院医', 'bt_code'])
    df_doctor_g = unpivot(df_doctor, ['bt_code', 'department']).persist()
    df_doctor_g = df_doctor_g.withColumn('dr1', func.concat(col('department'), func.lit('_'), col('feature'))) \
                        .groupBy('bt_code', 'dr1').agg(func.sum('value'))
    df_doctor_g = df_doctor_g.groupBy('bt_code').pivot('dr1').sum('sum(value)').persist()
    
    # 1.2 BT_PHA 文件
    df_BT_PHA = df_BT_PHA.select('bt', 'pha').dropDuplicates(['pha'])
    
    # 1.3 ind 文件
    for each in df_ind.columns[18:]:
        df_ind = df_ind.withColumn(each, df_ind[each].cast(DoubleType()))
    
    # 1.4 cpa_pha_mapping 文件
    df_cpa_pha_mapping = deal_ID_length(df_cpa_pha_mapping)
    
    # 1.5 product_map 文件
    df_product_map = df_product_map.select('molecule','标准通用名').distinct()
    
    # 1.6 mkt_mapping 文件
    df_mkt_mapping = df_mkt_mapping.withColumnRenamed('mkt', 'market') \
                        .withColumnRenamed("model", "market") \
                        .select('market', '标准通用名').distinct()
    df_molecule_mkt_map = df_product_map.join(df_mkt_mapping, on='标准通用名', how='left')
    # molecule_mkt_map.where(molecule_mkt_map.Market.isNull()).select('标准通用名').show()
    
    # 1.7 rawdata 数据
    df_raw_data = deal_ID_length(df_raw_data)
    df_raw_data = df_raw_data.withColumn('date', col('date').cast(IntegerType()))
    df_raw_data = df_raw_data.where((col('date') >= model_month_left) & (col('date') <= model_month_right))
    df_raw_data = df_raw_data.join(df_molecule_mkt_map, on='molecule', how='left') \
                        .join(df_cpa_pha_mapping, on='id', how='left').persist()

    # %%
    # 2. ======== 每个市场进行 randomForest 分析 =============  
    def getHospitalSample(df_hospital_range):
        l_hospital_sample = df_hospital_range.where(col('panel') == 1).select('panel_id').distinct().toPandas()['panel_id'].values.tolist()
        return l_hospital_sample
    
    def getRawdataSample(df_raw_data, l_hospital_sample, market):
        df_raw_data_mkt = df_raw_data.where(col('market') == market) \
                                .withColumnRenamed('pha', 'pha_id') \
                                .withColumnRenamed('market', 'doi')
        df_raw_data_sample = df_raw_data_mkt.where(col('pha_id').isin(l_hospital_sample)) \
                                .groupBy('pha_id', 'doi').agg(func.sum('sales').alias('sales'))
        return df_raw_data_sample
    
    def getIndColNames(df_ind_mkt):
         # 去掉不用的列名
        drop_cols = ['panel_id', 'pha_hosp_name', 'pha_id', 'bayer_id', 'if_panel', 'segment', 'segment_description', 'if_county']
        all_cols = list(set([i.lower()  for i in df_ind_mkt.columns])-set(drop_cols))
        new_all_cols = []
        for each in all_cols:
            if len(re.findall('机构|省|市|县|医院级别|医院等次|医院类型|性质|地址|邮编|年诊疗|总收入|门诊药品收入|住院药品收入|总支出', each)) == 0:
                new_all_cols.append(each)
        # 计算每列非空行数
        df_agg = df_ind_mkt.agg(*[func.count(func.when(~func.isnull(c), c)).alias(c) for c in new_all_cols]).persist()
        # 转置为长数据
        df_agg_col = df_agg.toPandas().T
        df_agg_col.columns = ["notNULL_Count"]
        l_agg_col_names = df_agg_col[df_agg_col.notNULL_Count >= 15000].index.tolist()
        return l_agg_col_names
    
    def dealInd(df_ind_mkt, l_agg_col_names, df_doctor_g):
        df_ind2 = df_ind_mkt.select('pha_id', *l_agg_col_names, *[i for i in df_ind_mkt.columns if '心血管' in i ])
        df_ind3 = df_ind2.join(df_BT_PHA, df_ind2['pha_id']==df_BT_PHA['pha'], how='left') \
                    .drop('pha')
        df_ind4 = df_ind3.join(df_doctor_g, df_ind3['bt']==df_doctor_g['bt_code'], how='left') \
                    .drop('bt_code')
        df_ind5 = df_ind4.select(*df_ind2.columns, *[i for i in df_ind4.columns if '_dr_n_' in i ])
    
        num_cols = list(set([i.lower()  for i in df_ind5.columns]) -set(['pha_id', 'hosp_level', 'region', 'respailty', 'province', 'prefecture', 'city_tier_2010', 'specialty_1', 'specialty_2', 're_speialty', 'specialty_3']))
        df_ind5 = df_ind5.fillna(0, subset=num_cols)
        return df_ind5
    
    def f1(x):
        y=(x+0.001)**(1/2)
        return(y)
    
    def f2(x):
        y=x**(2)-0.001
        return(y)    
    
    def getModeldata(df_ind5, df_raw_data_sample, df_hospital_range):
        df_modeldata = df_ind5.join(df_raw_data_sample, on='pha_id', how='left')
        Panel_ID_list = df_hospital_range.where(col('panel') == 1).select('panel_id').toPandas()['panel_id'].values.tolist()
    
        df_modeldata = df_modeldata.withColumn('flag_model', 
                                func.when(col('pha_id').isin(Panel_ID_list), func.lit('TRUE')) \
                                    .otherwise(func.lit('FALSE'))).persist()
        df_modeldata = df_modeldata.withColumn('sales', func.when((col('sales').isNull()) & (col('flag_model')=='TRUE'), func.lit(0)) \
                                                     .otherwise(col('sales')))
        df_modeldata = df_modeldata.withColumn('v', func.when(col('sales') > 0, f1(col('sales'))).otherwise(func.lit(0)))
        df_trn = df_modeldata.where(col('pha_id').isin(Panel_ID_list))
        return [df_modeldata, df_trn]
        
        
    for market in all_models:
        logger.debug("当前market为:" + str(market))
        # 输入
        df_hospital_range = getUniverse(market, dict_universe_choice)
 
        # 2.1 数据处理    
        all_hospital = df_hospital_range.where(col('bedsize') > 99 ).select('panel_id').distinct()
        
        l_hospital_sample = getHospitalSample(df_hospital_range)
        df_raw_data_sample = getRawdataSample(df_raw_data, l_hospital_sample, market)
        
        df_ind_mkt = df_ind.drop('panel_id').join(df_hospital_range.select('panel_id').distinct(), df_ind['pha_id']==df_hospital_range['panel_id'], how='inner')
    
        # 2.2 计算 ind_mkt 符合条件的列
        l_agg_col_names = getIndColNames(df_ind_mkt)    
        '''
        from functools import reduce
        df_agg_col = reduce(
            lambda a, b: a.union(b),
            (df_agg.select(func.lit(c).alias("Column_Name"), func.col(c).alias("notNULL_Count")) 
                for c in df_agg.columns)
        ).persist()
        df_agg_col = df_agg_col.where(col('notNULL_Count') >= 15000)
        l_agg_col_names = df_agg_col.toPandas()['Column_Name'].values
        '''
    
        # 2.3 ind_mkt 文件处理
        df_ind5 = dealInd(df_ind_mkt, l_agg_col_names, df_doctor_g)
    

        # 2.4 获得 df_modeldata 
        df_modeldata_trn = getModeldata(df_ind5, df_raw_data_sample, df_hospital_range)
        df_modeldata = df_modeldata_trn[0]
        df_trn = df_modeldata_trn[1]
        
    
        # 2.5 ===  随机森林 ===
        # 1. 数据准备
        logger.debug("RandomForest：data prepare")
        not_features_cols = ['pha_id', 'province', 'prefecture', 'specialty_1', 'specialty_2', 'specialty_3', 'doi', 'sales', 'flag_model']
        features_str_cols = ['hosp_level', 'region', 'respailty', 're_speialty', 'city_tier_2010']
        features_cols = list(set(df_trn.columns) -set(not_features_cols)- set(features_str_cols) - set('v'))
    
        def data_for_forest(data, features_str_cols):
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
    
        data = data_for_forest(df_trn, features_str_cols)
    
        # 2. 随机森林模型
        logger.debug("RandomForest：model")
        rf = RandomForestRegressor(labelCol="label", featuresCol="indexedFeatures", 
                numTrees=500, minInstancesPerNode=5, maxDepth=8)
        # numTrees=100, minInstancesPerNode=2, maxDepth=8
        model = rf.fit(data)
    
        # 特征重要性
        logger.debug("RandomForest：importances")
        dp = model.featureImportances
        dendp = DenseVector(dp)
        df_importances = pd.DataFrame(dendp.array)
        df_importances['feature'] = features_cols
        df_importances.columns=['importances','feature']  
        df_importances = df_importances.sort_values(by='importances', ascending=False)
        df_importances = spark.createDataFrame(df_importances)
    

        # 3. 对 modeldata预测
        logger.debug("RandomForest：predict result")
        df_result = data_for_forest(df_modeldata, features_str_cols)  
        df_result = df_result.withColumn('doi', func.lit(market))
        df_result = model.transform(df_result)
        df_result = df_result.withColumn('sales_from_model', f2(col('prediction'))) \
                .withColumn('training_set', func.when(col('pha_id').isin(df_trn.select('pha_id').distinct().toPandas()['pha_id'].values.tolist()), 
                                                    func.lit(1)) \
                                                .otherwise(func.lit(0)))
        df_result = df_result.withColumn('final_sales', func.when(col('flag_model')== 'TRUE', col('sales')) \
                                                    .otherwise(col('sales_from_model')))
        df_result = df_result.where(col('pha_id').isin(all_hospital.toPandas()['panel_id'].values.tolist()))
        
        df_result = df_result.withColumn('features',col('features').cast('string')) \
                                .withColumn('indexedfeatures',col('indexedfeatures').cast('string'))
        def lowerColumns(df):
            df = df.toDF(*[i.lower() for i in df.columns])
            return df
        df_result = lowerColumns(df_result)
        
        AddTableToGlue(df=df_result, database_name_of_output=g_database_temp, table_name_of_output=g_table_result, 
                           path_of_output_file=p_out, mode=out_mode) \
                    .add_info_of_partitionby({"version":run_id,"provider":project_name,"owner":owner})
        logger.debug("RandomForest：finish")
             
    # %%
    # =========== 数据输出 =============
    # 读回
    df_out = spark.sql("SELECT * FROM %s.%s WHERE version='%s' AND provider='%s' AND  owner='%s'" 
                                 %(g_database_temp, g_table_result, run_id, project_name, owner))
    df_out = df_out.drop('version', 'provider', 'owner')
    df_out = df_out.drop("indexedfeatures", "features")
    
    return {"out_df":df_out}