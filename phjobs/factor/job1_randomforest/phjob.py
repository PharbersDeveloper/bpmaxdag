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

from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorIndexer, StringIndexer
from pyspark.ml.linalg import DenseVector
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
import pandas as pd


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
    #logger.info(kwargs["b"])
    #logger.info(kwargs["c"])
    #logger.info(kwargs["d"])
    #return {}
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
    '''    
    # 输入
    max_path = kwargs["max_path"]
    project_name = kwargs["project_name"]
    outdir = kwargs["outdir"]
    all_models = kwargs["all_models"]
    universe_choice = kwargs["universe_choice"]
    model_month_right = kwargs["model_month_right"]
    model_month_left = kwargs["model_month_left"]
    rf_ntree = kwargs["rf_ntree"]
    rf_minnode = kwargs["rf_minnode"]
    
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
    
    
    # =======  数据执行  ============
    
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
        
    doctor = spark.read.csv(doctor_path, header='True')
    doctor = doctor.where(~col('BT_Code').isNull()) \
                    .select(['Department', 'Dr_N', 'Dr_N_主任', 'Dr_N_副主任', 'Dr_N_主治', 'Dr_N_住院医', 'BT_Code'])
    doctor_g = unpivot(doctor, ['BT_Code', 'Department']).persist()
    doctor_g = doctor_g.withColumn('dr1', func.concat(col('Department'), func.lit('_'), col('feature'))) \
                        .groupBy('BT_Code', 'dr1').agg(func.sum('value'))
    doctor_g = doctor_g.groupBy('BT_Code').pivot('dr1').sum('sum(value)').persist()
    
    # 1.2 BT_PHA 文件
    BT_PHA = spark.read.csv(BT_PHA_path, header='True')
    BT_PHA = BT_PHA.select('BT', 'PHA').dropDuplicates(['PHA'])
    
    # 1.3 ind 文件
    # 列名有点的无法识别，把点换成_
    ind = spark.read.csv(ind_path, header='True')
    ind = ind.toDF(*(re.sub(r'[\.\s]+', '_', c) for c in ind.columns))
    for each in ind.columns[18:]:
        ind = ind.withColumn(each, ind[each].cast(DoubleType()))
    
    # 1.4 cpa_pha_mapping 文件
    hosp_mapping = spark.read.parquet(cpa_pha_mapping_path)
    
    # 1.5 product_map 文件
    molecule_map = spark.read.parquet(product_map_path)
    molecule_map = molecule_map.select('Molecule','标准通用名').distinct()
    
    # 1.6 mkt_mapping 文件
    mkt_mapping = spark.read.parquet(mkt_mapping_path)
    mkt_mapping = mkt_mapping.withColumnRenamed('mkt', 'Market')
    molecule_mkt_map = molecule_map.join(mkt_mapping, on='标准通用名', how='left')
    # molecule_mkt_map.where(molecule_mkt_map.Market.isNull()).select('标准通用名').show()
    
    # 1.7 rawdata 数据
    rawdata = spark.read.parquet(raw_data_path)
    rawdata = rawdata.withColumn('Date', col('Date').cast(IntegerType()))
    rawdata = rawdata.where((col('Date') >= model_month_left) & (col('Date') <= model_month_right))
    rawdata = rawdata.join(molecule_mkt_map, on='Molecule', how='left') \
                        .join(hosp_mapping, on='ID', how='left').persist()
                        
    # 2. == 每个市场进行 randomForest 分析 ==
    
    # market = '固力康'
    
    for market in all_models:
        print("当前market为:" + str(market))
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
        modeldata = modeldata.withColumn('v', func.when(col('Sales') > 0, f1(col('Sales'))).otherwise(col('Sales')))
        
        trn = modeldata.where(modeldata.PHA_ID.isin(Panel_ID_list))
        
        # 2.5 ===  随机森林 ===
        # 1. 数据准备
        print("RandomForest：data prepare")
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
            featureIndexer = VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=10).fit(data)
            data = featureIndexer.transform(data)
            return data
            
        data = data_for_forest(trn)
        
        # 2. 随机森林模型
        print("RandomForest：model")
        rf = RandomForestRegressor(labelCol="label", featuresCol="indexedFeatures", 
                numTrees=rf_ntree, minInstancesPerNode=rf_minnode, maxDepth=8, seed=10)
        model = rf.fit(data)
        
        # 特征重要性
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
        print("RandomForest：predict result")
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
                
        # 4. 评价模型（5次随机森林，计算nmse）
        print("nmse：RandomForest")
        for i in range(1,6):
            # 模型构建
            (df_training, df_test) = data.randomSplit([0.7, 0.3])
            rf = RandomForestRegressor(labelCol="label", featuresCol="indexedFeatures", 
                    numTrees=rf_ntree, minInstancesPerNode=rf_minnode, maxDepth=8, seed=100)
            model = rf.fit(df_training)
            # 结果预测
            # pipeline = Pipeline(stages=[rf])
            df_training_pred = model.transform(df_training)
            df_training_pred = df_training_pred.withColumn('datatype', func.lit('train'))
            df_test_pred = model.transform(df_test)
            df_test_pred = df_test_pred.withColumn('datatype', func.lit('test'))
            
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
        print("nmse：计算")
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
            
    
    return {} 
        
        
