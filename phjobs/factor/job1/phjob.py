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
'''
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
    logger.info(kwargs["a"])
    logger.info(kwargs["b"])
    logger.info(kwargs["c"])
    logger.info(kwargs["d"])
    return {}
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
    
# 输入

max_path = 's3a://ph-max-auto/v0.0.1-2020-06-08/'
project_name = 'Eisai'
outdir = '202009'

doctor_path = max_path + '/Common_files/factor_files/doctor.csv' 
BT_PHA_path = max_path + '/Common_files/factor_files/BT_PHA.csv'
ind_path = max_path + '/Common_files/factor_files/ind.csv'

cpa_pha_mapping_path = max_path + '/' + project_name + '/cpa_pha_mapping'
mkt_mapping_path = max_path + '/' + project_name + '/mkt_mapping'

#raw_data_path = max_path + '/' + project_name + '/' + outdir + '/raw_data'
raw_data_path = 's3a://ph-max-auto/v0.0.1-2020-06-08/Test/Eisai/raw_data.csv'
product_map_path = max_path + '/' + project_name + '/' + outdir + '/prod_mapping'

universe_choice = 'Empty'

# 市场的universe文件
universe_choice_dict={}
if universe_choice != "Empty":
    for each in universe_choice.replace(", ",",").split(","):
        market_name = each.split(":")[0]
        universe_name = each.split(":")[1]
        universe_choice_dict[market_name]=universe_name


    
# 输出


# =======  数据执行  ============




# 匹配文件准备

# 1. doctor 文件
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

# 2. BT_PHA 文件
BT_PHA = spark.read.csv(BT_PHA_path, header='True')
BT_PHA = BT_PHA.select('BT', 'PHA').dropDuplicates(['PHA'])

# 3. ind 文件
# 列名有点 无法识别
import re 
ind = spark.read.csv(ind_path, header='True')
ind = ind.toDF(*(re.sub(r'[\.\s]+', '_', c) for c in ind.columns))
for each in ind.columns[18:]:
    ind = ind.withColumn(each, ind[each].cast(DoubleType()))

    
# 4. cpa_pha_mapping 文件
hosp_mapping = spark.read.parquet(cpa_pha_mapping_path)

# 5. product_map 文件
mole_mapping = spark.read.parquet(product_map_path)
mole_mapping = mole_mapping.select('Molecule','标准通用名').distinct()

# 6. mkt_mapping 文件
mkt_mapping = spark.read.parquet(mkt_mapping_path)
mkt_mapping = mkt_mapping.withColumnRenamed('mkt', 'Market')
mole_mkt_mapping = mole_mapping.join(mkt_mapping, on='标准通用名', how='left')
mole_mkt_mapping.where(mole_mkt_mapping.Market.isNull()).select('标准通用名').show()

# 数据读取
rawdata = spark.read.csv(raw_data_path, header='True')
rawdata = rawdata.withColumn('Date', col('Date').cast(IntegerType()))
rawdata = rawdata.where((col('Date') > 201900) & (col('Date') < 202000))

rawdata_m = rawdata.join(mole_mkt_mapping, on='Molecule', how='left') \
                    .join(hosp_mapping, on='ID', how='left')
                    
$========================================

# 处理单个市场，用pandas udf
market = '固力康'

if market in universe_choice_dict.keys():
    universe_path = max_path + '/' + project_name + '/' + universe_choice_dict[market]
    hosp_range =  spark.read.parquet(universe_path)
else:
    universe_path = max_path + '/' + project_name + '/universe_base'
    hosp_range =  spark.read.parquet(universe_path)
    
all_hosp = hosp_range.where(hosp_range.BEDSIZE > 99 ).select('Panel_ID').distinct()

hosp_range_sample = hosp_range.where((hosp_range.PANEL == 1) & (hosp_range.BEDSIZE > 99)).select('Panel_ID').distinct()

rawdata_mkt = rawdata_m.where(rawdata_m.Market == market) \
                        .withColumnRenamed('PHA', 'PHA_ID') \
                        .withColumnRenamed('Market', 'DOI')

####得到因变量
tmp=hosp_range.where(hosp_range.PANEL == 1).select('Panel_ID').distinct().toPandas()['Panel_ID'].values.tolist()
rawdata_i = rawdata_mkt.where(col('PHA_ID').isin(tmp)) \
                        .groupBy('PHA_ID', 'DOI').agg(func.sum('Sales').alias('Sales'))

ind_mkt = ind.join(hosp_range.select('Panel_ID').distinct(), ind['PHA_ID']==hosp_range['Panel_ID'], how='inner')

# %% 计算 ind_mkt 每列非空的行数

# 去掉无用的列名
import re
drop_cols = ["Panel_ID", "PHA_Hosp_name", "PHA_ID", "Bayer_ID", "If_Panel", "Segment", "Segment_Description", "If_County"]
all_cols = list(set(ind_mkt.columns)-set(drop_cols))
new_all_cols = []
for each in all_cols:
    if len(re.findall('机构|省|市|县|医院级别|医院等次|医院类型|性质|地址|邮编|年诊疗|总收入|门诊药品收入|住院药品收入|总支出', each)) == 0:
        new_all_cols.append(each)

# 计算每列非空行数
df_agg = ind_mkt.agg(*[func.count(func.when(~func.isnull(c), c)).alias(c) for c in new_all_cols]).persist()
# 转置为长数据    
from functools import reduce
df_agg_col = reduce(
    lambda a, b: a.union(b),
    (df_agg.select(func.lit(c).alias("Column_Name"), func.col(c).alias("notNULL_Count")) 
        for c in df_agg.columns)
).persist()

df_agg_col = df_agg_col.where(col('notNULL_Count') >= 15000)

df_agg_col_names = df_agg_col.toPandas()['Column_Name'].values

# %% 获得 ind5
ind2 = ind_mkt.select('PHA_ID', *df_agg_col_names, *[i for i in ind_mkt.columns if '心血管' in i ])
ind3 = ind2.join(BT_PHA, ind2.PHA_ID==BT_PHA.PHA, how='left') \
            .drop('PHA')
ind4 = ind3.join(doctor_g, ind3.BT==doctor_g.BT_Code, how='left') \
            .drop('BT_Code')
ind5 = ind4.select(*ind2.columns, *[i for i in ind4.columns if '_Dr_N_' in i ])

num_cols = list(set(ind5.columns) -set(['PHA_ID', 'Hosp_level', 'Region', 'respailty', 'Province', 'Prefecture', 'City_Tier_2010', 'Specialty_1', 'Specialty_2', 'Re_Speialty', 'Specialty_3']))
ind5 = ind5.fillna(0, subset=num_cols) 


# %%
def f1(x):
    y=(x+0.001)**(1/2)
    return(y)
    
def f2(x):
    y=x**(2)-0.001
    return(y)

# %% 获得 modeldata 
modeldata = ind5.join(rawdata_i, on='PHA_ID', how='left')
Panel_ID_list = hosp_range.where(hosp_range.PANEL == 1).select('Panel_ID').toPandas()['Panel_ID'].values.tolist()

modeldata = modeldata.withColumn('flag_model', 
                        func.when(modeldata.PHA_ID.isin(Panel_ID_list), func.lit('TRUE')) \
                            .otherwise(func.lit('FALSE'))).persist()
modeldata = modeldata.withColumn('Sales', func.when((col('Sales').isNull()) & (col('flag_model')=='TRUE'), func.lit(0)) \
                                             .otherwise(col('Sales')))
modeldata = modeldata.withColumn('v', func.when(col('Sales') > 0, f1(col('Sales'))).otherwise(col('Sales')))

trn = modeldata.where(modeldata.PHA_ID.isin(Panel_ID_list))

trn.repartition(1).write.format("parquet") \
    .mode("overwrite").save('s3a://ph-max-auto/v0.0.1-2020-06-08/Test/Eisai/trn')

# %%  ===========  随机森林 ===================
from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorIndexer, StringIndexer
from pyspark.ml.linalg import SparseVector, DenseVector
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.feature import VectorAssembler


# 1. 数据准备
data = trn
not_features_cols = ["PHA_ID", "Province", "Prefecture", "Specialty_1", "Specialty_2", "Specialty_3", "DOI", "Sales", "flag_model"]
features_str_cols = ['Hosp_level', 'Region', 'respailty', 'Re_Speialty', 'City_Tier_2010']

# 使用StringIndexer，将features中的字符型变量转为分类数值变量
indexers = [StringIndexer(inputCol=column, outputCol=column+"_index").fit(data) for column in features_str_cols]
pipeline = Pipeline(stages=indexers)
data = pipeline.fit(data).transform(data)

# 使用 VectorAssembler ，将特征合并为features
features_cols = list(set(data.columns) -set(not_features_cols)- set(features_str_cols) - set('v'))
assembler = VectorAssembler( \
     inputCols = features_cols, \
     outputCol = "features")
data = assembler.transform(data)
data = data.withColumnRenamed('v', 'label')

# 识别哪些是分类变量，Set maxCategories so features with > 4 distinct values are treated as continuous.
featureIndexer = VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=10).fit(data)
data = featureIndexer.transform(data)

# 2. 重要性
rf = RandomForestRegressor(labelCol="label", featuresCol="indexedFeatures", numTrees=100, seed=10)
model = rf.fit(data)
dp = model.featureImportances
dendp = DenseVector(dp)
dp2 = pd.DataFrame(dendp.array)
dp2['feature'] = features_cols
dp2.columns=['importances','feature']  
dp2.sort_values(by='0')

# 2. 模型构建
(df_training, df_test) = data.randomSplit([0.7, 0.3])

rf = RandomForestRegressor(labelCol="label", featuresCol="indexedFeatures", numTrees=100, seed=100)
model = rf.fit(df_training)



# 3. 结果预测
# pipeline = Pipeline(stages=[rf])
df_training_pred = model.transform(df_training)
df_training_pred = df_training_pred.withColumn('datatype', func.lit('train'))
df_test_pred = model.transform(df_test)
df_test_pred = df_test_pred.withColumn('datatype', func.lit('test'))

# 4. 计算误差
df_all = df_training_pred.union(df_test_pred.select(df_training_pred.columns))

schema = StructType([
            StructField("Province", StringType(), True), 
            StructField("datatype", StringType(), True),
            StructField("NMSE", StringType(), True)
            ])

@pandas_udf(schema, PandasUDFType.GROUPED_MAP)   
def nmse_func(pdf):
    import pandas as pd
    import numpy as np
    Province = pdf['Province'][0]
    datatype = pdf['datatype'][0]
    pdf['tmp1'] = (pdf['y_prd'] - pdf['y_true']) **2
    tmp1_mean = pdf['tmp1'].mean()
    y_true_mean = pdf['y_true'].mean()
    pdf['tmp2'] = (pdf['y_true'] - y_true_mean) **2
    tmp2_mean = pdf['tmp2'].mean()
    if tmp2_mean == 0:
        NMSE = 'inf'
    NMSE = str(tmp1_mean/tmp2_mean)
    return pd.DataFrame([[Province] + [datatype] + [NMSE]], columns=["Province", "datatype", "NMSE"])


df_all = df_all.withColumn('y_prd', f2(col('prediction'))) \
        .withColumn('y_true', f2(col('label')))
df_nmse = df_all.select('Province', 'y_true', 'y_prd', 'datatype') \
                .groupBy('Province', 'datatype').apply(nmse_func)
df_nmse = df_nmse.withColumn('NMSE', col('NMSE').cast(DoubleType()))
df_nmse = df_nmse.pivot
    

'''
df_training_pred.select("label","prediction", "features").show(5)

evaluator = RegressionEvaluator(
    labelCol="label", predictionCol="prediction", metricName="rmse")	    

rmse = evaluator.evaluate(df_test_pred)
print('测试数据的均方根误差（rmse）:{}'.format(rmse))
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)
rfModel = model.stages[1]
print(rfModel)
'''
# ===========================

from pyspark.ml.feature import RFormula

# features列中的所有分类变量都被转换为数值
colnames = data2.columns
colnames.remove('v')
formula_list = 'v' + ' ~ ' + '+'.join(colnames)

formula = RFormula(formula=formula_list, featuresCol="features", labelCol="label")

t1 = formula.fit(data)
train1 = t1.transform(Train1)
test1 = t1.transform(Test1)


    


model = RandomForestRegressor(numTrees=100, labelCol="indexed", seed=3)
model1 = rf.fit(train_cv)
predictions = model1.transform(test_cv)

from pyspark.ml.evaluation import RegressionEvaluator
evaluator = RegressionEvaluatormse = evaluator.evaluate(predictions,{evaluator.metricName:"mse" })
import numpy as np
np.sqrt(mse)

