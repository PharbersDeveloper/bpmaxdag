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
max_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/"
project_name = "Eisai"
ourdir = "202009"
all_models = ""
minimum_product_columns = "Brand, Form, Specifications, Pack_Number, Manufacturer"
minimum_product_sep = "|"

minimum_product_columns = minimum_product_columns.replace(" ","").split(",")
id_bedsize_path = max_path + '/' + '/Common_files/ID_Bedsize'
universe_path = max_path + '/' + project_name + '/universe_base'
city_info_path = max_path + '/' + project_name + '/province_city_mapping'
mkt_mapping_path = max_path + '/' + project_name + '/mkt_mapping'
cpa_pha_mapping_path = max_path + '/' + project_name + '/cpa_pha_mapping'
product_map_path = max_path + '/' + project_name + '/' + ourdir + '/prod_mapping'
raw_data_path = max_path + '/' + project_name + '/' + ourdir + '/raw_data'


# ===== 数据执行 =====
@udf(StringType())
def city_change(name):
    if name in ["福州市", "厦门市", "泉州市"]:
        newname = "福厦泉"
    elif name in ["珠海市", "东莞市", "中山市", "佛山市"]:
        newname = "珠三角"
    else:
        newname = name
    return newname
    
def deal_ID_length(df):
    # ID不足7位的补足0到6位
    # 国药诚信医院编码长度是7位数字，cpa医院编码是6位数字。
    df = df.withColumn("ID", df["ID"].cast(StringType()))
    # 去掉末尾的.0
    df = df.withColumn("ID", func.regexp_replace("ID", "\\.0", ""))
    df = df.withColumn("ID", func.when(func.length(df.ID) < 7, func.lpad(df.ID, 6, "0")).otherwise(df.ID))
    return df
    
# 1. prod_map 文件
product_map = spark.read.parquet(product_map_path)
# a. 列名清洗统一
if project_name == "Sanofi" or project_name == "AZ":
    product_map = product_map.withColumnRenamed(product_map.columns[21], "pfc")
if project_name == "Eisai":
    product_map = product_map.withColumnRenamed(product_map.columns[22], "pfc")
for i in product_map.columns:
    if i in ["标准通用名", "通用名_标准", "药品名称_标准", "S_Molecule_Name"]:
        product_map = product_map.withColumnRenamed(i, "通用名")
    if i in ["packcode", "Pack_ID", "Pack_Id", "PackID", "packid"]:
        product_map = product_map.withColumnRenamed(i, "pfc")
    if i in ["商品名_标准", "S_Product_Name"]:
        product_map = product_map.withColumnRenamed(i, "标准商品名")
# b. 选取需要的列
product_map = product_map \
                .select("min1", "pfc", "通用名", "标准商品名") \
                .withColumn("pfc", product_map["pfc"].cast(IntegerType())) \
                .distinct()
# c. pfc为0统一替换为null
product_map = product_map.withColumn("pfc", func.when(product_map.pfc == 0, None).otherwise(product_map.pfc)).distinct()
# d. min2处理
product_map = product_map.withColumnRenamed("pfc", "Pack_ID") \
                .withColumn("min1", func.regexp_replace("min1", "&amp;", "&")) \
                .withColumn("min1", func.regexp_replace("min1", "&lt;", "<")) \
                .withColumn("min1", func.regexp_replace("min1", "&gt;", ">"))

# 2. universe 文件                  
universe = spark.read.parquet(universe_path)
universe = universe.select("Panel_ID", "Province", "City", "PANEL") \
                    .withColumnRenamed("Panel_ID", "PHA") \
                    .withColumn("PANEL", col("PANEL").cast(DoubleType())) \
                    .distinct()


# 3. mkt_mapping 文件
mkt_mapping = spark.read.parquet(mkt_mapping_path)
mkt_mapping = mkt_mapping.withColumnRenamed("标准通用名", "通用名")

# 4. cpa_pha_mapping 文件
cpa_pha_mapping = spark.read.parquet(cpa_pha_mapping_path)
cpa_pha_mapping = cpa_pha_mapping.where(col("推荐版本") == 1).select('ID', 'PHA').distinct()
cpa_pha_mapping = deal_ID_length(cpa_pha_mapping)

# 5. city_info_path
city_info = spark.read.parquet(city_info_path)
city_info = city_info.withColumnRenamed('City', 'City_imp').distinct()

# 6. id_bedsize
schema= StructType([
        StructField("ID", StringType(), True),
        StructField("Bedsize", DoubleType(), True),
        StructField("Bedsize>99", DoubleType(), True)
        ])
id_bedsize = spark.read.parquet(id_bedsize_path, schema=schema)
id_bedsize = deal_ID_length(id_bedsize)
id_bedsize = id_bedsize.join(cpa_pha_mapping, on='ID', how='inner')

# 7. raw_data 文件
raw_data = spark.read.parquet(raw_data_path)
raw_data = spark.read.csv('s3a://ph-max-auto/v0.0.1-2020-06-08/Test/Eisai/raw_data.csv', header=True)

# a. 生成min1
if project_name != "Mylan":
    raw_data = raw_data.withColumn("Brand", func.when((raw_data.Brand.isNull()) | (raw_data.Brand == 'NA'), raw_data.Molecule).
                               otherwise(raw_data.Brand))
for colname, coltype in raw_data.dtypes:
    if coltype == "logical":
        raw_data = raw_data.withColumn(colname, raw_data[colname].cast(StringType()))

raw_data = raw_data.withColumn("tmp", func.when(func.isnull(raw_data[minimum_product_columns[0]]), func.lit("NA")).
                               otherwise(raw_data[minimum_product_columns[0]]))

for i in minimum_product_columns[1:]:
    raw_data = raw_data.withColumn(i, raw_data[i].cast(StringType()))
    raw_data = raw_data.withColumn("tmp", func.concat(
        raw_data["tmp"],
        func.lit(minimum_product_sep),
        func.when(func.isnull(raw_data[i]), func.lit("NA")).otherwise(raw_data[i])))
# Mylan不重新生成minimum_product_newname: min1，其他项目生成min1
if project_name == "Mylan":
    raw_data = raw_data.drop("tmp")
else:
    if 'min1' in raw_data.columns:
        raw_data = raw_data.drop('min1')
    raw_data = raw_data.withColumnRenamed('tmp', 'min1')

# 字段类型修改
raw_data = raw_data.withColumn('Year', func.substring(col('Date').cast(StringType()), 1, 4))
raw_data = raw_data.withColumn('Year', col('Year').cast(IntegerType())) \
                        .withColumn('Date', col('Date').cast(IntegerType())) \
                        .withColumn('Sales', col('Sales').cast(DoubleType())) \
                        .withColumn('Units', col('Sales').cast(DoubleType())) \
                        .withColumn('Units_Box', col('Units_Box').cast(DoubleType()))
                        
# b.匹配信息: Pack_ID,通用名,标准商品名; mkt; PHA
raw_data2 = deal_ID_length(raw_data)
raw_data2 = raw_data2.join(product_map, on='min1', how='left')
raw_data2 = raw_data2.join(mkt_mapping, on='通用名', how='left')
raw_data2 = raw_data2.join(cpa_pha_mapping, on='ID', how='inner')
raw_data2 = raw_data2.join(universe.select("PHA", "Province", "City").distinct(), on='PHA', how='inner')

# 8. ims 数据
ims_sales_path = max_path + "/Common_files/extract_data_files/cn_IMS_Sales_Fdata_202007.csv"
ims_sales = spark.read.csv(ims_sales_path, header=True)

geo_name = {'BJH':'北京市', 'BOI':'天津市', 'BOJ':'济南市', 'CGH':'常州市', 'CHT':'全国', 'FXQ':'福厦泉', 
            'GZH':'广州市', 'NBH':'宁波市', 'SHH':'上海市', 'WZH':'温州市', 'SXC':'苏锡城市群'}
geo_dict={}
geo_dict['Geography_id'] = list(geo_name.keys())
geo_dict['City'] = list(geo_name.values())
geo_df = pd.DataFrame(geo_dict)
geo_df = spark.createDataFrame(geo_df)  

# 测试用：正式用 Brand 要改成 标准商品名
year_list=['2018', '2019']
ims_sales = ims_sales.withColumn('Year', func.substring(ims_sales.Period_Code, 0, 4)) \
                    .join(geo_df, on='Geography_id', how='inner') \
                    .join(raw_data2.select('Pack_ID', 'mkt', 'Brand').distinct(), on='Pack_ID', how='inner')
ims_sales = ims_sales.where(col('Year').isin(year_list)) \
                    .select('City', 'mkt', 'Year', 'LC', 'Brand').distinct().persist()
# 测试用over 

# ============  每个市场 =============
# 市场
market = '固力康'
factor_path = max_path + '/' + project_name + '/factor/factor_' + market
universe_ot_path = max_path + '/' + project_name + '/universe/universe_ot_' + market

# universe_ot 文件        
universe_ot = spark.read.parquet(universe_ot_path)

# factor 文件    
factor = spark.read.parquet(factor_path)
if "factor" not in factor.columns:
    factor = factor.withColumnRenamed("factor_new", "factor")
factor = factor.select('City', 'factor').distinct()

# 
data = raw_data2.where(col('mkt') == market)

'''
权重计算：
    利用医药收入和城市Factor
    从universe中去掉outliers以及其他Panel == 0的样本
'''
# 目的是实现 PANEL = 0 的样本回填
pha_list = universe.where(col('PANEL') == 0).select('PHA').distinct().toPandas()['PHA'].tolist()
# 要修改为PHA
pha_list2 = data.where(col('Date') > 202000).select('ID').distinct().toPandas()['ID'].tolist()

universe_ot_rm = universe_ot.where((col('PANEL') == 1) | (col('Panel_ID').isin(pha_list))) \
                            .where( ~((col('PANEL') == 0) & (col('Panel_ID').isin(pha_list2)) ))
universe_ot_rm = universe_ot_rm.fillna(0, subset=['Est_DrugIncome_RMB', 'BEDSIZE'])
universe_ot_rm = universe_ot_rm.withColumn('Panel_income', col('Est_DrugIncome_RMB') * col('PANEL')) \
                        .withColumn('Bedmark', func.when(col('BEDSIZE') >= 100, func.lit(1)).otherwise(func.lit(0)))
universe_ot_rm = universe_ot_rm.withColumn('non_Panel_income', col('Est_DrugIncome_RMB') * (1-col('PANEL')) * col('Bedmark'))

Seg_Panel_income = universe_ot_rm.groupBy('Seg').agg(func.sum('Panel_income').alias('Seg_Panel_income'))
universe_ot_rm = universe_ot_rm.join(Seg_Panel_income, on='Seg', how='left')

Seg_non_Panel_income = universe_ot_rm.groupBy('Seg', 'City') \
                            .agg(func.sum('non_Panel_income').alias('Seg_non_Panel_income'))
universe_ot_rm = universe_ot_rm.join(Seg_non_Panel_income, on=['Seg', 'City'], how='left').persist()

# 按Segment+City拆分样本
seg_city = universe_ot_rm.select('Seg','City','Seg_non_Panel_income','Seg_Panel_income').distinct()

seg_pha = universe_ot_rm.where(col('PANEL') == 1).select('Seg','City','Panel_ID','BEDSIZE').distinct() \
                        .withColumnRenamed('City', 'City_Sample')
                        
weight_table = seg_city.join(seg_pha, on='Seg', how='left') \
                        .join(factor, on='City', how='left').persist()
                        
# 只给在城市内100床位以上的样本，添加 1 
weight_table = weight_table.withColumn('tmp', func.when((col('City') == col('City_Sample')) & (col('BEDSIZE') > 99) , \
                                                    func.lit(1)).otherwise(func.lit(0)))
weight_table = weight_table.withColumn('weight', col('tmp') + \
                                            col('factor') * col('Seg_non_Panel_income') / col('Seg_Panel_income'))
# pandas当 Seg_non_Panel_income和Seg_Panel_income都为0结果是 null，只有 Seg_Panel_income 是 0 结果为 inf 
# pyspark 都会是null, 用-999代表无穷大
weight_table = weight_table.withColumn('weight', func.when((col('Seg_Panel_income') == 0) & (col('Seg_non_Panel_income') != 0), \
                                                       func.lit(-999)).otherwise(col('weight')))
weight_init = weight_table.select('Seg','City','Panel_ID','weight','City_Sample','BEDSIZE').distinct()

# %% 权重与PANEL的连接
# 置零之和 不等于 和的置零，因此要汇总到Date层面
data_sub = data.groupBy('PHA','ID','Date').agg(func.sum('Sales').alias('Sales'))

tmp_weight = weight_init.join(data_sub, weight_init.Panel_ID == data_sub.PHA, how='outer').persist()
tmp_weight = tmp_weight.fillna({'weight':1, 'Sales':0})
# 无穷大仍然为 null 
tmp_weight = tmp_weight.withColumn('weight', func.when(col('weight') == -999, func.lit(None)).otherwise(col('weight')))

# %% 匹配省份城市
tmp_city = tmp_weight.join(city_info, on='ID', how='left')
tmp_city = tmp_city.withColumn('City', func.when(col('City').isNull(), col('City_imp')).otherwise(col('City'))) \
            .withColumn('City_Sample', func.when(col('City_Sample').isNull(), col('City_imp')) \
                                            .otherwise(col('City_Sample')))

# %% 床位数匹配以及权重修正
tmp_bed =  tmp_city.join(id_bedsize.select('ID', 'Bedsize>99').distinct(), on='ID', how='left')

# 不在大全的床位数小于1的，权重减1，等同于剔除
tmp_bed = tmp_bed.withColumn('weight', func.when((col('Bedsize>99') == 0) & (col('Panel_ID').isNull()), \
                                            col('weight')-1).otherwise(col('weight')))
tmp_bed = tmp_bed.withColumn('Year', func.substring(col('Date').cast(StringType()), 1, 4))
tmp_bed = tmp_bed.withColumn('Year', col('Year').cast(IntegerType()))

# %% MAX结果计算
tmp_bed = tmp_bed.withColumn('MAX_2019', col('Sales') * col('weight'))
tmp_bed_seg = tmp_bed.groupBy('Seg','Date').agg(func.sum('MAX_2019').alias('MAX_2019')).persist()
tmp_bed_seg = tmp_bed_seg.withColumn('Positive', func.when(col('MAX_2019') >= 0, func.lit(1)).otherwise(func.lit(0)))
tmp_bed = tmp_bed.join(tmp_bed_seg.select('Seg','Positive').distinct(), on='Seg', how='left')
tmp_bed = tmp_bed.fillna({'Positive':1})

tmp_max_city = tmp_bed.where(col('Positive') == 1).where(~col('Year').isNull()) \
                    .groupBy('City','Year').agg(func.sum('MAX_2019').alias('MAX_2019'), func.sum('Sales').alias('Sales')).persist()

# %% 权重初始值
weight_0 = tmp_bed.select('City','Panel_ID','Positive','City_Sample','Seg','weight').distinct()

weight_0 = weight_0.withColumn('City', city_change(col('City'))) \
                    .withColumn('City_Sample', city_change(col('City_Sample')))

# %% 目标字典
'''
这个字典最好根据IMS数据自动生成
    本例是贝达项目的宁波市调整

若想自动优化各个城市，可以循环多重字典。字典生成需要：
    1. 处理IMS数据，计算增长率等
    2. 按 "城市 -> 类别 -> 产品" 的层级生成字典
'''

# 暂时用ims生成的
if False:
    data_count = data
    #选择要进行计算增长率的分子
    data_count = data_count.groupBy('Year','Brand','City').agg(func.sum('Sales').alias('Sales'))
    data_count = data_count.groupBy('City','Brand').pivot('Year').agg(func.sum('Sales')).fillna(0).persist()
    data_count = data_count.withColumn('gr', col('2019')/col('2018'))
    all_sales = data_count.groupBy('City').agg(func.sum('2018').alias('2018_sales'), func.sum('2019').alias('2019_sales'))
    
    data_count = data_count.join(all_sales, on='City', how='inner')
    gr_city = data_count.groupBy('City').agg(func.sum('2018_sales').alias('2018'), func.sum('2018_sales').alias('2019'))
    gr_city = gr_city.withColumn('gr', col('2019')/col('2018')) \
                    .withColumnRenamed('City', 'city')
    
    data_count = data_count.withColumn('share', col('2019')/col('2019_sales')) \
                            .withColumn('share_ly', col('2018')/col('2018_sales'))
    data_count = data_count.select('City','Brand','gr','share','share_ly').distinct() \
                            .fillna(0)
    #City|                Brand|                  gr|               share|            share_ly|
    #+--------------------+---------------------+--------------------+--------------------+--------------------+
    #|              苏州市|  阿法迪三-以色列梯瓦|   1.013510391125606| 0.12731186926315258| 0.12069570460450474|

# ims 数据
ims_sales_mkt = ims_sales.where(col('mkt') == market)
ims_sales_brand = ims_sales_mkt.groupBy('mkt', 'City', 'Brand', 'Year').agg(func.sum('LC').alias('Sales'))
ims_sales_city = ims_sales_mkt.groupBy('mkt', 'City', 'Year').agg(func.sum('LC').alias('Sales_city'))

ims_sales_gr = ims_sales_brand.join(ims_sales_city, on=['mkt', 'City', 'Year'], how='left').persist()
ims_sales_gr = ims_sales_gr.groupBy('mkt', 'City', 'Brand').pivot('Year') \
                        .agg(func.sum('Sales').alias('Sales'), func.sum('Sales_city').alias('Sales_city')) \
                        .fillna(0)
ims_sales_gr = ims_sales_gr.withColumn('gr', col('2019_Sales')/col('2018_Sales')) \
                           .withColumn('share', col('2019_Sales')/col('2019_Sales_city')) \
                           .withColumn('share_ly', col('2018_Sales')/col('2018_Sales_city'))
                           
# 测试用
ims_sales_gr = spark.read.csv('s3a://ph-max-auto/v0.0.1-2020-06-08/Test/Eisai/Share_total.csv', header=True)
ims_sales_gr = ims_sales_gr.select('IMS_gr', 'IMS_share_2019', 'IMS_share_2018', 'Brand', 'City', 'DOI') \
                            .withColumn('IMS_gr', col('IMS_gr').cast(DoubleType())) \
                            .withColumn('IMS_share_2019', col('IMS_share_2019').cast(DoubleType())) \
                            .withColumn('IMS_share_2018', col('IMS_share_2018').cast(DoubleType())) \
                            .withColumnRenamed('IMS_gr', 'gr') \
                            .withColumnRenamed('IMS_share_2019', 'share') \
                            .withColumnRenamed('IMS_share_2018', 'share_ly') \
                            .fillna(0)
ims_sales_gr = ims_sales_gr.where(col('Brand') != 'total')
ims_sales_gr = ims_sales_gr.where(col('DOI') == market)
# 测试用 over


# 可以在这里输出  ims_sales_gr 和 data ，拆分为两个 job 后面只用了这两个文件，市场名命名


# ===== 城市 ===========================
# 输入
city_brand = "福厦泉:4"
city_brand_dict={}
for each in city_brand.replace(" ","").split(","):
    city_name = each.split(":")[0]
    brand_number = each.split(":")[1]
    city_brand_dict[city_name]=brand_number

city_list = list(city_brand_dict.keys())       
#target_city = u'福厦泉'
#brand_number = 4

# DOI 要改成mkt
'''
target_share = ims_sales_gr.where(col('City') == target_city)
target_share = target_share.withColumn('row_num', func.row_number() \
                    .over(Window().orderBy(col('share').desc())))
target_share = target_share.where(col('row_num').between(0, brand_number))
target = target_share.select('Brand').distinct().toPandas()['Brand'].tolist()
'''
# 利用ims_sales_gr，生成每个城市 top 产品的 'gr','share','share_ly' 字典
ims_sales_gr_city = ims_sales_gr.where(col('City').isin(city_list))


def func_target_brand(pdf, city_brand_dict):
    city_name = pdf['City'][0]
    brand_number = city_brand_dict[city_name]
    pdf = pdf.sort_values(by='share', ascending=False).reset_index()[0:int(brand_number)]
    dict_share = pdf.groupby(['City'])['Brand','gr','share','share_ly'].apply(lambda x : x.set_index('Brand').to_dict()).to_dict()
    dict_share = json.dumps(dict_share)
    return pd.DataFrame([[city_name] + [dict_share]], columns=['City', 'dict'])

schema= StructType([
        StructField("City", StringType(), True),
        StructField("dict", StringType(), True)
        ])
@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def udf_target_brand(pdf):
    return func_target_brand(pdf, city_brand_dict)

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

# ======= dict_target_share 已经存为字典，然后对 data 进行group ========


# %% 观察放大结果
#data_target = data.where(col('Date').between(201800, 202000)) \
#                    .select('Date','PHA','Brand','Sales').distinct() \
#                    .join(weight_0.where(col('City') == target_city), data.PHA == weight_0.Panel_ID, how='inner').persist()

data_city = data.where(col('Date').between(201800, 202000)) \
                    .select('Date','PHA','Brand','Sales').distinct() \
                    .join(weight_0, data.PHA == weight_0.Panel_ID, how='inner').persist()
data_city = data_city.where(col('City').isin(city_list))

data_city = data_city.withColumn('MAX_weighted', col('weight')*col('Sales')) \
                        .withColumn('Year', func.substring(col('Date').cast(StringType()), 1, 4)) \
                        .withColumn('tmp', func.concat(col('Year'), func.lit('_'), col('Brand'))) \
                        .join(id_bedsize.select('PHA','Bedsize>99').dropDuplicates('PHA'), on='PHA', how='left')
                        
# 梯度下降
def func_target_brand(pdf, dict_target_share, l=100, m = 10000):
    city_name = pdf['City'][0]
    target = list(dict_target_share[city_name]['gr'].keys())
    brand_number = len(target)
    
    H = pdf.pivot_table(index=['City','Panel_ID','City_Sample','weight'], columns=['tmp'], values='Sales',
    fill_value=0, aggfunc='sum')
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
    W_result, X, Growth, Share= gradient_descent(W_0, S, G, S_ly, l, m, brand_number)
    
    # 首先标准化，使w优化前后的点积相等
    W_norm = W_result * Ht_19.T.dot(W_0)[0]/Ht_19.T.dot(W_result)[0]
    
    H2 = H['Bedsize>99'].fillna(0)
    H2['weight_factor'] = (W_norm-1)/(np.array(H2['weight']).reshape(-1,1)-1)
    H2['weight_factor1'] = (W_norm)/(np.array(H2['weight']).reshape(-1,1))
    H2.loc[(H2['Bedsize>99'] == 0), 'weight_factor'] = H2.loc[(H2['Bedsize>99'] == 0), 'weight_factor1']
    H2['weight_factor'].fillna(0, inplace=True)
    H2.loc[(H2['weight_factor'] < 0), 'weight_factor'] = 0
    
    # %% report
    H2['W'] = W_norm

    data_final = data_target[data_target.City == target_city].merge(H2[['Panel_ID','W']], 
        how='left', on='Panel_ID')
    
    data_final['MAX_new'] = data_final['Sales'] * data_final['W']
    
    df_sum = data_final.groupby(['Brand','Year'])[['MAX_new']].agg('sum').reset_index(). \
        pivot_table(index='Brand', columns=['Year'], values='MAX_new',
        fill_value=0, aggfunc='sum')
    df_sum['Share_2018'] = list(np.round(np.array(df_sum['2018']/df_sum['2018'].sum()),3))
    df_sum['Share_2019'] = list(np.round(np.array(df_sum['2019']/df_sum['2019'].sum()),3))
    df_sum['GR'] = list(np.round(np.array(df_sum['2019']/df_sum['2018']-1),3))
    
    return pd.DataFrame([[city_name] + [dict_share]], columns=['City', 'dict'])

schema= StructType([
        StructField("City", StringType(), True),
        StructField("dict", StringType(), True)
        ])
@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def udf_target_brand(pdf):
    return func_target_brand(pdf, dict_target_share)


data_city.groupBy('City').apply()

'''
H = data_target.groupBy('City','Panel_ID','City_Sample','weight') \
                        .pivot('tmp').agg(func.sum('Sales')).fillna(0).persist()

# %% total and others
H = H.withColumn('2018_total', sum(col(y) for y in  [i for i in H.columns if '2018_' in i])) \
     .withColumn('2019_total', sum(col(y) for y in  [i for i in H.columns if '2019_' in i]))

H = H.withColumn('2018_others', col('2018_total') - sum(col(y) for y in ['2018_' + i for i in target])) \
     .withColumn('2019_others', col('2019_total') - sum(col(y) for y in ['2019_' + i for i in target]))
     

# %% 参数设置
G = target_share.select('gr').toPandas()['gr'].tolist()
S = target_share.select('share').toPandas()['share'].tolist()
S_ly = target_share.select('share_ly').toPandas()['share_ly'].tolist()
S_ly = np.array(S_ly)

W_0 = np.array(H.select('weight').toPandas()['weight'].tolist()).reshape(-1,1)

H_18 = H.select(['2018_' + i for i in target]).toPandas().values
H_19 = H.select(['2019_' + i for i in target]).toPandas().values

Ht_18 = H.select('2018_total').toPandas().values
Ht_19 = H.select('2019_total').toPandas().values
'''
# learning rate和迭代次数请通过尝试自定义


# %% 辅助函数
# 计算增长率或者份额
def r(W, h_n, h_d):
    return h_n.T.dot(W)/h_d.T.dot(W)

# 计算和IMS的差距
def delta_gr(W, h_n, h_d, g):
  return (r(W, h_n, h_d) - g)

# Loss-func求导
'''
loss function是差距（份额或增长）的平方
'''
def gradient(W, h_n, h_d, g):
  CrossDiff = (h_d.sum() - W.reshape(-1)*h_d) * h_n - \
              (h_n.sum() - W.reshape(-1)*h_n) * h_d    
  dW = delta_gr(W, h_n, h_d, g) * np.power((h_d.T.dot(W)), -2) * CrossDiff
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

def gradient_descent(W_0, S, G, S_ly, learning_rate, max_iteration, brand_number, H_n=H_19, H_share=Ht_19, H_gr=H_18, H_ly=Ht_18):
    X = np.array([]); Growth = np.array([]); Share = np.array([])
    W = W_0.copy()
    W_init = W_0.copy()
    w_gd = np.array([0]*H_n.shape[1])
    for i in range(max_iteration):
        gradient_sum = np.zeros((len(H_n),1))
        for k in range(H_n.shape[1]):
            gd_share = gradient(W, H_n[:,k], H_share, S[k]) 
            gd_growth = gradient(W, H_n[:,k], H_gr[:,k], G[k])
            '''
            方法一：份额和增长同样重要，Loss func简单加和
            '''
            # gradient_sum += gd_share + gd_growth
            '''
            方法二：注重两年份额的调优
            '''
            # if w_gd[k] > 0.05:
            #     gradient_sum += gd_share
            # else:
            #     gradient_sum +=  gd_growth + gd_share
            '''
            方法三：手动增加Share或者Growth的权重，优化方向自定义
            '''   
            # 加权/降权
            # yyw == 三种，默认第一个  单独gd_share，单独gd_growth，两者一起
            if k in range(brand_number):
                gradient_sum += gradient(W, H_n[:,k], H_gr[:,k], G[k]) + gradient(W, H_n[:,k], H_share, S[k])#1、gradient(W, H_n[:,k], H_gr[:,k], G[k]) + gradient(W, H_n[:,k], H_share, S[k])     2、gd_share
            # 只优化Share
            elif k in []: 
                gradient_sum += gradient(W, H_n[:,k], H_share, S[k])
            # 优化Share和增长
            else:
                gradient_sum += gd_share + gd_growth
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

W_result, X, Growth, Share= gradient_descent(W_0, S, G, S_ly, l, m)

