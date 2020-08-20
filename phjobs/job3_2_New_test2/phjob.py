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
from pyspark.sql.functions import pandas_udf, PandasUDFType

import pandas as pd
import numpy as np
from scipy.spatial import distance
import math

#def execute(a, b):
spark = SparkSession.builder \
    .master("yarn") \
    .appName("data from s3") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.instance", "1") \
    .config("spark.executor.memory", "3g") \
    .config('spark.sql.codegen.wholeStage', False) \
    .config("spark.sql.execution.arrow.enabled", "true") \
    .config("spark.sql.execution.arrow.maxRecordsPerBatch", 10000) \
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
        
    
# 输入
df_sales_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/Sales_right"
df_units_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/Units_right"

data_complete_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/data_complete"
weidao_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/2019年未到名单_v2.csv"
universe_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/universe"
cpa_pha_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/cpa_pha_mapping"
prod_map_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/prod_mapping"
prod_ref_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/cn_prod_ref"
mnf_ref_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/cn_mnf_ref"

# 输出
data_missing_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/Out/data_missing_tmp"

# ==========

#%% Read-in 
data_sale = spark.read.parquet(df_sales_path)
data_unit = spark.read.parquet(df_units_path)

data_sale = data_sale.select([func.col(col).alias(col + "_Sales") for col in data_sale.columns]) \
                .withColumnRenamed("ID_Sales", "ID") \
                .withColumnRenamed("pfc_Sales", "pfc")
data_unit = data_unit.select([func.col(col).alias(col + "_Units") for col in data_unit.columns]) \
                .withColumnRenamed("ID_Units", "ID") \
                .withColumnRenamed("pfc_Units", "pfc")

data=data_sale.join(data_unit, on=['ID','pfc'], how = 'inner')

#原始数据转置版
weidao = spark.read.csv(weidao_path, header=True)
#未到名单

#%% 产品匹配
prod_map = spark.read.parquet(prod_map_path)
prod_map = prod_map.select('pfc','标准通用名').distinct()

#%% 匹配省份城市信息
universe = spark.read.parquet(universe_path)
cpa_pha = spark.read.parquet(cpa_pha_path)

hosp_info = universe.where(universe["重复"] == "0").select('新版ID', '新版名称', 'Hosp_level', 'Province', 'City')
cpa_pha = cpa_pha.where(cpa_pha["推荐版本"] == "1").select('ID','PHA')

#%% mnc map
prod_ref = spark.read.parquet(prod_ref_path)
mnf_ref = spark.read.parquet(mnf_ref_path)

mnc_map = prod_ref.select('Pack_Id','MNF_ID') \
            .join(mnf_ref.select('MNF_ID','MNF_TYPE'), on="MNF_ID", how="inner") \
            .drop('MNF_ID') \
            .withColumnRenamed("Pack_Id", "pfc")
            
#%% VBP mark
data_complete = spark.read.parquet(data_complete_path)

# *************  这样配置的VBP_mole是first, 有问题，后面的调用还是会出现随机first  
vbp_mark = data_complete.repartition(1).select('pfc','VBP_mole').drop_duplicates(['pfc'])

#%% 匹配各种信息
data_pha = data.join(cpa_pha, on='ID', how = 'left')
data_info = data_pha.join(hosp_info, data_pha["PHA"]==hosp_info["新版ID"], how = 'left')

data_prod = data_info.join(prod_map,  on = 'pfc', how = 'left') \
        .join(mnc_map, on='pfc', how = 'left') \
        .join(vbp_mark.repartition(1), on='pfc', how='left')

data_prod = data_prod.withColumn("Province" , \
            func.when(data_prod.City.isin('大连市','沈阳市','西安市','厦门市','广州市', '深圳市','成都市'), data_info.City). \
            otherwise(data_prod.Province))
            
#%% 拆分数据
data_non_vbp = data_prod.where(data_prod.VBP_mole == "False") \
            .drop(*[i for i in data_prod.columns if "Units" in i])
for eachcol in data_non_vbp.columns:
    if "_Sales" in eachcol:
        data_non_vbp = data_non_vbp.withColumnRenamed(eachcol, eachcol.replace("_Sales", ""))

data_vbp = data_prod.where(data_prod.VBP_mole == "True") \
            .drop(*[i for i in data_prod.columns if "Sales" in i])
for eachcol in data_vbp.columns:
    if "_Units" in eachcol:
        data_vbp = data_vbp.withColumnRenamed(eachcol, eachcol.replace("_Units", ""))

data_mnc = data_vbp.where(data_vbp.MNF_TYPE != 'L')

data_local = data_vbp.where(data_vbp.MNF_TYPE == 'L')   

# =============== 函数定义 ================

# NearestHosp 内部 udf 函数
def pandas_udf_NearestHosp_func(data_level_province, level, min_level, date_hist, k):
    
    import pandas as pd
    import numpy as np
    from scipy.spatial import distance
    import math
    import json
    
    # 分组后的 level 和 Province
    level_name = data_level_province[level][0].astype(str)
    Province = data_level_province["Province"][0]
    
    # 每个SKU下的政策区域的字典
    dict_prov = {}
    dict_prov['TOP1'] = []
    # 按政策区域分组
    
    # ***yyw*** data_mole = data[data[level] == m]，data_mole[data_mole.Province == p] 就是 data_level_province

    data_prov_wide = data_level_province[['ID', min_level] + date_hist].set_index('ID', drop=True)

    # data_prov_wide=data_mole[data_mole.Province == p] \
    #     [['ID',level]+date_hist].fillna(0). \
    #         groupby(['ID',level]).sum().reset_index(). \
    #             set_index('ID',drop=True)

    # 判断每个SKU-政策区域下面历史时间的TOP 1医院(TOP 1在未来会变吗)
    top_1 = data_prov_wide.sum(1, skipna=True).idxmax(0)
    dict_prov['TOP1'].append(top_1)

    # 选取历史时间的数据（这里需要设置参数）
    data_prov = data_level_province. \
        pivot(index='ID',
              columns=min_level,
              values=data_level_province.columns[data_level_province.columns. \
              str.contains('Date2018')]). \
        fillna(0)

    n = k

    if len(data_prov) <= k:
        n = len(data_prov) - 1

    disttmp = distance.cdist(data_prov, data_prov, 'euclidean')

    disttmp[np.diag_indices_from(disttmp)] = np.inf  # 解决对角点排序问题

    disttmp = pd.DataFrame(disttmp,
                           index=data_prov.index,
                           columns=data_prov.index)

    a = disttmp.apply(lambda x: x.index[np.argpartition(x, (0, n))],
                      axis=0). \
        reset_index(drop=True)
        
    # ***yyw*** python2没有ignore_index=True，用python2的方法.values替换
    c = disttmp.apply(lambda x: x.sort_values().values, axis=0). \
        reset_index(drop=True)

    b = a.iloc[0:n]  # 临近医院
    d = c.iloc[0:n]  # Distance

    for i in disttmp.columns:
        dict_hosp = {}
        dict_hosp['NN'] = b[i].tolist()
        dict_hosp['Dist'] = [(c + 1) ** 3 for c in d[i].tolist()]  # 去掉0再平方
        dict_prov[i] = dict_hosp
            
    dict_prov = json.dumps(dict_prov)  
    
    return pd.DataFrame([[level_name] + [Province] + [dict_prov]], columns=["level", "Province", "dict"])

    

#%% SKU层面找相似
def NearestHosp(data, level, period=(201801,201812,201912), k = 3):
    
    start_point = period[0]
    end_point = period[1]
    current_point = period[2]
    
    min_level = str(np.where('pfc' in data.columns, 'pfc', 'min2'))
    
    date_list_num = []
    for y in range(start_point // 100, current_point // 100 + 1):
        date_list_num += list(range(np.where(start_point > y * 100 + 1, start_point, y * 100 + 1),
                                    np.where(current_point < y * 100 + 12, current_point + 1, y * 100 + 12 + 1)))
    
    date_list = ['Date' + str(d) for d in date_list_num]
    
    date_hist = date_list[date_list_num.index(start_point):date_list_num.index(end_point) + 1]
    
    
    schema= StructType([
        StructField("level", StringType(), True),
        StructField("Province", StringType(), True),
        StructField("dict", StringType(), True)
        ])
    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)    
    def pandas_udf_NearestHosp(df):
        return pandas_udf_NearestHosp_func(df, level, min_level, date_hist, k)
        
    # SKU的字典
    dict_level = data.groupby([level, "Province"]).apply(pandas_udf_NearestHosp)
    
    return dict_level
    
# %% SKU层面的临近医院名单
near_hosp_non_vbp_sku = NearestHosp(data_non_vbp, level='pfc')

near_hosp_non_vbp_sku.show()


