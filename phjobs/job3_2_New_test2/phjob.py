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
import json

#def execute(a, b):
spark = SparkSession.builder \
    .master("yarn") \
    .appName("data from s3") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.instance", "1") \
    .config("spark.executor.memory", "2g") \
    .config('spark.sql.codegen.wholeStage', False) \
    .config("spark.sql.execution.arrow.enabled", "false") \
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

universe_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/universe"
cpa_pha_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/cpa_pha_mapping"
prod_map_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/prod_mapping"

MNFPath = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/MNF_TYPE_PFC"
VBPPath = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/Zhongbiao"

# 输出
data_missing_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/Out/data_missing_tmp"

# ==========

'''
PART 2
生成近邻名单，并且保存全局
'''

#%% Read-in 
df_sales = spark.read.parquet(df_sales_path)
df_units = spark.read.parquet(df_units_path)

df_sales = df_sales.select([func.col(col).alias(col + "_Sales") for col in df_sales.columns]) \
                .withColumnRenamed("ID_Sales", "ID") \
                .withColumnRenamed("pfc_Sales", "pfc")
df_units = df_units.select([func.col(col).alias(col + "_Units") for col in df_units.columns]) \
                .withColumnRenamed("ID_Units", "ID") \
                .withColumnRenamed("pfc_Units", "pfc")

df=df_sales.join(df_units, on=['ID','pfc'], how = 'inner')


#%% 产品匹配
universe = spark.read.parquet(universe_path)
cpa_pha = spark.read.parquet(cpa_pha_path)
hosp_info = universe.where(universe["重复"] == "0").select('新版ID', '新版名称', 'Hosp_level', 'Province', 'City')
cpa_pha = cpa_pha.where(cpa_pha["推荐版本"] == "1").select('ID','PHA')

mnf_map = spark.read.parquet(MNFPath)
vbp_map = spark.read.parquet(VBPPath)
vbp_map = vbp_map.select('pfc','药品通用名_标准').distinct()

prod_map = spark.read.parquet(prod_map_path)
prod_map = prod_map.select('pfc','标准通用名').distinct()

#%% 匹配各种信息
data_prod = df.join(cpa_pha, on='ID', how = 'left')
data_prod = data_prod.join(hosp_info, data_prod["PHA"]==hosp_info["新版ID"], how = 'left') \
        .join(mnf_map,  on = 'pfc', how = 'left') \
        .join(prod_map, on='pfc', how = 'left')
        
vbp_map_pfc = vbp_map.select("pfc").distinct().toPandas()["pfc"].tolist()        
data_prod = data_prod.withColumn("VBP_prod", func.when(data_prod.pfc.isin(vbp_map_pfc), func.lit("True")).otherwise(func.lit("False")))
# 有中文的药品通用名_标准 ，toPandas()报错
data_prod = data_prod.join(vbp_map.select("药品通用名_标准").distinct(), data_prod[u"标准通用名"] == vbp_map[u"药品通用名_标准"], how="left")
data_prod = data_prod.withColumn("VBP_mole", func.when(func.isnull(data_prod["药品通用名_标准"]), func.lit("False")).otherwise(func.lit("True")))
data_prod = data_prod.drop("药品通用名_标准")

data_prod = data_prod.withColumn("Province" , \
            func.when(data_prod.City.isin('大连市','沈阳市','西安市','厦门市','广州市', '深圳市','成都市'), data_prod.City). \
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


# NearestHosp 内部 udf 函数
def pandas_udf_NearestHosp_func(data_level, level, min_level, date_hist, k):
    import pandas as pd
    import numpy as np
    from scipy.spatial import distance
    import math
    import json
    import sys
    reload(sys)
    sys.setdefaultencoding('utf-8')
    
    # 分组后的 level
    level_name = data_level[level][0].astype(str)
    dict_level ={}
    
    # 每个SKU下的政策区域的字典
    dict_prov = {}
    dict_prov['TOP1'] = []
    
    # 按政策区域分组
    # ***yyw*** data_mole = data[data[level] == m] 就是 data_level
    for p in data_level.Province.unique().tolist():
        data_prov_wide=data_level[data_level.Province == p] \
                [['ID',min_level]+date_hist].set_index('ID',drop=True)
        # data_prov_wide=data_mole[data_mole.Province == p] \
        #     [['ID',level]+date_hist].fillna(0). \
        #         groupby(['ID',level]).sum().reset_index(). \
        #             set_index('ID',drop=True)
        # 判断每个SKU-政策区域下面历史时间的TOP 1医院(TOP 1在未来会变吗)
        top_1 = data_prov_wide.sum(1, skipna=True).idxmax(0)
        dict_prov['TOP1'].append(top_1)
        # 选取历史时间的数据（这里需要设置参数）
        data_prov = data_level[data_level.Province == p]. \
            pivot(index='ID',
                  columns=min_level,
                  values=data_level.columns[data_level.columns. \
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
        
    dict_level[level_name] = dict_prov        
    dict_level = json.dumps(dict_level)  
    
    return pd.DataFrame([[level_name] + [dict_level]], columns=["level", "dict"])

    
#%% SKU层面找相似
def NearestHosp(data, level, period=(201801,201812,201912), k = 3):
    #near_hosp_non_vbp_sku = NearestHosp(data_non_vbp, level='pfc')    
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
    
    # SKU的字典
    
    # 分组计算
    schema= StructType([
        StructField("level", StringType(), True),
        #StructField("Province", StringType(), True),
        StructField("dict", StringType(), True)
        ])
    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)    
    def pandas_udf_NearestHosp(df):
        return pandas_udf_NearestHosp_func(df, level, min_level, date_hist, k)
        
    # 输出 level，dict
    dict_level = data.groupby([level]).apply(pandas_udf_NearestHosp)
    
    return dict_level

# *** 输出，再读入，缓解内存 ***

# %% SKU层面的临近医院名单
#  data_non_vbp
'''
df_near_hosp_non_vbp_sku = NearestHosp(data_non_vbp, level='pfc')
df_near_hosp_non_vbp_sku = df_near_hosp_non_vbp_sku.repartition(1)
df_near_hosp_non_vbp_sku.write.format("parquet") \
    .mode("overwrite").save("s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/Out/df_near_hosp_non_vbp_sku")
'''
df_near_hosp_non_vbp_sku = spark.read.parquet("s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/Out/df_near_hosp_non_vbp_sku")
print("df_near_hosp_non_vbp_sku")

# 转化为字典格式
df_near_hosp_non_vbp_sku = df_near_hosp_non_vbp_sku.groupBy().agg(func.collect_list('dict').alias('dict_all')).select("dict_all").toPandas()
df_near_hosp_non_vbp_sku = df_near_hosp_non_vbp_sku["dict_all"].values[0]
length_dict = len(df_near_hosp_non_vbp_sku)

dict_near_hosp_non_vbp_sku = ""
for index, each in enumerate(df_near_hosp_non_vbp_sku):
    if index == 0:
        dict_near_hosp_non_vbp_sku += "{" + each[1:-1]
    elif index == length_dict - 1:
        dict_near_hosp_non_vbp_sku += "," + each[1:-1] + "}"
    else:
        dict_near_hosp_non_vbp_sku += "," + each[1:-1]
near_hosp_non_vbp_sku  = json.loads(dict_near_hosp_non_vbp_sku)

# data_mnc
'''
df_near_hosp_mnc_sku = NearestHosp(data_mnc, level='pfc')
df_near_hosp_mnc_sku = df_near_hosp_mnc_sku.repartition(1)
df_near_hosp_mnc_sku.write.format("parquet") \
    .mode("overwrite").save("s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/Out/df_near_hosp_mnc_sku")
'''
df_near_hosp_mnc_sku = spark.read.parquet("s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/Out/df_near_hosp_mnc_sku")
print("df_near_hosp_mnc_sku")

# 转化为字典格式
df_near_hosp_mnc_sku = df_near_hosp_mnc_sku.groupBy().agg(func.collect_list('dict').alias('dict_all')).select("dict_all").toPandas()
df_near_hosp_mnc_sku = df_near_hosp_mnc_sku["dict_all"].values[0]
length_dict = len(df_near_hosp_mnc_sku)

dict_near_hosp_mnc_sku = ""
for index, each in enumerate(df_near_hosp_mnc_sku):
    if index == 0:
        dict_near_hosp_mnc_sku += "{" + each[1:-1]
    elif index == length_dict - 1:
        dict_near_hosp_mnc_sku += "," + each[1:-1] + "}"
    else:
        dict_near_hosp_mnc_sku += "," + each[1:-1]
near_hosp_mnc_sku  = json.loads(dict_near_hosp_mnc_sku)


'''
PART 3
补充数据
'''
# %% Moving average先补一批non-VBP
def pandas_udf_MovAvg_func(data_mole, level, date, near_hosp_sku):
    import pandas as pd
    import numpy as np
    from scipy.spatial import distance
    import math
    import json
    import sys
    reload(sys)
    sys.setdefaultencoding('utf-8')
    
    # 分组后的 level
    level_name = data_mole[level][0].astype(str)
    near_hosp = near_hosp_sku[level_name]
    
    date_list_num = []
    # 这里直接把年份里的月都表示出来吧
    for y in range((date - 4) // 100, date // 100 + 1):
        date_list_num += list(range(y * 100 + 1, y * 100 + 12 + 1))
    date_list = ['Date' + str(d) for d in date_list_num]
    # Top1医院是在每个政策区域的每个SKU上都有一个，并且必须和NN同步
    current_col = 'Date' + str(date)
    his_col = date_list[date_list.index(current_col) - 3:date_list.index(current_col)]
    # 当月
    data_month = data_mole[['ID', level, current_col]]
    # 当月未到
    data_month_na = data_month.loc[data_month[current_col].isna(),
                                   ['ID', level, current_col]]. \
        reset_index(drop=True)
    # 未到前三月
    data_month_na_his = data_mole.loc[data_mole[current_col].isna(), his_col]. \
        reset_index(drop=True)
    for i in range(len(data_month_na)):
        # *** yyw *** 数据类型统一，否则字典找不到key,near_hosp[na_id] id是字符串， near_hosp['TOP1']里面储存的是数字list
        na_id = data_month_na.loc[i].ID.astype("int").astype("str")
        na_min = data_month_na.loc[i][level]
        if (len(near_hosp[na_id]['NN']) < 3) | (int(na_id) in near_hosp['TOP1']):
            data_month_na.loc[i, current_col] = data_month_na_his.loc[i]. \
                mean(skipna=True)
                
    # ** yyw ** 重整数据
    '''
    data_month_na["Date"] = current_col
    data_month_na = data_month_na.rename(columns={current_col: 'value'})
    data_month["Date"] = current_col
    data_month = data_month.rename(columns={current_col: 'value'})
    '''
    data_month = pd.concat([data_month, data_month_na])
    data_month = data_month[~data_month[current_col].isna()]
    
    # *** yyw *** 数据类型,int 报错
    data_month[['ID', level]] = data_month[['ID', level]].astype("str")
    return data_month
    

def MovAvg(data, level, date, near_hosp_sku):
    # Top1医院是在每个政策区域的每个SKU上都有一个，并且必须和NN同步
    
    # 分组计算
    current_date = 'Date' + str(date)

    schema= StructType([
        StructField("ID", StringType(), True),
        StructField(level, StringType(), True),
        StructField(current_date, DoubleType(), True)
        # StructField("Date", StringType(), True)
        ])
    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)    
    def pandas_udf_MovAvg(df):
        return pandas_udf_MovAvg_func(df, level, date, near_hosp_sku)
    # 输出 level，dict
    result = data.groupby([level]).apply(pandas_udf_MovAvg)
    result = result.withColumn("ID", result["ID"].cast(IntegerType())) \
            .withColumn(level, result[level].cast(IntegerType()))
    return result


#%% ma
## 这里怎么优化？
for m in range(201901, 201912 + 1):
    if m == 201901:
        result_ma = data_non_vbp
    else:
        if (m % 2) == 0:
            result_ma = spark.read.parquet("s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/Out/result_ma_tmp_1")
        else:
            result_ma = spark.read.parquet("s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/Out/result_ma_tmp_2")
        #result_ma.repartition(1).createOrReplaceTempView('my_table')
        #spark.catalog.refreshTable("my_table")
    result_month = MovAvg(result_ma, 'pfc', m, near_hosp_non_vbp_sku)
    result_ma = result_ma.drop('Date' + str(m))
    result_ma = result_ma.join(result_month, on=['ID', 'pfc'], how='left')
    if (m % 2) == 0:
        result_ma.repartition(1).write.format("parquet") \
            .mode("overwrite").save("s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/Out/result_ma_tmp_2")
    else:
        result_ma.repartition(1).write.format("parquet") \
            .mode("overwrite").save("s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/Out/result_ma_tmp_1")
    
result_ma = result_ma.repartition(2)
result_ma.write.format("parquet") \
    .mode("overwrite").save("s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/Out/result_ma")
result_ma = spark.read.parquet("s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/Out/result_ma")



# ====================  
# %% nn

# %% 补充数据的func: MA先于KNN
def pandas_udf_NHospImpute_func(data_mole, level, date, near_hosp_sku, min_level):
    import pandas as pd
    import numpy as np
    from scipy.spatial import distance
    import math
    import json
    import sys
    reload(sys)
    sys.setdefaultencoding('utf-8')
    # 分组后的 level
    level_name = data_mole[level][0].astype(str)
    near_hosp = near_hosp_sku[level_name]
    # result = pd.DataFrame()
    current_col = 'Date' + str(date)
    data_month = data_mole[['ID', min_level, current_col]]
    data_month_na = data_month.loc[data_month[current_col].isna(),
                                   ['ID', min_level, current_col]]. \
        reset_index(drop=True)
    for i in range(len(data_month_na)):
        na_id = data_month_na.loc[i].ID.astype("int").astype("str")
        na_min = data_month_na.loc[i][min_level].astype("int")
        if (len(near_hosp[na_id]['NN']) == 3):
            nn_values = [np.hstack((data_month.loc[(data_month.ID == near_hosp[na_id]['NN'][c]) &
                                                   (data_month[min_level] == na_min),
                                                   current_col].values, np.nan))[0] for c in [0, 1, 2]]
            nn_values = [max(x, 0) for x in nn_values]
            nn_weights = [1 / w for w in near_hosp[na_id]['Dist']]
            nn_values_ma = np.ma.array(nn_values, mask=np.isnan(np.array(nn_values)))
            data_month_na.loc[i, current_col] = np.ma.average(nn_values_ma, weights=nn_weights) * 1.05
    data_month = pd.concat([data_month, data_month_na])
    data_month = data_month[~data_month[current_col].isna()]
    # result = pd.concat([result, data_month])
    # *** yyw *** 数据类型,int 报错
    data_month[['ID', level]] = data_month[['ID', level]].astype("str")
    return data_month
                
                
def NHospImpute(data, level, date, near_hosp_non_vbp_sku):
    min_level = str(np.where('pfc' in data.columns, 'pfc', 'min2'))
    
    # 分组计算
    current_date = 'Date' + str(date)
    
    schema= StructType([
        StructField("ID", StringType(), True),
        StructField(level, StringType(), True),
        StructField(current_date, DoubleType(), True)
        # StructField("Date", StringType(), True)
        ])
    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)    
    def pandas_udf_MovAvg(df):
        return pandas_udf_NHospImpute_func(df, level, date, near_hosp_non_vbp_sku, min_level)
    # 输出 level，dict
    result = data.groupby([level]).apply(pandas_udf_MovAvg)
    result = result.withColumn("ID", result["ID"].cast(IntegerType())) \
            .withColumn(level, result[level].cast(IntegerType()))
    return result
    
# %% nn  
result_non_vbp = data_non_vbp.select(['ID', 'pfc'] + [i for i in data_non_vbp.columns if "Date2018" in i])
for m in range(201901, 201912 + 1):
    result_month = NHospImpute(result_ma, 'pfc', m, near_hosp_non_vbp_sku)
    result_non_vbp = result_non_vbp.join(result_month, on=['ID', 'pfc'], how='left')
result_non_vbp = result_non_vbp.fillna(0)    

result_non_vbp = result_non_vbp.repartition(2)
result_non_vbp.write.format("parquet") \
    .mode("overwrite").save("s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/Out/result_non_vbp")
result_non_vbp = spark.read.parquet("s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/Out/result_non_vbp")

# %% MNC
result_mnc = data_mnc.select(['ID', 'pfc'] + [i for i in data_mnc.columns if "Date2018" in i])
for m in range(201901, 201912 + 1):
    result_month = NHospImpute(data_mnc, 'pfc', m, near_hosp_mnc_sku)
    result_mnc = result_mnc.join(result_month, on=['ID', 'pfc'], how='left')
    
result_mnc = result_mnc.repartition(2)
result_mnc.write.format("parquet") \
    .mode("overwrite").save("s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/Out/result_mnc")
result_mnc = spark.read.parquet("s3a://ph-max-auto/v0.0.1-2020-06-08/New_add_test/Out/result_mnc")

# %% Local
result_local = data_local
