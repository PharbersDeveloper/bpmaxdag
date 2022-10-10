import os
import pandas as pd
from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
from pyspark.sql import functions as func 
import json
import boto3
from pyspark.sql.functions import lit, col, struct, to_json, json_tuple
from functools import reduce

from functools import reduce
def convert_union_schema(df):
    rows = df.select("schema").distinct().collect()
    return list(reduce(lambda pre, next: set(pre).union(set(next)), list(map(lambda row: [schema["name"] for schema in json.loads(row["schema"])], rows))  ))


# 将统一Schema的DF转成正常的DataFrame
def convert_normal_df(df, cols):
    return df.select(json_tuple(col("data"), *cols)) \
    .toDF(*cols)
#df1 = spark.read.parquet('s3://ph-platform/2020-11-11/lake/pharbers/ejA5i96yvzkkIywWt8zz/PHA_county/traceId=cmax_county_cmax_county_developer_2022-09-16T07%3A04%3A26+00%3A00_袁毓蔚/')
#df1 = convert_normal_df(df1, convert_union_schema(df1))
#df1.count()

#df1.groupby('type').agg(func.sum('value_m_sum')).show()

# +------+------------------+
# |  type| sum(sum(value_m))|
# +------+------------------+
# |Volume| 1491245.841798905|
# | Value|6.75332631453235E8|
# +------+------------------+


# ************  低代码产生的中间文件空值都为None字符串，所以空值判断要注意判断None和null

# cmax_county_data
ZB_CPA_County_181920_v2 = spark.read.csv('s3://ph-max-auto/v0.0.1-2020-06-08/Test/ZB_CPA_DATA_P1_Q3.csv', header=True, encoding='GBK')


# === join: data_join_market
# Pre Filter —— cmax_county_data  packcode 有值 and 不等于None
# pre Computed Columns —— packcode_new：LPAD(`packcode`,7,0)，mkt：market
# cmax_county_market_define[molecule, mkt],  cmax_county_data[ 去掉packcode ]
market_def = spark.read.csv('s3://ph-max-auto/v0.0.1-2020-06-08/Test/market_def.csv', header=True, encoding='GBK')
all_raw_data_m3 = ZB_CPA_County_181920_v2.where(~col('packcode').isNull()) \
                                    .join(market_def, col('Molecule_Desc')==col('molecule'), how='left') \
                                    .withColumn('packcode', func.when(func.length(col('packcode')) < 7, func.lpad(col('packcode'), 6, "0")).otherwise(col('packcode'))) \
                                    .withColumnRenamed('Market', 'MKT')

# === distinct: data_filter_mkt_1   {"codeFree":{"$mkt1":"AD"}}
# Pre Filter —— mkt 等于 $mkt1
AD_ZB_CPA_181920 = all_raw_data_m3.where(col('MKT') == 'AD')



# v3 ===== 可行, 去掉城市 for循环，同时计算 Value 和 Volume =======

# === pivot: pivot_mkt_data
# Pre Filter：Hosp_code 有值
data = AD_ZB_CPA_181920
mkt_data = data.groupby('Province', 'City', 'County',  'Hosp_code', 'packcode', 'Month') \
        .agg(func.sum('Volume').alias('Volume'), func.sum('Dosage_Unit').alias('Dosage_Unit'), func.sum('Value').alias('Value'))
mkt_data_m = mkt_data.where(~col('Hosp_code').isNull()) \
                    .select('Province', 'City', 'County',  'Hosp_code', 'packcode', 'Month', 'Value', 'Volume') \
                    .groupby('Province', 'City', 'County',  'Hosp_code', 'packcode').pivot("Month").agg(func.sum('Value').alias('Value'), func.sum('Volume').alias('Volume'))

# === distinct:PHA_county
PHA_county_final = ZB_CPA_County_181920_v2.select('Hosp_code').distinct() # 为了标记哪些是来自ZB

# === join: cmax_county_universe + PHA_county = join_mkt_universe
# pre Computed Columns —— district：county， hosp_code_zb：hosp_code
# cmax_county_universe[('Hosp_TYPE', 'Panel_ID', 'ISCOUNTY', 'Hosp_level', 'Province', 'City', 'CITYGROUP', 'City_Tier', 'Est_DrugIncome_RMB', '是否综合', 'PHA_Hosp_name', 'Seg', 'District'],PHA_county[ hosp_code_zb ]
# post Computed Columns —— flag_in_zb：if(`hosp_code_zb` is null, 0, 1)
county_universe = spark.read.csv('s3://ph-max-auto/v0.0.1-2020-06-08/Test/county_universe_P1.csv', header=True, encoding='GBK')
county_universe_m = county_universe.toDF(*[i.replace('.','_') for i in county_universe.columns]) \
                    .join(PHA_county_final, col('Panel_ID')==col('Hosp_code'), how='left') \
                    .withColumn('flag_in_zb', func.when(col('Hosp_code').isNull(), func.lit(0)).otherwise( func.lit(1) )) \
                    .withColumnRenamed( 'County', 'District')

# === spark: join_mkt_universe_final
'''
def execute(**kwargs):
    county_universe_m = kwargs['df_join_mkt_universe']
    mkt_data_m = kwargs['df_pivot_mkt_data']
    
    mkt_universe_1 = county_universe_m.join(mkt_data_m.withColumnRenamed('Hosp_code', 'Panel_ID').drop('City'), on=['Province', 'Panel_ID'], how='left')
    mkt_universe = mkt_universe_1.select('Hosp_TYPE', 'Panel_ID', 'PHA_Hosp_name', 'Province', 'City', 'City_Tier',
             'CITYGROUP',  'District', 'ISCOUNTY', 'Hosp_level', 'Seg', 
             '是否综合', 'Est_DrugIncome_RMB',
             'flag_in_zb', 'packcode_new', *[i for i in mkt_universe_1.columns if i.startswith("20")])   
             
    
    out_df = mkt_universe.toDF(*[i.lower() for i in mkt_universe.columns])
    

    return {"out_df": out_df}
'''
mkt_universe_1 = county_universe_m.drop('Hosp_code').join(mkt_data_m.withColumnRenamed('Hosp_code', 'Panel_ID').drop('City'), on=['Province', 'Panel_ID'], how='left')
mkt_universe = mkt_universe_1.select('Hosp_TYPE', 'Panel_ID', 'PHA_Hosp_name', 'Province', 'City', 'City_Tier',
             'CITYGROUP',  'District', 'ISCOUNTY', 'Hosp_level', 'Seg', 
             '是否综合', 'Est_DrugIncome_RMB',
             'flag_in_zb', 'packcode', *[i for i in mkt_universe_1.columns if i.startswith("20")])    

# === distinct:distinct_seg_packids
# Pre Filter:packcode_new 有值 and 不等于None
seg_packids = mkt_universe.select('Province', 'Seg', 'packcode').distinct() \
                        .where(~col('packcode').isNull())

# === join:mkt_universe_non_sample(列名会变)
mkt_universe_non_sample = mkt_universe.where(col('packcode').isNull()).where(col('flag_in_zb') != 1) \
                                    .drop('packcode') \
                                    .join(seg_packids, on=['Province', 'Seg'], how='left')

# === distinct:mkt_universe_sample
mkt_universe_sample = mkt_universe.where(~col('packcode').isNull())

# === spark: unpivot_mkt_universe_m
'''
def execute(**kwargs):
    mkt_universe_sample = kwargs['df_mkt_universe_sample']
    mkt_universe_non_sample = kwargs['df_mkt_universe_non_sample']
    import os
    import pandas as pd
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func 
    import json
    import boto3
    from pyspark.sql.functions import lit, col, struct, to_json, json_tuple
    from functools import reduce
    
    def unpivot(df, keys):
        # 功能：数据宽变长
        # 参数说明 df:dataframe,  keys 待转换表中需要保留的主键key，以list[]类型传入
        # 转换是为了避免字段类不匹配，统一将数据转换为string类型，如果保证数据类型完全一致，可以省略该句
        df = df.select(*[col(_).astype("string") for _ in df.columns])
        cols = [_ for _ in df.columns if _ not in keys]
        stack_str = ','.join(map(lambda x: "'%s', `%s`" % (x, x), cols))
        # feature, value 转换后的列名，可自定义
        df = df.selectExpr(*[f"`{i}`" for i in keys], "stack(%s, %s) as (feature, value)" % (len(cols), stack_str))
        return df
    
    mkt_universe_m = mkt_universe_sample.union(mkt_universe_non_sample.select(mkt_universe_sample.columns))
    mkt_universe_m_1 = unpivot(mkt_universe_m.drop('version'), ['hosp_type', 'panel_id', 'pha_hosp_name', 'province', 'city', 'city_tier', 'citygroup', 'district', 'iscounty', 'hosp_level', 'seg', '是否综合', 'est_drugincome_rmb', 'flag_in_zb', 'packcode_new']) \
                    .withColumn('value', func.when((col('flag_in_zb') == 1) & (col('value').isNull()),  0).otherwise(col('value')) ) \
                    .withColumnRenamed('feature', 'date_type')
                    
    mkt_universe_m_1 = mkt_universe_m_1.toDF(*[i.lower() for i in mkt_universe_m_1.columns])
    
    return {"out_df": mkt_universe_m_1}
'''
def unpivot(df, keys):
    # 功能：数据宽变长
    # 参数说明 df:dataframe,  keys 待转换表中需要保留的主键key，以list[]类型传入
    # 转换是为了避免字段类不匹配，统一将数据转换为string类型，如果保证数据类型完全一致，可以省略该句
    df = df.select(*[col(_).astype("string") for _ in df.columns])
    cols = [_ for _ in df.columns if _ not in keys]
    stack_str = ','.join(map(lambda x: "'%s', `%s`" % (x, x), cols))
    # feature, value 转换后的列名，可自定义
    df = df.selectExpr(*[f"`{i}`" for i in keys], "stack(%s, %s) as (feature, value)" % (len(cols), stack_str))
    return df
mkt_universe_m = mkt_universe_sample.union(mkt_universe_non_sample.select(mkt_universe_sample.columns))
mkt_universe_m_1 = unpivot(mkt_universe_m, ['Hosp_TYPE', 'Panel_ID', 'PHA_Hosp_name', 'Province', 'City', 'City_Tier', 'CITYGROUP', 'District', 'ISCOUNTY', 'Hosp_level', 'Seg', '是否综合', 'Est_DrugIncome_RMB', 'flag_in_zb', 'packcode']) \
                    .withColumn('value', func.when((col('flag_in_zb') == 1) & (col('value').isNull()),  0).otherwise(col('value')) ) \
                    .withColumnRenamed('feature', 'date')

# 没有开窗函数的函数，只能多个groupby再join
from pyspark.sql import Window

# === groupby:mkt_universe_m_groupby1： mean_value，Est_DrugIncome_RMB_mean
mkt_universe_m_2 = mkt_universe_m_1.withColumn('mean_value', func.mean('value').over(Window.partitionBy('Province', 'Seg', 'flag_in_zb', 'packcode', 'date').orderBy())) \
                                    .withColumn('Est_DrugIncome_RMB', func.mean('Est_DrugIncome_RMB').over(Window.partitionBy('Province', 'Seg', 'flag_in_zb', 'packcode', 'date').orderBy())) \
                                    .withColumn('sales_est_ratio', col('mean_value')/col('Est_DrugIncome_RMB') )

# === groupby:mkt_universe_m_groupby2：flag_in_zb_sum, count
mkt_universe_m_3 = mkt_universe_m_2.withColumn('sample_ratio', func.sum('flag_in_zb').over(Window.partitionBy('Province', 'Seg', 'packcode', 'date').orderBy())) \
                                    .withColumn('n', func.count('date').over(Window.partitionBy('Province', 'Seg', 'packcode', 'date').orderBy())) \
                                    .withColumn('sample_ratio', col('sample_ratio')/col('n') ) \
                                    .withColumn('sales_est_ratio_m', func.max('sales_est_ratio').over(Window.partitionBy('Province', 'Seg', 'packcode', 'date').orderBy()))
# === groupby:mkt_universe_m_groupby3: sales_est_ratio_double 设置为double，sales_est_ratio_max


# === join: mkt_universe_sale_sample_ratio：mkt_universe_m_groupby1 + mkt_universe_m_groupby2     ### 问题？？？？sales_est_ratio 生成后已经指定为double，但是仍然为字符型
# post Computed Columns —— sales_est_ratio:`value_avg`/`est_drugincome_rmb_avg`, sample_ratio:`flag_in_zb_sum`/`count`
# === join: mkt_universe_m_join：mkt_universe_sale_sample_ratio +  mkt_universe_m_groupby3
# post Computed Columns —— value_m:if((`flag_in_zb` ==1 or `sample_ratio` >=0.8), `value`, `est_drugincome_rmb_avg`*`sales_est_ratio_max`)
mkt_universe_m_4 = mkt_universe_m_3.withColumn('value_m', func.when((col('flag_in_zb') == 1) | (col('sample_ratio') >= 0.8 ),  col('value')).otherwise(col('Est_DrugIncome_RMB')*col('sales_est_ratio_m')) )


# distinct :jidu_yuefen
jidu_yuefen = data.select('Province', 'Quarter','Month').distinct() \
                  .withColumnRenamed('Month', 'date')  

# === groupby：mkt_universe_m_groupby
mkt_universe_m_groupby = mkt_universe_m_4.groupby('Province','packcode', 'flag_in_zb', 'date', 'city', 'district').agg(func.sum('value_m'))

# value_m_sum
# === spark:mkt_universe_m_split
'''
def execute(**kwargs):
    data_frame = kwargs['df_mkt_universe_m_groupby']
    
    from pyspark.sql import functions as func 
    df_out = data_frame.withColumn('type', func.split(func.col('date_type'), '_')[1] ) \
                        .withColumn('date', func.split(func.col('date_type'), '_')[0] ) \

    return {"out_df": df_out}
'''
mkt_universe_m_split = mkt_universe_m_groupby.withColumn('type', func.split(col('date'), '_')[1] ) \
                                    .withColumn('date', func.split(col('date'), '_')[0] ) 

# join: project_data_7pack
project_data_7pack = mkt_universe_m_split.join(jidu_yuefen, on=['Province', 'date'], how='left')



# +--------+-------+--------+----------+------------+------+-------+
# |Province|   date|packcode|flag_in_zb|sum(value_m)|  type|Quarter|
# +--------+-------+--------+----------+------------+------+-------+
# |  重庆市|2021/09|    null|         0|        null| Value|   null|
# |  重庆市|2021/07|    null|         0|        null| Value|   null|
# |  重庆市|2021/08|    null|         0|        null| Value|   null|
# |  重庆市|2021/09|    null|         0|        null|Volume|   null|
# |  重庆市|2021/08|    null|         0|        null|Volume|   null|
# |  重庆市|2021/07|    null|         0|        null|Volume|   null|
# +--------+-------+--------+----------+------------+------+-------+


#  宁夏回族自治区|    3: 因为有几个月的数据为null，但是R是分省份的所有不会出现没有数据的date
#+--------------+-------+--------+----------+------------------+-------+
#|      Province|   date|packcode|flag_in_zb|      sum(value_m)|Quarter|
#+--------------+-------+--------+----------+------------------+-------+
#|宁夏回族自治区|2021/07| 5772202|         1|               0.0|   null|
#|宁夏回族自治区|2021/09|    null|         0|              null| 2021Q3|
#|宁夏回族自治区|2021/07| 5772202|         0|               0.0|   null|
#|宁夏回族自治区|2021/08| 5772202|         0|               0.0|   null|
#|宁夏回族自治区|2021/09| 5772202|         1|            9200.0| 2021Q3|
#|宁夏回族自治区|2021/07|    null|         0|              null|   null|
#|宁夏回族自治区|2021/08| 5772202|         1|               0.0|   null|
#|宁夏回族自治区|2021/08|    null|         0|              null|   null|
#|宁夏回族自治区|2021/09| 5772202|         0|19376.251880902648| 2021Q3|


project_data_7pack.groupby('type').count().show()
# 1581
# +------+-----+
# |  type|count|
# +------+-----+
# |Volume| 1590|
# | Value| 1590|
# +------+-----+
# 调整后
# +------+-----+
# |  type|count|
# +------+-----+
# |Volume|37581|
# | Value|37581|
# +------+-----+


#project_data_7pack.where(col('type')=='Value').groupby('Province').count().orderBy('count').show(30)

# +----------------+-----+
# |        Province|count|
# +----------------+-----+
# |      西藏自治区|   27|
# |          吉林省|   63|
# |          云南省|   63|
# |          浙江省|   63|
# |          甘肃省|   33|
# |          四川省|   39|
# |          青海省|   39|
# |          江苏省|  105|
# |          海南省|   21|
# |          福建省|   57|
# |    内蒙古自治区|   69|
# |          贵州省|   57|
# |          广东省|   21|
# |新疆维吾尔自治区|   81|
# |          河南省|   87|
# |  广西壮族自治区|   60|
# |          河北省|   63|
# |  宁夏回族自治区|    9|
# |          山西省|   69|
# |          湖北省|   66|
# +----------------+-----+

project_data_7pack.groupby('type').agg(func.sum('sum(value_m)')).show()

# AD
# +------+------------------+
# |  type| sum(sum(value_m))|
# +------+------------------+
# |Volume| 1491245.841798905|
# | Value|6.75332631453235E8|
# +------+------------------+

# RA
# +------+-------------------+
# |  type|  sum(sum(value_m))|
# +------+-------------------+
# |Volume|  350945.8940055265|
# | Value|2.119262210860441E8|
# +------+-------------------+

# RCC
# +------+--------------------+
# |  type|   sum(sum(value_m))|
# +------+--------------------+
# |Volume|   67122.96549126094|
# | Value|2.2671996300269192E8|
# +------+--------------------+