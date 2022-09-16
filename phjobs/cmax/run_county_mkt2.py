import os
import pandas as pd
from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
from pyspark.sql import functions as func 
import json
import boto3
from pyspark.sql.functions import lit, col, struct, to_json, json_tuple
from functools import reduce
from pyspark.sql import Window

from functools import reduce
def convert_union_schema(df):
    rows = df.select("schema").distinct().collect()
    return list(reduce(lambda pre, next: set(pre).union(set(next)), list(map(lambda row: [schema["name"] for schema in json.loads(row["schema"])], rows))  ))


# 将统一Schema的DF转成正常的DataFrame
def convert_normal_df(df, cols):
    return df.select(json_tuple(col("data"), *cols)) \
    .toDF(*cols)
df = spark.read.parquet('s3://ph-platform/2020-11-11/lake/pharbers/ejA5i96yvzkkIywWt8zz/mkt2_volume/traceId=cmax_county_cmax_county_developer_2022-09-16T02%3A54%3A10+00%3A00_袁毓蔚/')
df = convert_normal_df(df, convert_union_schema(df))
df.count()



# 参数：{"codeFree":{"$mkt2":"细菌"}}

ZB_CPA_County_181920_v2 = spark.read.csv('s3://ph-max-auto/v0.0.1-2020-06-08/Test/ZB_CPA_DATA_P1_Q3.csv', header=True, encoding='GBK')

# === join: data_join_market（空值判断：有值 and 不等于 None）（#LPAD(`packcode`,7,0)）
# Pre Filter: packcode 有值，不等于None
# pre Computed Columns —— packcode_new：LPAD(`packcode`,7,0)， mkt：market
market_def = spark.read.csv('s3://ph-max-auto/v0.0.1-2020-06-08/Test/market_def.csv', header=True, encoding='GBK')
all_raw_data_m3 = ZB_CPA_County_181920_v2.where(~col('packcode').isNull()) \
                                    .join(market_def, col('Molecule_Desc')==col('molecule'), how='left') \
                                    .withColumn('packcode', func.when(func.length(col('packcode')) < 7, func.lpad(col('packcode'), 6, "0")).otherwise(col('packcode'))) \
                                    .withColumnRenamed('Market', 'MKT')

# === distinct: data_filter_mkt_2   {"codeFree":{"$mkt2":"细菌"}}
# Pre Filter: $mkt2
sample_hosp_raw = all_raw_data_m3.where(col('MKT') == '细菌')


# === groupby: universe_profile_m
county_universe = spark.read.csv('s3://ph-max-auto/v0.0.1-2020-06-08/Test/county_universe_P1.csv', header=True, encoding='GBK')
universe_profile_m = county_universe.groupby('Province', 'City', 'Hosp_level', '性质', 'City_Tier', 'Specialty_1_标准化', 'Specialty_2_标准化') \
                                    .agg(func.sum('Est_DrugIncome_RMB').alias('Est_DrugIncome_RMB'), func.sum('医生数').alias('医生数'), func.sum('床位数').alias('床位数'))

mkt_name = "Tumor"

# 多此一举 mol本来就是取自data-sample_hosp：data.join(mol, on='Molecule_Desc', how='inner')
# mol = sample_hosp_raw.select('Molecule_Desc').distinct()

# === groupby: sample_data_groupby
# pre Computed Columns —— 重命名 ，类型双浮点
sample_hosp = sample_hosp_raw.withColumnRenamed('Province', '省份') \
                            .withColumnRenamed('City', '城市') \
                            .withColumnRenamed('County', '区县') \
                            .withColumnRenamed('Hosp_code', 'PHA') \
                            .withColumnRenamed('Hospital_Name', '医院名称') \
                            .withColumnRenamed('Month', '月份') \
                            .withColumnRenamed('Volume', '数量') \
                            .withColumnRenamed('Dosage_Unit', '最小使用单位数量') \
                            .withColumnRenamed('Value', '金额')
sample_data_groupby = sample_hosp.groupby('省份', '城市', '区县',  'PHA', 'packcode', '月份') \
                        .agg(func.sum('数量').alias('数量'), func.sum('最小使用单位数量').alias('最小使用单位数量'), func.sum('金额').alias('金额'))

# === join: sample_data_groupby + universe_profile： sample_data_join_universe（空值判断：有值 and 不等于 None）
# pre Computed Columns —— 重命名
# Post Filter： PHA有值，不等于None
universe_profile = county_universe.withColumnRenamed('Panel_ID', '新版ID').withColumnRenamed('County', 'Prefecture')
sample_data_join_universe = sample_data_groupby.join(universe_profile, col("PHA") == col("新版ID"), how='left') \
                        .where(~col('PHA').isNull()) 


# === groupby：mkt_data
# pre Computed Columns —— 重命名
mkt_data = sample_data_join_universe.groupby('Province', 'City', 'Hosp_level',  '性质', 'City_Tier', 'Specialty_1_标准化', 'Specialty_2_标准化', 'packcode', '月份') \
                        .agg(func.sum('数量').alias('数量'), func.sum('最小使用单位数量').alias('最小使用单位数量'), func.sum('金额').alias('金额'))


# == pivot: mkt_data_m 
# Post Filter： PHA有值，不等于None
mkt_data_m = mkt_data.where(~col('Province').isNull()) \
                    .select('Province', 'City', 'Hosp_level', '性质', 'City_Tier', 'Specialty_1_标准化', 'Specialty_2_标准化', 'packcode', '月份', '金额', '数量') \
                    .groupby('Province', 'City', 'Hosp_level', '性质', 'City_Tier', 'Specialty_1_标准化', 'Specialty_2_标准化', 'packcode') \
                    .pivot('月份').agg(func.sum('金额').alias('金额'), func.sum('数量').alias('数量'))

# 低代码生成的结果文件字符型的会自动填充为None
mkt_data_m = mkt_data_m.fillna('NA', ['Province', 'City', 'Hosp_level', '性质', 'City_Tier', 'Specialty_1_标准化', 'Specialty_2_标准化'])
mkt_universe = universe_profile_m.fillna('NA', ['Province', 'City', 'Hosp_level', '性质', 'City_Tier', 'Specialty_1_标准化', 'Specialty_2_标准化'])
# mkt_universe 和 mkt_data_m 的字符串空值为None

#  ==  join：mkt_data_m1
mkt_data_m1 =  mkt_universe.join(mkt_data_m, on=['Province', 'City', 'Hosp_level', '性质', 'City_Tier', 'Specialty_1_标准化', 'Specialty_2_标准化'], how='inner') 

#  == join：universe_profile_m + mkt_data_m1 = region_projection_1  （类型设置为double，不要用float）
# pre Computed Columns —— 重命名
# post Computed Columns —— est_drugincome_rmb_gap：ABS(`est_drugincome_rmb_x`-`est_drugincome_rmb_y`)
region_projection_1 =  mkt_universe.withColumnRenamed("Est_DrugIncome_RMB", "Est_DrugIncome_RMB_x") \
                                    .withColumnRenamed("Province", "Province_x") \
                                    .withColumnRenamed("City", "City_x") \
                                    .withColumnRenamed("医生数", "医生数_x") \
                                    .withColumnRenamed("床位数", "床位数_x") \
                                    .join(mkt_data_m1.withColumnRenamed("Est_DrugIncome_RMB", "Est_DrugIncome_RMB_y"), on =["Hosp_level", "性质", "City_Tier","Specialty_1_标准化", "Specialty_2_标准化"], how='left') \
                                    .withColumn('Est_DrugIncome_RMB_gap', func.abs(col("Est_DrugIncome_RMB_x") - col("Est_DrugIncome_RMB_y")) )


# ======== spark 开窗函数 + where筛选 + unpivot + factor_09：region_projection_2 
'''
def execute(**kwargs):
    df = kwargs['df_region_projection_1']
    

    import os
    import pandas as pd
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func 
    import json
    import boto3
    from pyspark.sql.functions import lit, col, struct, to_json, json_tuple
    from functools import reduce
    from pyspark.sql import Window
    
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
    
    df = df.withColumn('est_drugincome_rmb_gap', col('est_drugincome_rmb_gap').cast('double')) \
            .withColumn('est_drugIncome_rmb_x', col('est_drugIncome_rmb_x').cast('double')) \
            .withColumn('est_drugIncome_rmb_y', col('est_drugIncome_rmb_y').cast('double'))
    
    
    region_projection_1 = df.withColumn('count', func.count('province_x').over(Window.partitionBy(["province_x", "city_x", "hosp_level", "性质", "city_tier","specialty_1_标准化", "specialty_2_标准化",   "est_drugIncome_rmb_gap"]).orderBy())) \
                            .withColumn('row_number', func.row_number().over(Window.partitionBy("province_x", "city_x", "hosp_level", "性质", "city_tier","specialty_1_标准化", "specialty_2_标准化").orderBy('est_drugIncome_rmb_gap', "province_y", "city_y", 'packcode_new')))
    
    region_projection_1 = region_projection_1.where(col('row_number') <= col('count')) \
                                    .withColumn('factor', col('est_drugIncome_rmb_x')/col('est_drugIncome_rmb_y') ) \
                                    .withColumn('factor', func.when(col('factor').isNull(), func.lit(1)).otherwise( col('factor') ))
    
    
    region_projection_m = unpivot(region_projection_1, list(set(region_projection_1.columns) - set([i for i in region_projection_1.columns if i.startswith("20")]))) \
                                .selectExpr('province_x as province ', 'city_x as city ', 'hosp_level', '`性质`', 'city_tier', '`specialty_1_标准化`', '`specialty_2_标准化`', 'est_drugIncome_rmb_x as Est_DrugIncome_RMB', 'est_drugIncome_rmb_y', 'factor', '`医生数_x` as `医生数`', '`床位数_x` as `床位数`', 'packcode_new', 'feature as date', 'value') \
                                .withColumn('type', func.split(col('date'), '_')[1] ) \
                                .withColumn('date', func.split(col('date'), '_')[0] )    
    region_projection_m_out = region_projection_m.where((~col('packcode_new').isNull()) & (col('packcode_new') != 'None')) \
                                        .withColumn('tmp', func.lit('tmp')) \
                                        .withColumn('factor_09', func.expr('percentile_approx(factor, 0.9)').over(Window.partitionBy('tmp'))) \
                                        .drop('tmp')
                                        
                                        
    df_out = region_projection_m_out.toDF(*[i.lower() for i in region_projection_m_out.columns])
    
    df_out.show()
    
    return {"out_df": df_out}

'''
region_projection_2_1 = region_projection_1.withColumn('cnt', func.count('Province_x').over(Window.partitionBy(["Province_x", "City_x", "Hosp_level", "性质", "City_Tier","Specialty_1_标准化", "Specialty_2_标准化", "Est_DrugIncome_RMB_gap"]).orderBy())) \
                                    .withColumn('row_number', func.row_number().over(Window.partitionBy("Province_x", "City_x", "Hosp_level", "性质", "City_Tier","Specialty_1_标准化", "Specialty_2_标准化").orderBy('Est_DrugIncome_RMB_gap', "Province", "City", 'packcode')))


region_projection_2_2 = region_projection_2_1.where(col('row_number') <= col('cnt')) \
                                    .withColumn('factor', col('Est_DrugIncome_RMB_x')/col('Est_DrugIncome_RMB_y') ) \
                                    .withColumn('factor', func.when(col('factor').isNull(), func.lit(1)).otherwise( col('factor') ))


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

region_projection_2 = unpivot(region_projection_2_2, list(set(region_projection_2_2.columns) - set([i for i in region_projection_2_2.columns if i.startswith("20")]))) \
                            .selectExpr('Province_x as Province ', 'City_x as City ', 'Hosp_level', '`性质`', 'City_Tier', '`Specialty_1_标准化`', '`Specialty_2_标准化`', 'Est_DrugIncome_RMB_x as Est_DrugIncome_RMB', 'Est_DrugIncome_RMB_y', 'factor', '`医生数_x` as `医生数`', '`床位数_x` as `床位数`', 'packcode', 'feature as date', 'value') \
                            .withColumn('type', func.split(col('date'), '_')[1] ) \
                            .withColumn('date', func.split(col('date'), '_')[0] ) \
                            .where(~col('packcode').isNull()) \
                            .withColumn('factor_09', func.expr('percentile_approx(factor, 0.9)').over(Window.partitionBy().orderBy()))


# === groupby: region_projection_m
region_projection_m = region_projection_2.withColumn('factor', func.when(col('factor') > col('factor_09'), col('factor_09') ).otherwise(col('factor') ) ) \
                                        .withColumn('value', col('factor')*col('value') ) \
                                        .groupby('Province', 'packcode', 'date', 'type').agg(func.sum('value').alias('value')) \
                                        .withColumn('Mkt', func.lit(mkt_name) )

region_projection_m.groupby('type').agg(func.sum('value')).show()

# +----+--------------------+
# |type|          sum(value)|
# +----+--------------------+
# |金额|2.5879637527622795E9|
# |数量| 7.942160173360413E7|
# +----+--------------------+