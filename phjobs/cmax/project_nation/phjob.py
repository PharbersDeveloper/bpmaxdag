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
    g_input_version = kwargs['g_input_version']
    ### input args ###
    
    ### output args ###
    ### output args ###

    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    import os
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col
    import time
    import re
    import pandas as pd
    import numpy as np
    import json
    from functools import reduce
    from pyspark.sql import Window    # %%
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
    
    
    def readClickhouse(database, dbtable, version):
        df = spark.read.format("jdbc") \
                .option("url", "jdbc:clickhouse://192.168.16.117:8123/" + database) \
                .option("dbtable", dbtable) \
                .option("driver", "ru.yandex.clickhouse.ClickHouseDriver") \
                .option("user", "default") \
                .option("password", "") \
                .option("batchsize", 1000) \
                .option("socket_timeout", 300000) \
                .option("rewrtieBatchedStatements", True).load()
        if version != 'all':
            version = version.replace(" ","").split(',')
            df = df.where(df['version'].isin(version))
        return df
    # %% 
    # =========== 输入数据读取 =========== 
    # df_pchc_city_tier = readClickhouse('default', 'ftZnwL38MzTJPr1s_pchc_city_tier', 'pchc_city_tier')
    # df_project_sample = readClickhouse('default', 'ftZnwL38MzTJPr1s_project_sample_fromR', 'Project_Sample_20220223_R')
    # df_hospital_universe = readClickhouse('default', 'ftZnwL38MzTJPr1s_hospital_universe', '袁毓蔚_Auto_cMax_enlarge_Auto_cMax_enlarge_developer_2022-02-23T03:37:29+00:00')
    # 64700
    
    df_pchc_city_tier = readInFile("df_pchc_city_tier")
    df_project_sample = readInFile("df_project_sample")
    df_hospital_universe = readInFile("df_hospital_universe")
    # %% 
    # =========== 函数定义 =============
    def reName(df, dict_rename={}):
        df = reduce(lambda df, i_dict:df.withColumnRenamed(i_dict[0], i_dict[1]), zip(dict_rename.keys(), dict_rename.values()), df)
        return df
    
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
    
    def getCityTierMap(df_pchc_city_tier):
        df_city_tier = df_pchc_city_tier.withColumn('tier', func.first('city_tier').over(Window.partitionBy('city').orderBy(col('city_tier').asc_nulls_last() ))) \
                                        .withColumn('tier', func.when(~col('city_tier').isNull(), col('city_tier')).otherwise(col('tier'))) \
                                        .withColumn('tier', func.when(col('tier').isNull(), 5).otherwise(col('tier'))) \
                                        .select('city', 'tier').distinct()
        return df_city_tier
    
    def getUniverseCity(df_hospital_universe, df_city_tier):
        df_universe_city = df_hospital_universe.join(df_city_tier, on='city', how='left') \
                                            .withColumn('tier', func.when(col('tier').isNull(), 1).otherwise(col('tier'))) \
                                            .where(~col('est').isNull()) \
                                            .groupby('province', 'city', 'district', 'tier') \
                                            .agg(func.sum('est').alias('est'))
        return df_universe_city
    
    def getNationSample(df_project_sample, df_universe_city, df_city_tier):
        df_nation_sample = df_project_sample.where(~col('province').isin(['北京', '上海'])) \
                                        .join(df_city_tier, on='city', how='left') \
                                        .withColumn('tier', func.when(col('tier').isNull(), 1).otherwise(col('tier'))) \
                                        .groupby('date', 'province', 'city', 'tier', 'district', 'market', 'packid') \
                                            .agg(func.sum('sales').alias('sales'), func.sum('units').alias('units')) \
                                        .join(df_universe_city, on=['province', 'city', 'district', 'tier'], how='inner')
    
        df_nation_sample_pivot = df_nation_sample.groupBy('province', 'city', 'tier', 'district', 'market', 'packid', 'est') \
                                                .pivot("date").agg(func.sum('sales').alias('sales'), func.sum('units').alias('units'))
        df_nation_sample_pivot = reName(df_nation_sample_pivot, 
                                        dict_rename={'province':'province_sample', 'city':'city_sample', 'district':'district_sample', 'est':'est_sample'})
        return df_nation_sample_pivot
    
    def getProjectNation(df_universe_city, df_nation_sample, df_project_sample):
        ## loop
        df_project_nation1 = df_universe_city.join(df_nation_sample, on='tier', how='left') \
                                        .withColumn('est_gap', func.abs( col('est') - col('est_sample') )) \
                                        .withColumn('est_gap_min', func.min('est_gap').over(Window.partitionBy('province', 'city', 'district', 'tier', 'est').orderBy())) \
                                        .where(col('est_gap') <= col('est_gap_min')) \
                                        .withColumn('slope', col('est')/col('est_sample') ) \
                                        .withColumn('slope', func.when(col('slope').isNull(), 1).otherwise(col('slope')) )
    
        df_project_nation2 = unpivot(df_project_nation1, ['tier', 'province', 'city', 'district', 'est', 'province_sample', 'city_sample', 'district_sample', 'market', 'packid', 'est_sample', 'est_gap', 'est_gap_min', 'slope']) \
                            .withColumn('index', func.substring_index(col('feature'), '_', -1) )\
                            .withColumn('date', func.substring_index(col('feature'), '_', 1) )\
                            .groupBy('tier', 'province', 'city', 'district', 'est', 'province_sample', 'city_sample', 'district_sample', 'market', 'packid', 'est_sample', 'est_gap', 'est_gap_min', 'slope', 'date') \
                            .pivot("index").agg(func.sum('value').alias('')) \
                            .select('date', 'province', 'city', 'tier', 'district', 'est', 'est_sample', 'market', 'packid', 'sales', 'units','slope')
        ## result
        double_slope85 = df_project_nation2.select(col('slope').cast('double')).approxQuantile("slope", [0.85], 0)[0]
        df_project_nation3 = df_project_nation2.withColumn('slope', func.when(col('slope')>double_slope85, func.lit(double_slope85)).otherwise(col('slope')) ) \
                                                .withColumn('sales_m', col('sales')*col('slope')) \
                                                .withColumn('units_m', col('units')*col('slope')) \
                                                .withColumn('flag_sample', func.lit(2)) \
                                                .groupby('date', 'province', 'city', 'district', 'market', 'packid', 'flag_sample') \
                                                .agg(func.sum('sales_m').alias('sales'), func.sum('units_m').alias('units')) \
                                                .where(col('sales') > 0.0).where(col('units') > 0.0) \
                                                .join(df_project_sample, on='city', how='left_anti')
        return df_project_nation3
    
    def unionAll(df_project_nation, df_project_sample):
        all_cols =  list(set(df_project_nation.columns).intersection(set(df_project_sample.columns)) - set(['version']))
        df_project_nation_all = df_project_nation.select(all_cols).union(df_project_sample.select(all_cols)) 
        return df_project_nation_all
    
    # %%
    # =========== 数据执行 =============
    def ProjectNationPip(df_pchc_city_tier, df_hospital_universe, df_project_sample):
        df_city_tier = getCityTierMap(df_pchc_city_tier)

        ##---- Nation info ----
        ## universe city
        df_universe_city = getUniverseCity(df_hospital_universe, df_city_tier)

        ## nation sample
        df_nation_sample = getNationSample(df_project_sample, df_universe_city, df_city_tier)

        ##---- Projection ----
        df_project_nation = getProjectNation(df_universe_city, df_nation_sample, df_project_sample)
        df_project_nation_all = unionAll(df_project_nation, df_project_sample)
        return df_project_nation_all
    
    df_project_nation_all = ProjectNationPip(df_pchc_city_tier, df_hospital_universe, df_project_sample)
    
    # %%
    # =========== 数据输出 =============
    # 读回
    df_out = lowCol(df_project_nation_all)
    return {"out_df":df_out}
