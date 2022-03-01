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
    from pyspark.sql import Window
    # %%
    # =========== 输入数据读取 ===========
    def dealToNull(df):
        df = df.replace(["None", ""], None)
        return df
    
    def dealScheme(df, dict_scheme):
        # 数据类型处理
        if dict_scheme == {}:
            for i in dict_scheme.keys():
                df = df.withColumn(i, col(i).cast(dict_scheme[i]))
        return df
    
    def lowCol(df):
        df = df.toDF(*[c.lower() for c in df.columns])
        return df
    
    def readInFile(df, dict_scheme={}):
        df = dealToNull(df)
        df = lowCol(df)
        df = dealScheme(df, dict_scheme)
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
    # df_imp_total = readClickhouse('default', 'ftZnwL38MzTJPr1s_imp_total', '袁毓蔚_Auto_cMax_enlarge_Auto_cMax_enlarge_developer_2022-02-23T03:37:29+00:00')
    # df_hospital_universe = readClickhouse('default', 'ftZnwL38MzTJPr1s_hospital_universe', '袁毓蔚_Auto_cMax_enlarge_Auto_cMax_enlarge_developer_2022-02-23T03:37:29+00:00')
    
    df_imp_total = readInFile( kwargs["df_imp_hz"])
    df_hospital_universe = readInFile( kwargs["df_hospital_universe_hangzhou"])
    df_out_sample = readInFile( kwargs["df_out_sample_hz"])
    df_imp_others = readInFile( kwargs["df_imp_others_hz"])
    
    
    # %%
    # =========== 函数定义 =============
    def reName(df, dict_rename={}):
        df = reduce(lambda df, i_dict:df.withColumnRenamed(i_dict[0], i_dict[1]), zip(dict_rename.keys(), dict_rename.values()), df)
        return df
    ##---- Universe info ----
    ## projection data
    def getProjData(df_imp_total):
        df_proj_data =  df_imp_total.withColumn('sales', func.sum('sales') \
                                                .over(Window.partitionBy('province', 'city', 'district', 'pchc', 'market', 'packid', 'date') \
                                                .orderBy('province', 'city', 'district', 'pchc', 'market', 'packid', 'date'))) \
                                    .withColumn('units', func.sum('units') \
                                                .over(Window.partitionBy('province', 'city', 'district', 'pchc', 'market', 'packid', 'date') \
                                                .orderBy('province', 'city', 'district', 'pchc', 'market', 'packid', 'date')))
        return df_proj_data
    def getOutData(df_proj_data, df_hospital_universe):
        df_out_data = df_proj_data.join(df_hospital_universe.select('pchc').distinct(), on='pchc', how='left_anti') \
                                    .withColumn('flag_sample', func.lit(1))
        return df_out_data
    ##---- Sample province projection ----
    def getDistrictMapping(df_hospital_universe):
        ## district mapping
        df_district_mapping = df_hospital_universe.withColumn('est', func.first('est') \
                                                        .over(Window.partitionBy('province', 'city', 'district', 'pchc', 'flag_sample') \
                                                        .orderBy( *[col(i).asc_nulls_last() for i in ['est'] ] ))) \
                                                .groupby('province', 'city', 'district') \
                                                        .agg(func.sum('flag_sample').alias('sample'), func.expr('percentile_approx(est, 0.5)').alias('est')) \
                                                .orderBy(col('province'), col('city'), col('est').desc())    
        return df_district_mapping
    def getSamplePack(df_proj_data):
        ## sample pack
        df_sample_pack = df_proj_data.where(~col('packid').isNull()) \
                                .select('province', 'city', 'district', 'date', 'market', 'packid').distinct()
        return df_sample_pack
    def getDistrictSample(df_district_mapping):
        ## district sample
        df_district_sample = df_district_mapping.where(col('sample') >0).select('province', 'city', 'district', 'est')
        df_district_sample = reName(df_district_sample, dict_rename={'city':'sample_city', 'district':'sample_dist', 'est':'sample_est'})
        return df_district_sample
    def getProjSample(df_district_mapping, df_district_sample, df_hospital_universe, df_proj_data, df_sample_pack):
        # 生成 est_flag
        df_proj_sample1 = df_district_mapping.join(df_district_sample, on='province', how='left') \
                                            .withColumn('est_diff', func.abs(col('est') - col('sample_est'))) \
                                            .withColumn('est_diff_min', func.min('est_diff').over(Window.partitionBy('province', 'city', 'district').orderBy())) \
                                            .withColumn('est_flag', func.when(col('est_diff') == col('est_diff_min'), func.lit(2)).otherwise(func.lit(0))  ) \
                                            .withColumn('est_flag', func.when( (col('city') == col('sample_city')) & (col('district') == col('sample_dist')), func.lit(1)).otherwise(col('est_flag')) ) \
                                            .where(col('est_flag') > 0) \
                                            .withColumn('est_flag_min', func.min('est_flag').over(Window.partitionBy('province', 'city', 'district').orderBy()))  \
                                            .where(col('est_flag') == col('est_flag_min')) \
                                            .withColumn('sample_dist_first', func.first('sample_dist').over(Window.partitionBy('province', 'city', 'district').orderBy('sample_dist')))  \
                                            .where(col('sample_dist') == col('sample_dist_first'))
        df_proj_sample2 = df_proj_sample1.select('province', 'city', 'district', 'sample_city', 'sample_dist') \
                                        .join(df_hospital_universe, on=['province', 'city', 'district'], how='left') \
                                        .join(reName(df_sample_pack, dict_rename={'city':'sample_city', 'district':'sample_dist'}), on=['province', 'sample_city', 'sample_dist'], how='left') \
                                        .join(df_proj_data, on=['province', 'city', 'district', 'pchc', 'market', 'packid', 'date'], how='left')
        ## est ratio
        df_proj_sample3 = df_proj_sample2.withColumn('sales', func.when( (col('flag_sample') == 1) & (col('sales').isNull()), func.lit(0)).otherwise(col('sales')) ) \
                                        .withColumn('units', func.when( (col('flag_sample') == 1) & (col('units').isNull()), func.lit(0)).otherwise(col('units')) ) \
                                        .withColumn('sales_mean', func.mean('sales').over(Window.partitionBy('sample_dist', 'flag_sample', 'market', 'packid', 'date').orderBy())) \
                                        .withColumn('units_mean', func.mean('units').over(Window.partitionBy('sample_dist', 'flag_sample', 'market', 'packid', 'date').orderBy())) \
                                        .withColumn('est_mean', func.mean('est').over(Window.partitionBy('sample_dist', 'flag_sample', 'market', 'packid', 'date').orderBy())) \
                                        .withColumn('sales_est_ratio', col('sales_mean')/col('est_mean') ) \
                                        .withColumn('units_est_ratio', col('units_mean')/col('est_mean') )
        df_proj_sample4 =  df_proj_sample3.withColumn('count', func.count('sample_dist').over(Window.partitionBy('sample_dist', 'market', 'packid', 'date').orderBy())) \
                                    .withColumn('flag_sample_sum', func.sum('flag_sample').over(Window.partitionBy('sample_dist', 'market', 'packid', 'date').orderBy())) \
                                    .withColumn('sales_est_ratio_m', func.first('sales_est_ratio').over(Window.partitionBy('sample_dist', 'market', 'packid', 'date').orderBy())) \
                                    .withColumn('units_est_ratio_m', func.first('units_est_ratio').over(Window.partitionBy('sample_dist', 'market', 'packid', 'date').orderBy())) \
                                    .withColumn('sample_ratio', col('flag_sample_sum')/col('count') ) \
                                    .withColumn('sales', func.when( (col('flag_sample')==1) & (col('sample_ratio') >= 0.8), col('sales'))
                                                            .otherwise( col('est')*col('sales_est_ratio_m') ) )  \
                                    .withColumn('units', func.when( (col('flag_sample')==1) & (col('sample_ratio') >= 0.8), col('units'))
                                                            .otherwise( col('est')*col('units_est_ratio_m') ) )      
        return df_proj_sample4
    def getProjSampleFinal(df_proj_sample, df_out_data, df_imp_others='NA', df_out_sample = 'NA'):
        all_cols =  list(set(df_out_data.columns).intersection(set(df_proj_sample.columns)) - set(['version']))
        df_proj_sample_all = df_proj_sample.select(all_cols).union(df_out_data.select(all_cols))
        if df_out_sample != 'NA':
            df_proj_sample_all = df_proj_sample_all.join(df_out_sample, on='pchc', how='left_anti')
        df_proj_sample_all_final = df_proj_sample_all.groupby('date', 'province', 'city', 'district', 'market', 'packid', 'flag_sample') \
                                                .agg(func.sum('sales').alias('sales'), func.sum('units').alias('units')) \
                                                .where(col('sales') > 0.0).where(col('units') > 0.0)
        if df_out_sample != 'NA':
            all_cols =  list(set(df_proj_sample_all_final.columns).intersection(set(df_imp_others.columns)) - set(['version']))
            df_proj_sample_all_final = df_proj_sample_all_final.select(all_cols).union(df_imp_others.select(all_cols))
        return df_proj_sample_all_final


    
    # =========== 数据执行 =============
    def projSamplePip(df_imp_total, df_hospital_universe, df_imp_others, df_out_sample):
        ##---- Sample province projection ----
        df_proj_data = getProjData(df_imp_total)
        df_out_data = getOutData(df_proj_data, df_hospital_universe)
        df_district_mapping = getDistrictMapping(df_hospital_universe)
        df_sample_pack = getSamplePack(df_proj_data)
        df_district_sample = getDistrictSample(df_district_mapping)
        df_proj_sample = getProjSample(df_district_mapping, df_district_sample, df_hospital_universe, df_proj_data, df_sample_pack)
        df_proj_sample_final = getProjSampleFinal(df_proj_sample, df_out_data, df_imp_others, df_out_sample)

        return df_proj_sample_final
    
    df_proj_sample_final = projSamplePip(df_imp_total, df_hospital_universe, df_imp_others, df_out_sample)
    # %%
    # =========== 数据输出 =============
    # 读回
    df_out = lowCol(df_proj_sample_final)
    return {"out_df":df_out}
