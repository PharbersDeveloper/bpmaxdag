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
    # df_imp_total = readClickhouse('default', 'F9YGH7iTKuoygfrd_rawdata_all', '袁毓蔚_Auto_cMax_Auto_cMax_developer_2022-02-18T07:50:08+00:00')
    # df_pchc_universe = readClickhouse('default', 'F9YGH7iTKuoygfrd_pchc_universe', '2021_PCHC_Universe更新维护')
    
    df_imp_total = readInFile(kwargs["df_imp_total"])
    df_pchc_universe = readInFile(kwargs["df_pchc_universe"])
    # %%
    # =========== 函数定义 =============
    def reName(df, dict_rename={}):
        df = reduce(lambda df, i_dict:df.withColumnRenamed(i_dict[0], i_dict[1]), zip(dict_rename.keys(), dict_rename.values()), df)
        return df
    
    def unionDf(df1, df2, utype='same'):
        if utype=='same':
            all_cols =  list(set(df1.columns).intersection(set(df2.columns)) - set(['version']))
        elif utype=='all':
            all_cols =  list(set(set(df1.columns + df2.columns) - set(['version'])))
            
        for i in all_cols:
            if i not in df1.columns:
                df1 = df1.withColumn(i, func.lit(None))
            if i not in df2.columns:
                df2 = df2.withColumn(i, func.lit(None))
            
        df_all = df1.select(all_cols).union(df2.select(all_cols)) 
        return df_all
    
    def getFirst(df, first_col=['province', 'city', 'district'], on_col=['pchc']):
        logger.debug(first_col)
        df_pchc_map_city = df.select(first_col + on_col).distinct() \
                            .withColumn('row_number', func.row_number().over(Window.partitionBy(*on_col) \
                                                    .orderBy(*[col(i).asc_nulls_last() for i in first_col]))) \
                            .where(col('row_number') == 1) 

        df_first_out = df.drop(*first_col) \
                                .join(df_pchc_map_city, on='pchc', how='left')
        return df_first_out

    def getPchcMappingM(df_pchc_universe):
        # pchc_universe 处理
        df_pchc_mapping = reName(df_pchc_universe, 
                                 dict_rename={'省':'province', '地级市':'city', '区[县_县级市]':'district', '新版PCHC_Code':'pchc', '其中：西药药品收入_千元_':'est'})
    
        df_pchc_mapping_m = df_pchc_mapping.where( col('est') > 0.0 ) \
                                        .withColumn('province', func.regexp_replace("province", "省|市", "")) \
                                        .withColumn('city', func.regexp_replace("city", "市", "")) \
                                        .select('pchc', 'province', 'city', 'district', 'est').distinct()
        df_pchc_mapping_m = getFirst(df_pchc_mapping_m)
        df_pchc_mapping_m = df_pchc_mapping_m.groupby('pchc', 'province', 'city', 'district').agg(func.sum('est').alias('est'))
        return df_pchc_mapping_m
    
    def getFlagSampleMap(df):
        df_flag_sample_map = df.select('pchc').distinct() \
                                        .withColumn('flag_sample', func.lit(1))
        return df_flag_sample_map
    
    def getHospitalUniverse(df_pchc_mapping_m, df_imp_total, df_flag_sample_map):
        # imp_total 处理：只保留 df_pchc_mapping_m 中 有的pchc
        df_imp_total_2 = df_imp_total.join(df_pchc_mapping_m.select('pchc').distinct(), on='pchc', how='inner') 
        # pchc_universe 和 imp_total 合并 
        df_hospital_universe = unionDf(df_pchc_mapping_m, df_imp_total_2, utype='all')
        # 地理信息处理，以pchc分组取第一行
        df_hospital_universe = getFirst(df_hospital_universe, first_col=['province', 'city', 'district', 'est'], on_col=['pchc']) \
                                        .select('province', 'city', 'district', 'est', 'pchc').distinct()
        df_hospital_universe = df_hospital_universe.where( ~col('province').isNull() ).where( ~col('city').isNull() ).where( ~col('district').isNull() ) \
                                                        .where( ~col('est').isNull() ) \
                                                    .join(df_flag_sample_map, on='pchc', how='left') \
                                                    .fillna(0, 'flag_sample')
        return df_hospital_universe
       
    # %%
    # =========== 数据执行 =============
    df_pchc_mapping_m = getPchcMappingM(df_pchc_universe)
    df_flag_sample_map = getFlagSampleMap(df_imp_total)
    df_hospital_universe = getHospitalUniverse(df_pchc_mapping_m, df_imp_total, df_flag_sample_map)
    
    # %%
    # =========== 数据输出 =============
    df_out = lowCol(df_hospital_universe)
    return {"out_df":df_out}