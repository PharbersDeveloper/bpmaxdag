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
    # df_imp_total = readClickhouse('default', 'ftZnwL38MzTJPr1s_imp_total', '袁毓蔚_Auto_cMax_enlarge_Auto_cMax_enlarge_developer_2022-02-24T08:41:53+00:00')
    # df_project_nation = readClickhouse('default', 'ftZnwL38MzTJPr1s_project_nation', '袁毓蔚_Auto_cMax_enlarge_Auto_cMax_enlarge_developer_2022-02-24T08:41:53+00:00')
    
    df_imp_total = readInFile(kwargs["df_imp_full_hz"])
    df_project_nation = readInFile(kwargs["df_project_nation_hz"])
    # %%
    # =========== 函数定义 =============
    def qtrMonthMap():
        pdf = pd.DataFrame({'qtr':['Q1']*3 + ['Q2']*3 + ['Q3']*3+ ['Q4']*3, 'mth':['01', '02', '03', '04', '05', '06', 
                                  '07', '08', '09', '10', '11', '12']})
        df = spark.createDataFrame(pdf)
        return df
    
    def getProjectPrice(df_project_nation, df_price_origin, df_price_city, df_price_province, df_price_year, df_price_pack, df_price_pack_year):
        df_qtr_month_map = qtrMonthMap()
    
        df_project_price1 = df_project_nation.withColumn('year', func.substring(col('date'), 1, 4) ) \
                                            .withColumn('quarter', func.substring(col('date'), 5, 2) )
        df_project_price1 = df_project_price1.join(df_qtr_month_map, df_project_price1['quarter']==df_qtr_month_map['mth'], how='left') \
                                                .withColumn('quarter', func.concat(col('year'), col('qtr') )).drop('mth', 'qtr')
    
    
        df_project_price2 = df_project_price1.join(df_price_origin, on=['packid', 'quarter', 'province', 'city'], how='left') \
                                               .join(df_price_city, on=['year', 'packid', 'province', 'city'], how='left') \
                                                .join(df_price_province, on=['quarter', 'packid', 'province'], how='left') \
                                                .join(df_price_year, on=['year', 'packid', 'province'], how='left') \
                                                .join(df_price_pack, on=['quarter', 'packid'], how='left') \
                                                .join(df_price_pack_year, on=['year', 'packid'], how='left')
    
        df_project_price3 = df_project_price2.withColumn('price', func.when(col('price').isNull(), col('price_city')).otherwise(col('price'))) \
                                            .withColumn('price', func.when(col('price').isNull(), col('price_prov')).otherwise(col('price'))) \
                                            .withColumn('price', func.when(col('price').isNull(), col('price_year')).otherwise(col('price'))) \
                                            .withColumn('price', func.when(col('price').isNull(), col('price_pack')).otherwise(col('price'))) \
                                            .withColumn('price', func.when(col('price').isNull(), col('price_pack_year')).otherwise(col('price'))) \
                                            .withColumn('units', col('sales')/col('price') ) \
                                            .where(col('units') > 0.0 ).where(col('sales') > 0.0 ).where(col('price') > 0.0 ) \
                                            .select('year', 'date', 'quarter', 'province', 'city', 'district', 'market', 'packid', 'price', 'units', 'sales', 'flag_sample')    
        return df_project_price3
    

    # %%
    # =========== 数据执行 =============
    ##---- Price ----
    ## origin price
    df_price_origin = df_imp_total.groupby('packid', 'quarter', 'province', 'city') \
                                    .agg(func.sum('sales').alias('sales'), func.sum('units').alias('units')) \
                                    .withColumn('price', col('sales')/col('units') ) \
                                    .select('packid', 'quarter', 'province', 'city', 'price')
    
    ## mean price by city year
    df_price_city = df_imp_total.groupby('packid', 'year', 'province', 'city') \
                                    .agg(func.sum('sales').alias('sales'), func.sum('units').alias('units')) \
                                    .withColumn('price_city', col('sales')/col('units') ) \
                                    .select('packid', 'year', 'province', 'city', 'price_city')
    
    ## mean price by province quarter
    df_price_province = df_imp_total.groupby('packid', 'quarter', 'province') \
                                    .agg(func.sum('sales').alias('sales'), func.sum('units').alias('units')) \
                                    .withColumn('price_prov', col('sales')/col('units') ) \
                                    .select('packid', 'quarter', 'province', 'price_prov')
    
    ## mean price by province year
    df_price_year = df_imp_total.groupby('packid', 'year', 'province') \
                                    .agg(func.sum('sales').alias('sales'), func.sum('units').alias('units')) \
                                    .withColumn('price_year', col('sales')/col('units') ) \
                                    .select('packid', 'year', 'province', 'price_year')
    
    ## mean price by pack quarter
    df_price_pack = df_imp_total.groupby('packid', 'quarter') \
                                    .agg(func.sum('sales').alias('sales'), func.sum('units').alias('units')) \
                                    .withColumn('price_pack', col('sales')/col('units') ) \
                                    .select('packid', 'quarter', 'price_pack')
    
    ## mean price by pack year
    df_price_pack_year = df_imp_total.groupby('packid', 'year') \
                                    .agg(func.sum('sales').alias('sales'), func.sum('units').alias('units')) \
                                    .withColumn('price_pack_year', col('sales')/col('units') ) \
                                    .select('packid', 'year', 'price_pack_year')
    
    ##---- Update ----
    df_project_price = getProjectPrice(df_project_nation, df_price_origin, df_price_city, df_price_province, df_price_year, df_price_pack, df_price_pack_year)
    
    # %%
    # =========== 数据输出 =============
    # 读回
    df_out = lowCol(df_project_price)
    return {"out_df":df_out}
