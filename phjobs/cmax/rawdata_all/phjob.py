# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL


def execute(**kwargs):
    logger = phs3logger(kwargs["run_id"], LOG_DEBUG_LEVEL)
    spark = kwargs['spark']
    
    ### input args ###
    # g_current_quarter = '2021Q3'
    # g_min_quarter = '2018Q1'
    
    g_current_quarter = kwargs['g_current_quarter']
    g_min_quarter = kwargs['g_min_quarter']
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
    from pyspark.sql import Window
    
    # %%
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
        df = df.drop('traceId')
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
    #df_raw_data = readClickhouse('default', 'F9YGH7iTKuoygfrd_raw_data', 'all')
    #df_rawdata_tianjin = readClickhouse('default', 'F9YGH7iTKuoygfrd_rawdata_tianjin', '袁毓蔚_Auto_cMax_Auto_cMax_developer_2022-02-18T06:54:41+00:00')
    #df_rawdata_shanghai = readClickhouse('default', 'F9YGH7iTKuoygfrd_rawdata_shanghai', '袁毓蔚_Auto_cMax_Auto_cMax_developer_2022-02-18T06:54:41+00:00')
    #df_ims_molecule_info = readClickhouse('default', 'F9YGH7iTKuoygfrd_ims_molecule_info', '袁毓蔚_Auto_cMax_Auto_cMax_developer_2022-02-18T06:54:41+00:00')
    #df_market_molecule = readClickhouse('default', 'F9YGH7iTKuoygfrd_market_molecule', '袁毓蔚_Auto_cMax_Auto_cMax_developer_2022-02-18T06:54:41+00:00')
    
    df_raw_data = readInFile("df_raw_data")
    df_rawdata_tianjin = readInFile("df_rawdata_tianjin")
    df_rawdata_shanghai = readInFile("df_rawdata_shanghai")
    df_ims_molecule_info = readInFile("df_ims_molecule_info")
    df_market_molecule = readInFile("df_market_molecule")
    

    # %%
    # ==========  函数定义  ============
    def reName(df, dict_rename={}):
        df = reduce(lambda df, i_dict:df.withColumnRenamed(i_dict[0], i_dict[1]), zip(dict_rename.keys(), dict_rename.values()), df)
        return df
    
    def dealIDLength(df, colname='ID', id_length=6):
        # ID不足7位的前面补0到6位
        # 国药诚信医院编码长度是7位数字，cpa医院编码是6位数字
        # 其他来源的ID 还有包含字母的, 所以要为字符型，不能转为 数值型
        df = df.withColumn(colname, col(colname).cast(StringType()))
        # 去掉末尾的.0
        df = df.withColumn(colname, func.regexp_replace(colname, "\\.0", ""))
        df = df.withColumn(colname, func.when(func.length(col(colname)) < 7, func.lpad(col(colname), id_length, "0")).otherwise(col(colname)))
        return df
    
    def qtrMonthMap():
        pdf = pd.DataFrame({'qtr':['Q1']*3 + ['Q2']*3 + ['Q3']*3+ ['Q4']*3, 'mth':['01', '02', '03', '04', '05', '06', 
                                  '07', '08', '09', '10', '11', '12']})
        df = spark.createDataFrame(pdf)
        return df
    
    
    def dealPackid(df):
        df = df.withColumn('packid', func.when(func.substring('packid', 1, 5)=='47775', func.concat(func.lit('58906'), func.substring('packid', 6, 2))).otherwise(col('packid'))) \
                .withColumn('packid', func.when(func.substring('packid', 1, 5)=='06470', func.concat(func.lit('64895'), func.substring('packid', 6, 2))).otherwise(col('packid')))
        return df
    
    def dealRawData(df_raw_data, df_ims_molecule_info, df_market_molecule):
        raw_gz_Servier_guangzhou = df_raw_data.where(col('version') == 'raw_gz_Servier_guangzhou')
        raw_gz_Servier_guangzhou = raw_gz_Servier_guangzhou.withColumn('project', func.lit('Servier'))
    
        df_raw_data_others = df_raw_data.where(col('version') != 'raw_gz_Servier_guangzhou')
        df_raw_data_all = raw_gz_Servier_guangzhou.union(df_raw_data_others) \
                                            .where(col('project') == 'Servier').where(col('hospital_flag') == 'CHC')
    
        df_raw_data2 = reName(df_raw_data_all, dict_rename={'month':'date', 'hospital_name':'hospital', 'value':'sales', 'hospital_category':'pchc', 'packcode':'packid', 'county':'district'})
        df_raw_data2 = dealIDLength(df_raw_data2, colname='packid', id_length=7)
    
        df_raw_data2 = df_raw_data2.withColumn('province', func.regexp_replace('province', '省|市', '')) \
                                .withColumn('city', func.when(col('city')=='市辖区', func.lit('北京')).otherwise( func.regexp_replace('city', '市', '') )) \
                                .withColumn('units', func.when(col('volume').isNull(), col('sales')/col('price')).otherwise(col('volume'))) \
                                 .select('project', 'year', 'quarter', 'date', 'province', 'city', 'district', 'hospital',
                                         'packid', 'units', 'sales', 'pchc', 'hosp_code').distinct()
    
        df_raw_data2  = df_raw_data2.withColumn('pchc', func.when(col('pchc')=='基层医疗机构', col('hosp_code')).otherwise(col('pchc')) ) \
                                    .where(col('province').isin('安徽','北京','福建','江苏','天津','浙江','广东','山东')) \
                                    .where(~col('pchc').isNull()).where(~col('packid').isNull())
    
        df_raw_data2 = dealPackid(df_raw_data2)
    
        df_raw_data2 = df_raw_data2.join(df_ims_molecule_info, on='packid', how='left') \
                                    .join(df_market_molecule, on='molecule', how='inner') \
                                    .withColumn('market', col('molecule')) \
                                    .withColumn('date', func.regexp_replace('date', '\\/', '')) \
                                    .select("year", "date", "quarter", "province", "city", "district", "pchc", "packid", "market", "units", "sales")
        return df_raw_data2
    
    def getAllData(df_raw_data, df_rawdata_tianjin, df_rawdata_shanghai):
        df_all = df_raw_data.union(df_rawdata_tianjin.select(df_raw_data.columns))
        df_pchc_map_city = df_all.select('province', 'city', 'district', 'pchc').distinct() \
                                .withColumn('row_number', func.row_number().over(Window.partitionBy("pchc").orderBy('province', 'city', 'district', 'pchc'))) \
                                .where(col('row_number') == 1)
        df_all_final = df_all.drop('province', 'city', 'district') \
                                .join(df_pchc_map_city, on='pchc', how='left') \
                                .groupby("year", "date", "quarter", "province", "city", "district", "pchc", "market", "packid") \
                                .agg(func.sum('units').alias('units'), func.sum('sales').alias('sales')) \
                            .union(df_rawdata_shanghai.select(df_raw_data.columns))
        return df_all_final
    
    # %%
    # =========== 数据执行 =============
    # 分子信息
    df_ims_molecule_info = reName(df_ims_molecule_info, dict_rename={'ATC4_Code':'atc4', 'NFC123_Code':'nfc', 'Molecule_Desc':'molecule', 'Prd_desc':'product', 'Corp_Desc':'corp'})
    
    df_raw_data_clean = dealRawData(df_raw_data, df_ims_molecule_info, df_market_molecule)
    df_raw_data_all = getAllData(df_raw_data_clean, df_rawdata_tianjin, df_rawdata_shanghai)
    
    # 样本里有错误，需删除这些列
    df_raw_data_all = df_raw_data_all.where( ~ (( col('province')=='江苏' ) & ( col('city')=='北京' )) ) \
                                    .where( (col('quarter') >= g_min_quarter) & (col('quarter') <= g_current_quarter) )  \
                                    .withColumn('flag', func.lit(0))
    # %%
    # =========== 数据输出 =============
    # 读回
    df_out = lowCol(df_raw_data_all)
    return {"out_df":df_out}
