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
    g_tj_method = kwargs['g_tj_method']
    g_current_quarter = kwargs['g_current_quarter']
    g_min_quarter = kwargs['g_min_quarter']
    #g_current_quarter = '2021Q3'
    #g_min_quarter = '2018Q1'
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
        version = version.replace(" ","").split(',')
        df = spark.read.format("jdbc") \
                .option("url", "jdbc:clickhouse://192.168.16.117:8123/" + database) \
                .option("dbtable", dbtable) \
                .option("driver", "ru.yandex.clickhouse.ClickHouseDriver") \
                .option("user", "default") \
                .option("password", "") \
                .option("batchsize", 1000) \
                .option("socket_timeout", 300000) \
                .option("rewrtieBatchedStatements", True).load()
        df = df.where(df['version'].isin(version))
        return df
    
    # %% 
    # =========== 输入数据读取 =========== 
    # df_pchc_mapping = readClickhouse('default', 'F9YGH7iTKuoygfrd_pchc_mapping', '袁毓蔚_Auto_cMax_Auto_cMax_developer_2022-02-16T02:42:54+00:00')
    # df_ims_molecule_info = readClickhouse('default', 'F9YGH7iTKuoygfrd_ims_molecule_info', '袁毓蔚_Auto_cMax_Auto_cMax_developer_2022-02-16T02:42:54+00:00')
    # df_market_molecule = readClickhouse('default', 'F9YGH7iTKuoygfrd_market_molecule', '袁毓蔚_Auto_cMax_Auto_cMax_developer_2022-02-16T02:42:54+00:00')
    # df_tianjin_raw = readClickhouse('default', 'F9YGH7iTKuoygfrd_tianjin_packid_moleinfo', 'tj_CHC_18Q2_21Q3_packid')
    
    df_pchc_mapping = readInFile(kwargs['df_pchc_mapping'])
    df_ims_molecule_info = readInFile(kwargs['df_ims_molecule_info'])
    df_market_molecule = readInFile(kwargs['df_market_molecule'])
    df_tianjin_raw = readInFile(kwargs['df_tianjin_packid_moleinfo'])
    
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
        # 生成 季度 月份 mapping表
        pdf = pd.DataFrame({'qtr':['Q1']*3 + ['Q2']*3 + ['Q3']*3+ ['Q4']*3, 'mth':['01', '02', '03', '04', '05', '06', 
                                  '07', '08', '09', '10', '11', '12']})
        df = spark.createDataFrame(pdf)
        return df
    
    
    def dealPackid(df):
        # 错误packid 处理
        df = df.withColumn('packid', func.when(func.substring('packid', 1, 5)=='47775', func.concat(func.lit('58906'), func.substring('packid', 6, 2))).otherwise(col('packid'))) \
                .withColumn('packid', func.when(func.substring('packid', 1, 5)=='06470', func.concat(func.lit('64895'), func.substring('packid', 6, 2))).otherwise(col('packid')))
        return df
    
    def dealRawCommon(df, df_tj_district, df_ims_molecule_info, df_market_molecule):
        # rawdata清洗，join信息
        df1 = reName(df, dict_rename={'month':'date', 'hospital_name':'hospital', 'value':'sales', 'hosp_code':'pchc', 'packcode':'packid'})
        df1 = dealIDLength(df1, colname='packid', id_length=7)
        df1 = dealPackid(df1)
        df1 = df1.where(col('hospital_flag') == 'CHC') \
                .where( (~col('packid').isNull()) & (~col('pchc').isNull()) ) \
                .withColumn('province', func.lit('天津')) \
                .withColumn('city', func.lit('天津')) \
                .withColumn('units', func.when(col('volume').isNull(), col('sales')/col('price')).otherwise(col('volume'))) \
                .select('project', 'year', 'quarter', 'date', 'province', 'city', 'hospital', 'packid', 'units', 'sales', 'pchc').distinct()
        
        df1 = df1.join(df_tj_district, on='pchc', how='left') \
                    .join(df_ims_molecule_info, on='packid', how='left') \
                    .join(df_market_molecule, on='molecule', how='inner') \
                    .withColumn('market', col('molecule')) \
                    .select("year", "date", "quarter", "province", "city", "district", "pchc", "atc4", "nfc", "molecule", "product", "corp", "packid", "units", "sales", "market")
        return df1                       
        
    
    def methodOne(df_tianjin_raw1, df_qtr_month_map):
        # 方法1：每个季度平分成三条记录，分别对应该季度下每个月份的数据
        df_tianjin_raw2 = df_tianjin_raw1.withColumn('qtr', func.substring('quarter', 5, 2)).distinct()
        df_tianjin_raw3 = df_tianjin_raw2.join(df_qtr_month_map, on='qtr', how='left') \
                                        .withColumn('date', func.concat(col('year'), col('mth'))) \
                                        .withColumn('units', col('units')/3 ) \
                                        .withColumn('sales', col('sales')/3 ) \
                                        .where(col('units') >0.0 ).where(col('sales') >0.0  )                                  
        return df_tianjin_raw3
    
    def methodTwo(df_tianjin_raw1):
        # 方法2：根据先来后到的原则将天津数据分配给该季度下对应的月份。即如果该 季度 省份 城市 区县 市场 packcode  pchc code 下有四条纪录，那么将第一条分给该季度下第一个月份，第二条分给该季度下第二个月份，最后两条分给该季度下最后一个月份
        df_tj_ref = df_tianjin_raw1.groupby('quarter','pchc','packid').count() \
                                    .withColumn('groupx', func.row_number().over(Window().orderBy('quarter','pchc','packid')))
    
        df_raw_tj_x = df_tianjin_raw1.join(df_tj_ref, on=['quarter','pchc','packid'], how='left') \
                                        .withColumn('Q', func.substring('quarter', 6, 1))
    
        df_raw_tj_clear = df_raw_tj_x.where(col('count') == 1) \
                                    .withColumn('date', col('year')*100 +1 + (col('Q')-1)*3 ) \
                                    .drop('count', 'groupx', 'Q')
    
        df_raw_tj_y = df_raw_tj_x.where(col('count') > 1) \
                    .withColumn('rowid', func.row_number().over(Window.partitionBy('groupx').orderBy(col('sales').desc(),col('units').desc()))) \
                    .withColumn('rowid', func.when(col('rowid') > 3, func.lit(3)).otherwise(col('rowid'))) \
                    .withColumn('date', col('year')*100 + col('rowid') + (col('Q')-1)*3 ) 
    
        df_raw_tj_f =  df_raw_tj_clear.union(df_raw_tj_y.select(df_raw_tj_clear.columns))
                                    
        return df_raw_tj_f
    
    # %%
    # ==========  数据执行  ============
    # 地理信息
    df_tj_district = df_pchc_mapping.where(col('province') == '天津') \
                                    .groupby('pchc').agg(func.first('district', ignorenulls=True).alias('district'))
    
    # 季度月份
    df_qtr_month_map = qtrMonthMap()
    
    # 分子信息
    df_ims_molecule_info = reName(df_ims_molecule_info, dict_rename={'ATC4_Code':'atc4', 'NFC123_Code':'nfc', 'Molecule_Desc':'molecule', 'Prd_desc':'product', 'Corp_Desc':'corp'})
    
    # 市场包含的分子
    df_market_molecule = df_market_molecule.drop('version')
    
    # 处理
    df_tianjin_raw1 = dealRawCommon(df_tianjin_raw, df_tj_district, df_ims_molecule_info, df_market_molecule)
    if g_tj_method == "method1":
        df_tianjin_raw_out = methodOne(df_tianjin_raw1, df_qtr_month_map)
    elif g_tj_method == "method2":
        df_tianjin_raw_out = methodTwo(df_tianjin_raw1)
        
    # 时间
    df_tianjin_raw_out = df_tianjin_raw_out.where(col('quarter') >= g_min_quarter).where(col('quarter') <= g_current_quarter) \
                                        .select("year", "date", "quarter", "province", "city", "district", "pchc", "atc4", "nfc", "molecule", "product", "corp", "packid", "units", "sales", "market")
        
    # %%
    # =========== 数据输出 =============
    df_out = lowCol(df_tianjin_raw_out)
    return {"out_df":df_out}
