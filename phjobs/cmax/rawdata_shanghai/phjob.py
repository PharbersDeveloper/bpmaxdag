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
    g_sh_method = kwargs['g_sh_method']
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
    # df_ims_molecule_info = readClickhouse('default', 'F9YGH7iTKuoygfrd_ims_molecule_info', '袁毓蔚_Auto_cMax_Auto_cMax_developer_2022-02-16T02:42:54+00:00')
    # df_market_molecule = readClickhouse('default', 'F9YGH7iTKuoygfrd_market_molecule', '袁毓蔚_Auto_cMax_Auto_cMax_developer_2022-02-16T02:42:54+00:00')
    # df_shanghai_raw = readClickhouse('default', 'F9YGH7iTKuoygfrd_shanghai_packid_moleinfo', 'shanghai_201805_202107_all, shanghai_202108_202110')
    
    df_ims_molecule_info = readInFile(kwargs['df_ims_molecule_info'])
    df_market_molecule = readInFile(kwargs['df_market_molecule'])
    df_shanghai_raw = readInFile(kwargs['df_shanghai_packid_moleinfo'])
    # %% 
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
    
    def dealPackid(df):
        df = df.withColumn('packid', func.when(func.substring('packid', 1, 5)=='47775', func.concat(func.lit('58906'), func.substring('packid', 6, 2))).otherwise(col('packid'))) \
                .withColumn('packid', func.when(func.substring('packid', 1, 5)=='06470', func.concat(func.lit('64895'), func.substring('packid', 6, 2))).otherwise(col('packid')))
        return df
    
    def methodFull(df_shanghai_raw,df_ims_molecule_info, df_market_molecule):
        df_raw = reName(df_shanghai_raw, dict_rename={'month':'date', 'hospital_name':'hospital', 'value':'sales', 'hosp_code':'pchc', 'packcode':'packid', 'county':'district'})
        df_raw = dealIDLength(df_raw, colname='packid', id_length=7)
    
        df_raw2 = df_raw.where(col('hospital_flag') == 'CHC') \
                        .where( (~col('packid').isNull()) & (~col('pchc').isNull()) ) \
                        .withColumn('province', func.lit('上海')) \
                        .withColumn('city', func.lit('上海')) \
                        .withColumn('units', func.when(col('volume').isNull(), col('sales')/col('price')).otherwise(col('volume'))) \
                        .select('project', 'year', 'quarter', 'date', 'province', 'city', "district", 'hospital',
                                 'packid', 'units', 'sales', 'pchc').distinct()
    
        df_raw2 = dealPackid(df_raw2)
    
        df_raw3 = df_raw2.join(df_ims_molecule_info, on='packid', how='left') \
                        .join(df_market_molecule, on='molecule', how='inner') \
                        .withColumn('date', func.regexp_replace('date', '\\/', '')) \
                        .where( (col('quarter') >=  '2018Q1') & (col('quarter') <=  '2021Q3') ) \
                        .withColumn('market', col('molecule')) \
                        .select("year", "date", "quarter", "province", "city", "district", "pchc", "atc4", "nfc", "molecule", "product", "corp", "packid", "units", "sales", "market")
    
        df_pchc_map = df_raw3.groupby('pchc').agg(func.first('province').alias('province'), func.first('city').alias('city'), func.first('district').alias('district')) \
    
        df_raw4 = df_raw3.drop('province', 'city', 'district').join(df_pchc_map, on='pchc', how='left') \
                        .groupby('year', 'date', 'quarter', 'province', 'city', 'district', 'pchc', 'market', 'packid') \
                        .agg(func.sum('units').alias('units'), func.sum('sales').alias('sales')) \
                        .where(~col('province').isNull()) \
                        .select('year', 'date', 'quarter', 'province', 'city', 'district', 'pchc', 'market', 'packid', "units", "sales")
                        
        
        return df_raw4
                    
    # %% 
    def methodNotFull(df_shanghai_raw,df_ims_molecule_info, df_market_molecule):
        df_raw = reName(df_shanghai_raw, dict_rename={'month':'date', 'hospital_name':'hospital', 'value':'sales', 'hosp_code':'pchc', 'packcode':'packid', 'county':'district'})
        df_raw = dealIDLength(df_raw, colname='packid', id_length=7)
    
        df_raw2 = df_raw.where(col('hospital_flag') == 'CHC') \
                        .withColumn('province', func.lit('上海')) \
                        .withColumn('city', func.lit('上海')) \
                        .withColumn('date', func.regexp_replace('date', '\\/', '')) \
                        .withColumn('units', func.when(col('volume').isNull(), col('sales')/col('price')).otherwise(col('volume'))) \
                        .select('year', 'quarter', 'date', 'province', 'city', 'district', 'price',
                                 'packid', 'units', 'sales', 'pchc').distinct()
    
        df_raw_sh_Q3 = df_raw2.where(~col('packid').isNull()).where(col('quarter') == '2021Q3')
        df_raw_sh_Q2 = df_raw2.where(~col('packid').isNull()).where(col('quarter') == '2021Q2') \
                                            .groupby('date', 'packid').agg(func.sum('sales').alias('sales'))
    
        df_shQ2_numb = df_raw_sh_Q2.groupby('packid').count()
    
        df_pckshlist = df_shQ2_numb.where(col('count') ==3 ).select('packid').distinct()
    
        df_c_Res = df_raw_sh_Q2.join(df_pckshlist, on='packid', how='inner') \
                    .withColumn('sales_last',func.lag(col('sales')).over(Window.partitionBy('packid').orderBy('packid','date'))) \
                    .withColumn("diff", col('sales') - col('sales_last')) \
                    .withColumn('lc',func.first(col('sales')).over(Window.partitionBy('packid').orderBy('packid','date'))) \
                    .withColumn("ratio", col('diff')/col('lc')) \
                    .where(~col('ratio').isNull()) \
                    .select('packid', 'date', 'ratio')
    
    
        df_raw_sh_Q3_aug = df_raw_sh_Q3.drop('date').join(df_c_Res, on='packid', how='left') \
                                        .join(df_pckshlist, on='packid', how='inner') \
                                        .withColumn("units", col('units')*(col('ratio')+1 )) \
                                        .withColumn("sales", col('sales')*(col('ratio')+1 )) \
                                        .withColumn("date", func.when(col('date') == '202105', func.lit('202108')).otherwise(func.lit('202109')))
    
        df_raw_sh_Q2R = df_raw2.where(col('quarter') == '2021Q2')
    
    
        df_raw_sh_Q3_aug_rest = df_raw_sh_Q3.join(df_pckshlist, on='packid', how='left_anti') \
                                            .withColumn("units", col('units')*1.5 ) \
                                            .withColumn("sales", col('sales')*1.5 ) \
                                            .withColumn("date", func.lit('202108') )
    
        colist = ['year', 'quarter', 'date', 'province', 'city', 'district', 'pchc', 'packid', 'price', 'units', 'sales']
    
        df_raw_sh = df_raw_sh_Q3.select(colist).union(df_raw_sh_Q3_aug.select(colist)).union(df_raw_sh_Q3_aug_rest.select(colist))
    
        df_raw_sh_restx = df_raw2.where(col('quarter') != '2021Q3')
    
        df_raw_sh_all = df_raw_sh.select(colist).union(df_raw_sh_restx.select(colist))
    
        df_raw_sh_f = df_raw_sh_all.where(col('quarter') <= '2021Q3').where(col('quarter') >= '2018Q1').where(col('pchc') != '#N/A') \
                                .join(df_ims_molecule_info, on='packid', how='left') \
                                .join(df_market_molecule, on='molecule', how='inner') \
                                .withColumn('date', func.regexp_replace('date', '\\/', '')) \
                                .withColumn('market', col('molecule')) \
                                .where(col('units') > 0.0).where(col('sales') > 0.0).where(~col('market').isNull()) \
                                .select("year", "date", "quarter", "province", "city", "district", "pchc", "atc4", "nfc", "molecule", "product", "corp", "packid", "units", "sales", "market")
        return df_raw_sh_f
    # %%
    # 分子信息
    df_ims_molecule_info = reName(df_ims_molecule_info, dict_rename={'ATC4_Code':'atc4', 'NFC123_Code':'nfc', 'Molecule_Desc':'molecule', 'Prd_desc':'product', 'Corp_Desc':'corp'})
    
    # 市场包含的分子
    df_market_molecule = df_market_molecule.drop('version')
    
    if g_sh_method == "full":
        df_shanghai_raw_out = methodFull(df_shanghai_raw,df_ims_molecule_info, df_market_molecule)
    elif g_sh_method == "not_full":
        df_shanghai_raw_out = methodNotFull(df_shanghai_raw,df_ims_molecule_info, df_market_molecule)

    # %%
    # =========== 数据输出 =============
    # 读回
    df_out = lowCol(df_shanghai_raw_out)
    return {"out_df":df_out}
