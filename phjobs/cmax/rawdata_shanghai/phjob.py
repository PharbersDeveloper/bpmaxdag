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
    # g_lack_month=['202108', '202109']
    # g_current_quarter = '2021Q3'
    # g_last_quarter = '2021Q2'
    # g_min_quarter = '2018Q1'
    
    g_sh_method = kwargs['g_sh_method']
    g_current_quarter = kwargs['g_current_quarter']
    g_min_quarter = kwargs['g_min_quarter']
    g_last_quarter = kwargs['g_last_quarter']
    g_lack_month = kwargs['g_lack_month']
    
    
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
    # df_shanghai_packid_moleinfo = readClickhouse('default', 'F9YGH7iTKuoygfrd_shanghai_packid_moleinfo', 'shanghai_201805_202107_all')
    
    df_ims_molecule_info = readInFile(kwargs['df_ims_molecule_info'])
    df_market_molecule = readInFile(kwargs['df_market_molecule'])
    df_shanghai_raw = readInFile(kwargs['df_shanghai_packid_moleinfo'])

    # %% 
    # =========== 函数定义 =============
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
    
    def dealPackid(df):
        df = df.withColumn('packid', func.when(func.substring('packid', 1, 5)=='47775', func.concat(func.lit('58906'), func.substring('packid', 6, 2))).otherwise(col('packid'))) \
                .withColumn('packid', func.when(func.substring('packid', 1, 5)=='06470', func.concat(func.lit('64895'), func.substring('packid', 6, 2))).otherwise(col('packid')))
        return df
    
    def dealRawCommon(df, g_min_quarter, g_current_quarter):
        # rawdata清洗，join信息
        df1 = reName(df, dict_rename={'month':'date', 'hospital_name':'hospital', 'value':'sales', 'hosp_code':'pchc', 'packcode':'packid', 'county':'district'})
        df1 = dealIDLength(df1, colname='packid', id_length=7)
        
        df1 = df1.where(col('hospital_flag') == 'CHC') \
                .withColumn('province', func.lit('上海')) \
                .withColumn('city', func.lit('上海')) \
                .withColumn('units', func.when(col('volume').isNull(), col('sales')/col('price')).otherwise(col('volume'))) \
                .withColumn('date', func.regexp_replace('date', '\\/', '')) \
                .where( (col('quarter') >= g_min_quarter) & (col('quarter') <= g_current_quarter) )    
        return df1 
    
    
    def methodFull(df_raw1, df_ims_molecule_info, df_market_molecule):
        df_raw2 = df_raw1.where( (~col('packid').isNull()) & (~col('pchc').isNull()) ) \
                        .select('project', 'year', 'quarter', 'date', 'province', 'city', "district", 'hospital',
                                 'packid', 'units', 'sales', 'pchc').distinct()
    
        df_raw2 = dealPackid(df_raw2)
    
        df_raw3 = df_raw2.join(df_ims_molecule_info, on='packid', how='left') \
                        .join(df_market_molecule, on='molecule', how='inner') \
                        .withColumn('market', col('molecule')) \
                        .select("year", "date", "quarter", "province", "city", "district", "pchc", "atc4", "nfc", "molecule", "product", "corp", "packid", "units", "sales", "market")
    
        df_pchc_map = df_raw3.groupby('pchc').agg(func.first('province').alias('province'), func.first('city').alias('city'), func.first('district').alias('district')) \
    
        df_raw4 = df_raw3.drop('province', 'city', 'district').join(df_pchc_map, on='pchc', how='left') \
                        .groupby('year', 'date', 'quarter', 'province', 'city', 'district', 'pchc', 'market', 'packid') \
                        .agg(func.sum('units').alias('units'), func.sum('sales').alias('sales')) \
                        .where(~col('province').isNull()) \
                        .select('year', 'date', 'quarter', 'province', 'city', 'district', 'pchc', 'market', 'packid', "units", "sales")
                        
        
        return df_raw4
    
    def calLastQGrowthRate(df_raw_sh_Q2):
        # 计算上个季度，后两个月的增长率
        # 上个季度3个月均有数的packid
        df_pckshlist = df_raw_sh_Q2.groupby('packid').count() \
                                    .where(col('count') ==3 ) \
                                    .select('packid').distinct()  
        # 获得4-5月,4-6月增长率
        df_c_Res = df_raw_sh_Q2.join(df_pckshlist, on='packid', how='inner') \
                                .withColumn('sales_last',func.lag(col('sales')).over(Window.partitionBy('packid').orderBy('packid','date'))) \
                                .withColumn("diff", col('sales') - col('sales_last')) \
                                .withColumn('lc',func.first(col('sales'), ignorenulls=True).over(Window.partitionBy('packid').orderBy('packid','date'))) \
                                .withColumn("ratio", col('diff')/col('lc')) \
                                .where(~col('ratio').isNull()) \
                                .select('packid', 'date', 'ratio')
        return df_c_Res

    def getAddMonth(lack_month, g_last_quarter):
        lack_month = int(lack_month)
        if lack_month%100 - 3 <= 0:
            add_month = int(g_last_quarter[0:4])*100 + 12 - abs(lack_month%100 - 3)
        else:
            add_month = int(lack_month - 3)
        return str(add_month)

    def calCurrentQLackData(df_raw_sh_Q3, df_c_Res):
        # 当前季度
        # 两个增长率分别乘到 7月的数据上，得到上海8月和9月的数据 
        df_raw_sh_Q3_aug = df_raw_sh_Q3.drop('date').join(df_c_Res, on='packid', how='inner') \
                                        .withColumn("units", col('units')*(col('ratio')+1 )) \
                                        .withColumn("sales", col('sales')*(col('ratio')+1 ))

        for i_lack_month in g_lack_month:
            i_add_month = getAddMonth(i_lack_month, g_last_quarter)
            df_raw_sh_Q3_aug = df_raw_sh_Q3_aug.withColumn("date", func.when(col('date') == i_add_month, func.lit(i_lack_month)).otherwise(col('date')))

        df_raw_sh_Q3_aug = df_raw_sh_Q3_aug.where(col('date').isin(g_lack_month.replace(' ','').split(',')))

        df_raw_sh_Q3_aug_rest = df_raw_sh_Q3.join(df_c_Res.select('packid').distinct(), on='packid', how='left_anti') \
                                            .withColumn("units", col('units')*1.5 ) \
                                            .withColumn("sales", col('sales')*1.5 ) \
                                            .withColumn("date", func.lit(min(g_lack_month)))
        df_raw_sh = unionDf(df_raw_sh_Q3, df_raw_sh_Q3_aug, utype='same')
        df_raw_sh = unionDf(df_raw_sh, df_raw_sh_Q3_aug_rest, utype='same')
        return df_raw_sh
                    
    def methodNotFull(df_raw1, df_ims_molecule_info, df_market_molecule, g_current_quarter, g_last_quarter):
        # 上海的数据有时候会到不齐，需要补全。 如果上海只有7月的数据，那么
        # 1. 根据历史数据计算上海每一个packcode PCHC code, 省份城市区县, 市场 4-5月的增长率，和 4-6月的增长率
        # 2. 将两个增长率分别乘到 7月的数据上，得到上海8月和9月的数据
        # ======== 数据分析 ==========
        df_raw1 = df_raw1.select('year', 'quarter', 'date', 'province', 'city', 'district', 'price', 'packid', 'units', 'sales', 'pchc').distinct()
    
        df_raw_sh_Q3 = df_raw1.where(~col('packid').isNull()).where(col('quarter') == g_current_quarter)
        df_raw_sh_restx = df_raw1.where(col('quarter') != g_current_quarter)
        df_raw_sh_Q2 = df_raw1.where(~col('packid').isNull()).where(col('quarter') == g_last_quarter) \
                                    .groupby('date', 'packid').agg(func.sum('sales').alias('sales'))
                 
        # 计算上季度增长率
        df_c_Res = calLastQGrowthRate(df_raw_sh_Q2)
        # 对当前季度补数
        df_raw_sh = calCurrentQLackData(df_raw_sh_Q3, df_c_Res)
        # 数据合并
        df_raw_sh_all = unionDf(df_raw_sh, df_raw_sh_restx, utype='same')
    
        df_raw_sh_f = df_raw_sh_all.where(col('pchc') != '#N/A') \
                                .join(df_ims_molecule_info, on='packid', how='left') \
                                .join(df_market_molecule, on='molecule', how='inner') \
                                .withColumn('market', col('molecule')) \
                                .where(col('units') > 0.0).where(col('sales') > 0.0).where(~col('market').isNull()) \
                                .select("year", "date", "quarter", "province", "city", "district", "pchc", "atc4", "nfc", "molecule", "product", "corp", "packid", "units", "sales", "market")
        return df_raw_sh_f
                              

    # %%
    # ======== 数据执行 ========== 
    # 分子信息
    df_ims_molecule_info = reName(df_ims_molecule_info, dict_rename={'ATC4_Code':'atc4', 'NFC123_Code':'nfc', 'Molecule_Desc':'molecule', 'Prd_desc':'product', 'Corp_Desc':'corp'})
    
    # 市场包含的分子
    df_market_molecule = df_market_molecule.drop('version')
    
    df_raw1 = dealRawCommon(df_shanghai_raw, g_min_quarter, g_current_quarter)
    if g_sh_method == "full":
        df_shanghai_raw_out = methodFull(df_raw1, df_ims_molecule_info, df_market_molecule)
    elif g_sh_method == "notfull":
        df_shanghai_raw_out = methodNotFull(df_raw1, df_ims_molecule_info, df_market_molecule, g_current_quarter, g_last_quarter)

    # %%
    # =========== 数据输出 =============
    # 读回
    df_out = lowCol(df_shanghai_raw_out)
    return {"out_df":df_out}

