# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL


def execute(**kwargs):
    logger = phs3logger(kwargs["run_id"], LOG_DEBUG_LEVEL)
    spark = kwargs['spark']
    
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
    # %%
    # =========== 输入数据读取 ===========
    def dealToNull(df):
        df = df.replace(["None", ""], None)
        return df
    
    def dealScheme(df, dict_scheme):
        # 数据类型处理 {"col":"type"}
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
        
    # %% 
    # =========== 输入数据读取 =========== 
    df_cn_prod_ref = readInFile('df_cn_prod_ref')
    df_ims_chpa = readInFile('df_ims_chpa')
    df_cn_mol_lkp = readInFile('df_cn_mol_lkp')
    df_ims_mol_ref = readInFile('df_cn_mol_ref')
    df_ims_corp_ref = readInFile('df_cn_corp_ref')

    # %%
    # ==========  数据执行  ============
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
    
    df_ims_prod_ref = dealIDLength(df_cn_prod_ref, colname='pack_id', id_length=7)
    
    df_ims_mol1 = df_ims_chpa.distinct() \
                            .where(~col('pack_id').isNull()) \
                            .join(df_ims_prod_ref.select('pack_id', 'nfc123_code'), on='pack_id', how='left') \
                            .select('pack_id', 'corp_id', 'corp_desc', 'mnf_type', 'mnftype_desc', 'mnf_desc', 'atc4_code', 'nfc123_code', 'prd_desc', 'pck_desc', 'molecule_desc')
    
    
    df_ims_mol_lkp_ref = dealIDLength(df_cn_mol_lkp, colname='pack_id', id_length=7)
    
    df_ims_mol2 = df_ims_mol_lkp_ref.select('pack_id', 'molecule_id') \
                                    .join(df_ims_mol_ref, on=['molecule_id'], how='left') \
                                    .orderBy('pack_id', 'molecule_desc') \
                                    .groupby('pack_id').agg(func.collect_list('molecule_desc').alias('molecule_desc_list')) \
                                    .withColumn('molecule_desc', func.array_join('molecule_desc_list', '+')) \
                                    .select('pack_id', 'molecule_desc') \
                                    .join(df_ims_prod_ref, on='pack_id', how='left') \
                                    .join(df_ims_corp_ref, on='corp_id', how='left') \
                                    .select('pack_id', 'corp_id', 'corp_desc', 'atc4_code', 'nfc123_code', 'prd_desc', 'pck_desc', 'molecule_desc')
    
    df_ims_mol = df_ims_mol2.join(df_ims_mol1, on='pack_id', how='left_anti')
    df_ims_mol = dealIDLength(df_ims_mol, colname='corp_id', id_length=4)
    df_ims_mol = reduce(lambda df, i: df.withColumn(i, func.lit(None).cast('string')) if i not in df.columns else df, df_ims_mol1.columns, df_ims_mol)
    df_ims_mol = df_ims_mol.union(df_ims_mol1.select(df_ims_mol.columns))
    df_ims_mol = df_ims_mol.withColumnRenamed('pack_id', 'packid')
    
    # %%
    # =========== 数据输出 =============
    # 读回
    df_out = lowCol(df_ims_mol)
    return {"out_df":df_out}
