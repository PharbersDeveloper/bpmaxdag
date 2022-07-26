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
    p_out = kwargs['p_out']
    out_mode = kwargs['out_mode']
    run_id = kwargs['run_id']
    owner = kwargs['owner']
    project_name = kwargs['project_name']
    g_database_temp = kwargs['g_database_temp']
    ### input args ###
    
    ### output args ###
    ### output args ###

    from pyspark.sql import SparkSession, Window
    from pyspark.sql.types import StringType, IntegerType, DoubleType
    from pyspark.sql import functions as func
    import os
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col
    import time
    import re   
    from phcli.ph_tools.addTable.addTableToGlue import AddTableToGlue
    
    # %%
    # project_name = 'Gilead'
    # outdir = '202101'
    # if_two_source = 'True'
    # cut_time_left = '202101'
    # cut_time_right = '202101'
    # test = 'True'
    
    # %%
    # 输入 
    g_table_same_sheet_dup = 'rawdata_same_sheet_dup'
    
    std_names = ["Date", "ID", "Raw_Hosp_Name", "Brand", "Form", "Specifications", "Pack_Number", "Manufacturer", 
    "Molecule", "Source", "Corp", "Route", "ORG_Measure"]
    
    
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
        df = dealScheme(df, dict_scheme)
        df = getInputVersion(df, table_name.replace('df_', ''))
        return df
    
    # 上传的 raw_data
    raw_data = readInFile('df_max_raw_data_upload')
    
    molecule_adjust = readInFile('df_molecule_adjust')
            
    
    # %%
    # =============  数据执行 ==============
    # raw_data = spark.read.csv(raw_data_path, header=True)    
    if 'Corp' not in raw_data.columns:
        raw_data = raw_data.withColumn('Corp', func.lit(''))
    if 'Route' not in raw_data.columns:
        raw_data = raw_data.withColumn('Route', func.lit(''))
    for colname, coltype in raw_data.dtypes:
        if coltype == "boolean":
            raw_data = raw_data.withColumn(colname, raw_data[colname].cast(StringType()))
    
    # ==========  1. 同sheet去重(两行完全一样的)  ========== 
    def dropDupFromSameSheet(raw_data, std_names):
        # 1. 同sheet去重(两行完全一样的)
        # 去重的结果
        raw_data = raw_data.groupby(raw_data.columns).count()
        # 重复的信息
        same_sheet_dup = raw_data.where(raw_data['count'] > 1)

        # 重复条目数情况
        describe = same_sheet_dup.groupby('Sheet', 'Path').count() \
                            .withColumnRenamed('count', 'dup_count') \
                            .join(raw_data.groupby('Sheet', 'Path').count(), on=['Sheet', 'Path'], how='left')
        describe = describe.withColumn('ratio', describe['dup_count']/describe['count'])
        logger.debug("同Sheet中重复条目数:", describe.show())


        # 同sheet重复条目输出
        if same_sheet_dup.count() > 0:
            #same_sheet_dup = same_sheet_dup.repartition(1)
            #same_sheet_dup.write.format("csv").option("header", "true") \
            #    .mode("overwrite").save(same_sheet_dup_path)
            AddTableToGlue(df=same_sheet_dup, database_name_of_output=g_database_temp, table_name_of_output=g_table_same_sheet_dup, 
                           path_of_output_file=p_out, mode=out_mode) \
                    .add_info_of_partitionby({"version":run_id,"provider":project_name,"owner":owner})

        # group计算金额和数量（std_names + ['Sheet', 'Path']）
        raw_data = raw_data.withColumn('Sales', raw_data['Sales'].cast(DoubleType())) \
                        .withColumn('Units', raw_data['Units'].cast(DoubleType())) \
                        .withColumn('Units_Box', raw_data['Units_Box'].cast(DoubleType()))
        raw_data = raw_data.groupby(std_names + ['Sheet', 'Path']).agg(func.sum(col('Sales')).alias('Sales'), 
                                                    func.sum(col('Units')).alias('Units'), 
                                                    func.sum(col('Units_Box')).alias('Units_Box')).persist()
        return raw_data

    def moleculeAdjust(raw_data, molecule_adjust):
        # 分子名新旧转换
        # molecule_adjust = spark.read.csv(molecule_adjust_path, header=True)
        molecule_adjust = molecule_adjust.dropDuplicates(["Mole_Old"])

        raw_data = raw_data.join(molecule_adjust, raw_data['Molecule'] == molecule_adjust['Mole_Old'], how='left').persist()
        raw_data = raw_data.withColumn("S_Molecule", func.when(col('Mole_New').isNull(), col('Molecule')).otherwise(col('Mole_New'))) \
                            .drop('Mole_Old', 'Mole_New')
        return raw_data
    
    
    raw_data_dedup = dropDupFromSameSheet(raw_data, std_names)
    raw_data_dedup = moleculeAdjust(raw_data_dedup, molecule_adjust)
    
    
    # %%
    # =========== 数据输出 =============
    def lowerColumns(df):
        df = df.toDF(*[i.lower() for i in df.columns])
        return df

    raw_data_dedup = lowerColumns(raw_data_dedup)
    
    return {"out_df":raw_data_dedup}

