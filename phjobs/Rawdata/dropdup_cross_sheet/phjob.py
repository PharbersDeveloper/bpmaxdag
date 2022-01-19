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
    p_out = kwargs['p_out']
    out_mode = kwargs['out_mode']
    run_id = kwargs['run_id'].replace(":","_")
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
    g_table_across_sheet_dup_bymonth = 'rawdata_across_sheet_dup_bymonth'
    g_table_across_sheet_dup_bysales = 'rawdata_across_sheet_dup_bysales'

       
    # %% 
    # =========== 输入数据读取 =========== 
    def dealToNull(df):
        df = df.replace(["None", ""], None)
        return df
    
    raw_data = kwargs['df_dropdup_same_sheet']
    raw_data = dealToNull(raw_data)
                
    
    # %%
    # =============  数据执行 ==============
    # ==========  2. 跨sheet去重  ==========   
    
    @udf(StringType())
    def path_split(path):
        # 获取path的日期文件夹名, 纯数字的为文件夹名，只要包含字符就不是月份名
        path_month = path.replace('//', '/').split('/')
        month = ''
        for each in path_month:
            if len(re.findall('\D+', each)) == 0:
                month = each
        return month
    
    def dropDupBymonth(raw_data):
        # 2.1 path来自不同月份文件夹：'Date, S_Molecule' 优先最大月份文件夹来源的数据，生成去重后结果 raw_data_dedup_bymonth
        dedup_check_bymonth = raw_data.select('Date', 'S_Molecule', 'Path', 'Sheet', 'Source').distinct() \
                                    .withColumn('month_dir', path_split(col('Path')))
        dedup_check_bymonth = dedup_check_bymonth.withColumn('month_dir', col('month_dir').cast(IntegerType()))
        # func.rank() 排名重复占位
        dedup_check_bymonth = dedup_check_bymonth.withColumn('rank', func.rank().over(Window.partitionBy('Date', 'S_Molecule', 'Source') \
                                                                        .orderBy(dedup_check_bymonth['month_dir'].desc()))).persist()
        dedup_check_bymonth = dedup_check_bymonth.where(dedup_check_bymonth['rank'] == 1).select('Date', 'S_Molecule', 'Path', 'Sheet','Source')

        # 去重后数据
        raw_data_dedup_bymonth = raw_data.join(dedup_check_bymonth, on=['Date', 'S_Molecule', 'Path', 'Sheet','Source'], how='inner').persist()
        # 重复数据
        across_sheet_dup_bymonth = raw_data.join(dedup_check_bymonth, on=['Date', 'S_Molecule', 'Path', 'Sheet', 'Source'], how='left_anti')

        logger.debug('跨sheet去重，不同月份文件夹来源数据去重情况：')
        logger.debug('重复条目数：', across_sheet_dup_bymonth.count())
        logger.debug('去重后条目数：', raw_data_dedup_bymonth.count())
        logger.debug('总条目数：', raw_data.count())

        if across_sheet_dup_bymonth.count() > 0:
            AddTableToGlue(df=across_sheet_dup_bymonth, database_name_of_output=g_database_temp, table_name_of_output=g_table_across_sheet_dup_bymonth, 
                           path_of_output_file=p_out, mode=out_mode) \
                    .add_info_of_partitionby({"version":run_id,"provider":project_name,"owner":owner})

        return raw_data_dedup_bymonth

    def dropDupBysales(raw_data_dedup_bymonth):
        # 2.2  保留金额大的数据，生成去重后结果 raw_data_dedup
        dedup_check_bysales = raw_data_dedup_bymonth.groupby('Date', 'S_Molecule', 'Path', 'Sheet', 'Source') \
                                .agg(func.sum(col('Sales')).alias('Sales'), func.sum(col('Units')).alias('Units'))
        # func.row_number()
        dedup_check_bysales = dedup_check_bysales.withColumn('rank', func.row_number().over(Window.partitionBy('Date', 'S_Molecule', 'Source') \
                                                                        .orderBy(dedup_check_bysales['Sales'].desc()))).persist()
        dedup_check_bysales = dedup_check_bysales.where(dedup_check_bysales['rank'] == 1).select('Date', 'S_Molecule', 'Path', 'Sheet','Source')

        # 去重后数据
        raw_data_dedup = raw_data_dedup_bymonth.join(dedup_check_bysales, on=['Date', 'S_Molecule', 'Path', 'Sheet','Source'], how='inner').persist()
        # 重复数据
        across_sheet_dup = raw_data_dedup_bymonth.join(dedup_check_bysales, on=['Date', 'S_Molecule', 'Path', 'Sheet','Source'], how='left_anti')

        logger.debug('跨sheet去重，根据金额去重情况：')
        logger.debug('重复条目数：', across_sheet_dup.count())
        logger.debug('去重后条目数：', raw_data_dedup.count())
        logger.debug('总条目数：', raw_data.count())

        if across_sheet_dup.count() > 0:
            AddTableToGlue(df=across_sheet_dup, database_name_of_output=g_database_temp, table_name_of_output=g_table_across_sheet_dup_bysales, 
                           path_of_output_file=p_out, mode=out_mode) \
                    .add_info_of_partitionby({"version":run_id,"provider":project_name,"owner":owner})

        return raw_data_dedup
    
    def dropDupCrossSheet(raw_data):       
        raw_data_dedup_bymonth = dropDupBymonth(raw_data)
        raw_data_dedup = dropDupBysales(raw_data_dedup_bymonth)
        return raw_data_dedup
    
    raw_data_dedup = dropDupCrossSheet(raw_data)
    
    # %%
    # =========== 数据输出 =============
    def lowerColumns(df):
        df = df.toDF(*[i.lower() for i in df.columns])
        return df

    raw_data_dedup = lowerColumns(raw_data_dedup)    
    return {"out_df":raw_data_dedup}