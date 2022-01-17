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
    if_two_source = kwargs['if_two_source']
    
    p_out = kwargs['p_out']
    out_mode = kwargs['out_mode']
    run_id = kwargs['run_id']
    owner = kwargs['owner']
    project_name = kwargs['project_name']
    g_database_temp = kwargs['g_database_temp']
    ### input args ###
    
    ### output args ###
    c = kwargs['c']
    d = kwargs['d']
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
    if if_two_source != "False" and if_two_source != "True":
        phlogger.error('wrong input: if_two_source, False or True') 
        raise ValueError('wrong input: if_two_source, False or True')
    
    # %% 
    # =========== 输入数据读取 =========== 
    def dealToNull(df):
        df = df.replace(["None", ""], None)
        return df
    
    # 上传的 raw_data
    raw_data = kwargs['df_dropdup_cross_sheet']
    raw_data = dealToNull(raw_data)

    if if_two_source == 'True':  
        cpa_pha_mapping_common = kwargs['df_cpa_pha_mapping_common']
        cpa_pha_mapping_common = dealToNull(cpa_pha_mapping_common)
            
    
    # %%
    # =============  数据执行 ==============
    # ==========  3. 跨源去重，跨源去重优先保留CPA医院  ==========  
    
    # ID 的长度统一
    def dealIDLength(df, colname='ID'):
        # ID不足7位的前面补0到6位
        # 国药诚信医院编码长度是7位数字，cpa医院编码是6位数字
        # 其他来源的ID 还有包含字母的, 所以要为字符型，不能转为 数值型
        df = df.withColumn(colname, col(colname).cast(StringType()))
        # 去掉末尾的.0
        df = df.withColumn(colname, func.regexp_replace(colname, "\\.0", ""))
        df = df.withColumn(colname, func.when(func.length(col(colname)) < 7, func.lpad(col(colname), 6, "0")).otherwise(col(colname)))
        return df

    # %%
    # 3. 跨源去重，跨源去重优先保留CPA医院  
    def drop_dup_hospital(df, cpa_pha_map):
        column_names = df.columns
        df = df.join(cpa_pha_map, on='ID', how='left')

        Source_window = Window.partitionBy("Date", "PHA").orderBy(func.col('Source'))
        rank_window = Window.partitionBy("Date", "PHA").orderBy(func.col('Source').desc())
        Source = df.select("Date", "PHA", 'Source').distinct() \
            .select("Date", "PHA", func.collect_list(func.col('Source')).over(Source_window).alias('Source_list'),
            func.rank().over(rank_window).alias('rank')).persist()
        Source = Source.where(col('rank') == 1).drop('rank')
        Source = Source.withColumn('count', func.size('Source_list'))

        df = df.join(Source, on=['Date', 'PHA'], how='left')

        # 无重复
        df1 = df.where((df['count'] <=1) | (df['count'].isNull()))
        # 有重复
        df2 = df.where(df['count'] >1)
        df2 = df2.withColumn('Source_choice', 
                        func.when(func.array_contains('Source_list', 'CPA'), func.lit('CPA')) \
                            .otherwise(func.when(func.array_contains('Source_list', 'GYC'), func.lit('GYC')) \
                                            .otherwise(func.lit('DDD'))))
        df2 = df2.where(df2['Source'] == df2['Source_choice'])

        # 合并
        df_all = df1.union(df2.select(df1.columns))

        '''
        df_filter = df.where(~df.PHA.isNull()).select('Date', 'PHA', 'ID').distinct() \
                                .groupby('Date', 'PHA').count()

        df = df.join(df_filter, on=['Date', 'PHA'], how='left')

        # 有重复的优先保留CPA（CPA的ID为6位，GYC的ID为7位）,其次是国药，最后是其他
        df = df.where((df['count'] <=1) | ((df['count'] > 1) & (func.length('ID')==6)) | (df['count'].isNull()))

        '''

        # 检查去重结果
        check = df_all.select('PHA','Date').distinct() \
                    .groupby('Date', 'PHA').count()
        check_dup = check.where(check['count'] >1 ).where(~col('PHA').isNull())
        if check_dup.count() > 0 :
            logger.debug('有未去重的医院')

        df_all = df_all.select(column_names)
        return df_all
    # %%
    # =========== 数据输出 =============
    def lowerColumns(df):
        df = df.toDF(*[i.lower() for i in df.columns])
        return df
    
    if if_two_source == 'True':
        cpa_pha_mapping_common = cpa_pha_mapping_common.where(cpa_pha_mapping_common["推荐版本"] == 1).select('ID', 'PHA')
        cpa_pha_mapping_common = dealIDLength(cpa_pha_mapping_common)
        raw_data = dealIDLength(raw_data)
        raw_data_dedup_std = drop_dup_hospital(raw_data, cpa_pha_mapping_common)  
        raw_data_dedup_std = lowerColumns(raw_data_dedup_std) 
        return {"out_df":raw_data_dedup_std}
    else:
        return {}
        
