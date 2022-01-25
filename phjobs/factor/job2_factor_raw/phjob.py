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
    model_month_right = kwargs['model_month_right']
    model_month_left = kwargs['model_month_left']
    all_models = kwargs['all_models']
    ### input args ###
    
    ### output args ###
    p_out = kwargs['p_out']
    out_mode = kwargs['out_mode']
    run_id = kwargs['run_id'].replace(":","_")
    owner = kwargs['owner']
    project_name = kwargs['project_name']
    g_database_temp = kwargs['g_database_temp']
    ### output args ###

    
    
    from pyspark.sql import SparkSession, Window
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    import os
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col
    import time
    import re        
    from phcli.ph_tools.addTable.addTableToGlue import AddTableToGlue

    # %%
    # =========== 参数处理 =========== 
    model_month_right = int(model_month_right)
    model_month_left = int(model_month_left)
    all_models = all_models.replace(' ','').split(',')
    
    g_table_result = 'factor_raw'
    
    # %% 
    # =========== 输入数据读取 =========== 
    def dealToNull(df):
        df = df.replace(["None", ""], None)
        return df
    def dealScheme(df, dict_scheme):
        # 数据类型处理
        for i in dict_scheme.keys():
            df = df.withColumn(i, col(i).cast(dict_scheme[i]))
        return df
    def lowCol(df):
        df = df.toDF(*[c.lower() for c in df.columns])
        return df
    
    df_universe = kwargs['df_universe_base']
    df_universe = dealToNull(df_universe)
    df_universe = lowCol(df_universe)
    # 样本ID
    ID_list = df_universe.where(col('panel') == 1).select('panel_id').distinct().toPandas()['panel_id'].values.tolist()
    
    df_max_result = kwargs['df_max_result']
    df_max_result = dealToNull(df_max_result)
    df_max_result = lowCol(df_max_result)
    df_max_result = df_max_result.where((col('date') >= model_month_left) & (col('date') <= model_month_right))
    
    df_rf_out_all = kwargs['df_randomforest_result']
    df_rf_out_all = dealToNull(df_rf_out_all)    
    df_rf_out_all = lowCol(df_rf_out_all)
    
    # ============== 删除已有的s3中间文件 =============
    import boto3
    def deletePath(path_dir):
        file_name = path_dir.replace('//', '/').split('s3:/ph-platform/')[1]
        s3 = boto3.resource('s3', region_name='cn-northwest-1',
                            aws_access_key_id="AKIAWPBDTVEAEU44ZAGT",
                            aws_secret_access_key="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")
        bucket = s3.Bucket('ph-platform')
        bucket.objects.filter(Prefix=file_name).delete()
    deletePath(path_dir=f"{p_out + g_table_result}/version={run_id}/provider={project_name}/owner={owner}/")
    
    # %%
    # =========== 数据执行 ============
    logger.debug("job2_factor_raw")
    # 每个市场算 factor
   
    for market in all_models:
        logger.debug("当前market为:" + str(market))
        
        # rf 非样本
        df_rf_out = df_rf_out_all.where(col('doi') == market)
        df_rf_out = df_rf_out.select('pha_id', 'final_sales') \
                        .join(df_universe.select('panel_id', 'province', 'city').distinct(), 
                                df_rf_out['pha_id'] == df_universe['panel_id'], how='left') \
                        .where(~col('pha_id').isin(ID_list))
        df_rf_out = df_rf_out.groupBy('city', 'province').agg(func.sum('final_sales').alias('sales_rf'))
    
        # max 非样本
        df_spotfire_out = df_max_result.where(col('doi') == market)
        df_spotfire_out = df_spotfire_out.where(col('panel') != 1) \
                                .groupBy('city', 'province').agg(func.sum('predict_sales').alias('Sales'))
    
        # 计算factor 城市层面 ： rf 非样本的Sales 除以  max 非样本 的Sales                
        df_factor_city = df_spotfire_out.join(df_rf_out, on=['city', 'province'], how='left')
        df_factor_city = df_factor_city.withColumn('factor', col('sales_rf')/col('sales'))
    
        # df_universe join left factor_city 没有的城市factor为1
        df_factor_city1 = df_universe.select('city', 'province').distinct() \
                                .join(df_factor_city, on=['city', 'province'], how='left')
        df_factor_city1 = df_factor_city1.withColumn('factor', func.when(((col('factor').isNull()) | (col('factor') <=0)), func.lit(1)) \
                                                            .otherwise(col('factor')))
        df_factor_city1 = df_factor_city1.withColumn('factor', func.when(col('factor') >4, func.lit(4)) \
                                                            .otherwise(col('factor')))
    
        df_factor_out = df_factor_city1.select('city', 'province', 'factor')
        
        df_factor_out = df_factor_out.withColumn('doi', func.lit(market))

        
        def lowerColumns(df):
            df = df.toDF(*[i.lower() for i in df.columns])
            return df
        df_factor_out = lowerColumns(df_factor_out)
        
        AddTableToGlue(df=df_factor_out, database_name_of_output=g_database_temp, table_name_of_output=g_table_result, 
                           path_of_output_file=p_out, mode=out_mode) \
                    .add_info_of_partitionby({"version":run_id,"provider":project_name,"owner":owner})
        
        logger.debug("finish:" + str(market))
    
    # %%
    # =========== 数据输出 =============
    # 读回
    df_result = spark.sql("SELECT * FROM %s.%s WHERE version='%s' AND provider='%s' AND  owner='%s'" 
                                 %(g_database_temp, g_table_result, run_id, project_name, owner))
    
    df_result = df_result.drop('version', 'provider', 'owner')
    return {"out_df":df_result}       
    
        
        

