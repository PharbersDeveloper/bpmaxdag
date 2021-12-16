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
    project_name = kwargs['project_name']
    out_path = kwargs['out_path']
    run_id = kwargs['run_id']
    owner = kwargs['owner']
    # g_input_version = kwargs['g_input_version']
    g_database_temp = kwargs['g_database_temp']
    g_database_input = kwargs['g_database_input']
    ### input args ###
    
    ### output args ###
    # g_out_price_city = kwargs['g_out_price_city']
    ### output args ###

    from pyspark.sql import SparkSession, Window
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    import os
    import pandas as pd
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col    
    import json
    import boto3        # %%
    # =========== 数据执行 =============
    logger.debug('数据执行-start')
    raw_data = kwargs["df_raw_data_deal_poi"]
    # %%
    # ==== 计算价格 ====
    logger.debug('价格计算')
    
    # 补数部分的数量需要用价格得出
    # 2、城市层面
    price_city = raw_data.groupBy("min2", "date", 'City', 'Province') \
                        .agg((func.sum("Sales") / func.sum("Units")).alias("Price"))
    price_city = price_city.where(~col('Price').isNull())
    
    # %%
    # =========== 数据输出 =============
    def lowerColumns(df):
        df = df.toDF(*[i.lower() for i in df.columns])
        return df
    
    price_city = lowerColumns(price_city)
    
    logger.debug('数据执行-Finish')
    
    return {'out_df':price_city}
