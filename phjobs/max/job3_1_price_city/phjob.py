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
    ### input args ###
    
    ### output args ###
    ### output args ###

    from pyspark.sql import SparkSession, Window
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    import os
    import pandas as pd
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col    
    import json
    import boto3        
    
    # %%
    # =========== 输入数据读取 =========== 
    logger.debug('数据执行-start')
    
    def dealToNull(df):
        df = df.replace(["None", ""], None)
        return df
       
    raw_data = kwargs["df_raw_data_deal_poi"]
    raw_data = dealToNull(raw_data)
    
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
