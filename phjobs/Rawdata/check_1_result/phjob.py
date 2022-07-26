# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL


def execute(**kwargs):
    logger = phs3logger(kwargs["run_id"], LOG_DEBUG_LEVEL)
    spark = kwargs['spark']
    
    ### input args ###
    minimum_product_sep = kwargs['minimum_product_sep']
    minimum_product_columns = kwargs['minimum_product_columns']
    current_year = kwargs['current_year']
    current_month = kwargs['current_month']
    three = kwargs['three']
    twelve = kwargs['twelve']
    g_id_molecule = kwargs['g_id_molecule']
    ### input args ###
    
    ### output args ###
    ### output args ###

    
    
    from pyspark.sql import SparkSession, Window
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    import os
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, greatest, least, col
    import time
    import pandas as pd
    import numpy as np    

    # %%
    # 输入
    current_year = int(current_year)
    current_month = int(current_month)
    three = int(three)
    twelve = int(twelve)
    
    if minimum_product_sep == "kong":
        minimum_product_sep = ""
    minimum_product_columns = minimum_product_columns.replace(" ","").split(",")

    
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
    
    check_1 = kwargs['df_check_1']
    check_1 = dealToNull(check_1)
    check_1 = dealScheme(check_1, {'date':'int', '每月产品个数_min2':'int'})
    
    # %%
    # ================= 数据执行 ==================	
    
    MTH = current_year*100 + current_month
    
    if MTH%100 == 1:
        PREMTH = (MTH//100 -1)*100 +12
    else:
        PREMTH = MTH - 1
            
    # 当前月的前3个月
    if three > (current_month - 1):
        diff = three - current_month
        RQMTH = [i for i in range((current_year - 1)*100 +12 - diff , (current_year - 1)*100 + 12 + 1)] + [i for i in range(MTH - current_month + 1 , MTH)]
    else:
        RQMTH = [i for i in range(MTH - current_month + 1 , MTH)][-three:]
    
    # 当前月的前12个月
    if twelve > (current_month - 1):
        diff = twelve - current_month
        mat_month = [i for i in range((current_year - 1)*100 + 12 - diff , (current_year - 1)*100 + 12 + 1)] + [i for i in range(MTH - current_month + 1 , MTH)]
    else:
        mat_month = [i for i in range(MTH - current_month + 1 , MTH)][-twelve:]

 
    #========== check_1 ==========   
    ### 判断产品个数与上月相比是否超过 8%
    MTH_product_num = check_1.where(col('Date') == MTH).toPandas()['每月产品个数_min2'][0]
    PREMTH_product_num = check_1.where(col('Date') == PREMTH).toPandas()['每月产品个数_min2'][0]
    check_result_1 = (MTH_product_num/PREMTH_product_num < 0.08)
    
    def getResultDf(result, colname):
        dict = {colname:[str(result)]}
        df = pd.DataFrame(dict)
        df_out = spark.createDataFrame(df) 
        return df_out
    
    df_check_result_1 = getResultDf(check_result_1, colname = '产品个数与历史相差不超过008')

    # %%
    # =========== 数据输出 =============
    def lowerColumns(df):
        df = df.toDF(*[i.lower() for i in df.columns])
        return df
      
    df_check_result_1 = lowerColumns(df_check_result_1)
    
    logger.debug('数据执行-Finish')
    
    return {'out_df':df_check_result_1}