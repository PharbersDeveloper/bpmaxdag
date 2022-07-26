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
    
    Raw_data = kwargs['df_check_pretreat']
    Raw_data = dealToNull(Raw_data)
    Raw_data = dealScheme(Raw_data, {"Pack_Number":"int", "Date":"int"})
    Raw_data_1 = Raw_data.groupby('ID', 'Date', 'min2', '通用名','商品名','Pack_ID') \
                            .agg(func.sum('Sales').alias('Sales'), func.sum('Units').alias('Units')) \
                            .withColumnRenamed('min2', 'Prod_Name')
    
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

    # %%
    #========== check_15 价格==========
    if g_id_molecule == 'True':
        check_15_a = Raw_data.where(Raw_data.Date > (current_year-1)*100+current_month-1) \
                            .groupBy('ID', 'Date', 'min1') \
                            .agg(func.sum('Sales').alias('Sales'), func.sum('Units').alias('Units')) \
                            .withColumn('Price', col('Sales')/col('Units'))
        check_15_count = check_15_a.groupBy('ID', 'min1').count()
        check_15_sum = check_15_a.groupBy('ID', 'min1').agg(func.sum('Price').alias('sum'), func.max('Price').alias('max'),  
                                                        func.min('Price').alias('min'))
    
        check_15_a = check_15_a.distinct() \
                            .join(check_15_count, on=['ID', 'min1'], how='left') \
                            .join(check_15_sum, on=['ID', 'min1'], how='left')
    
        check_15_a = check_15_a.withColumn('Mean_Price', func.when(col('count') == 1, col('sum')) \
                                                       .otherwise((col('sum') - col('min') - col('max'))/(col('count') - 2 )))
    
        check_15_b = check_15_a.withColumn('Mean_times', func.when(func.abs(col('Price')) > col('Mean_Price'), func.abs(col('Price'))/col('Mean_Price')) \
                                                           .otherwise(col('Mean_Price')/func.abs(col('Price'))))
    
        check_15 = check_15_b.groupby('ID', 'min1').pivot('Date').agg(func.sum('Mean_times')).persist()
    
        check_15 = check_15.withColumn('max', func.greatest(*[i for i in check_15.columns if "20" in i]))
    else:
         return {}
    

    # %%
    # =========== 数据输出 =============
    def lowerColumns(df):
        df = df.toDF(*[i.lower() for i in df.columns])
        return df
    
    check_15 = lowerColumns(check_15)
    
    logger.debug('数据执行-Finish')
    
    return {'out_df':check_15}