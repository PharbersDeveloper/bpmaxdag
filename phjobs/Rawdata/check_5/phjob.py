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
    Raw_data = dealScheme(Raw_data, {"Pack_Number":"int", "Date":"int", 'sales':'double', 'units':'double'})
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

 
    #========== check_5 ==========
    
    # 最近12期每家医院每个月的销量规模
    check_5_1 = Raw_data.where(Raw_data.Date > (current_year - 1)*100 + current_month - 1) \
                        .groupBy('ID', 'Date').agg(func.sum('Sales').alias('Sales'))
    check_5_1 = check_5_1.groupBy("ID").pivot("Date").agg(func.sum('Sales')).persist() \
                        .orderBy('ID').persist()
    
    ### 检查当月缺失医院在上个月的销售额占比
    MTH_hospital_Sales = check_5_1.where(check_5_1[str(MTH)].isNull()).groupBy().agg(func.sum(str(PREMTH)).alias('sum')).toPandas()['sum'][0]
    PREMTH_hospital_Sales = check_5_1.groupBy().agg(func.sum(str(PREMTH)).alias('sum')).toPandas()['sum'][0]
    if MTH_hospital_Sales == None:
        MTH_hospital_Sales = 0
    check_result_5 = (MTH_hospital_Sales/PREMTH_hospital_Sales < 0.01)
    
    # 每家医院的月销金额在最近12期的误差范围内（mean+-1.96std），范围内的医院数量占比大于95%；
    check_5_2 = Raw_data.where((Raw_data.Date > (current_year-1)*100+current_month-1 ) & (Raw_data.Date < current_year*100+current_month)) \
                        .groupBy('ID', 'Date').agg(func.sum('Sales').alias('Sales')) \
                        .groupBy('ID').agg(func.mean('Sales').alias('Mean_Sales'), func.stddev('Sales').alias('Sd_Sales')).persist()
    
    check_5_2 = check_5_2.join(Raw_data.where(col('Date') == current_year*100+current_month).groupBy('ID').agg(func.sum('Sales').alias('Sales_newmonth')), 
                                            on='ID', how='left').persist()
    check_5_2 = check_5_2.withColumn('Check', func.when(col('Sales_newmonth') < col('Mean_Sales')-1.96*col('Sd_Sales'), func.lit('F')) \
                                                .otherwise(func.when(col('Sales_newmonth') > col('Mean_Sales')+1.96*col('Sd_Sales'), func.lit('F')) \
                                                                .otherwise(func.lit('T'))))
    check_5_2 = check_5_2.withColumn('Check', func.when(func.isnan(col('Mean_Sales')) | func.isnan(col('Sd_Sales')) | col('Sales_newmonth').isNull(), func.lit(None)) \
                                                    .otherwise(col('Check')))                            
    
    check_5 = check_5_1.join(check_5_2, on='ID', how='left').orderBy('ID').persist()

    # %%
    # =========== 数据输出 =============
    def lowerColumns(df):
        df = df.toDF(*[i.lower() for i in df.columns])
        return df
    
    check_5 = lowerColumns(check_5)
    
    logger.debug('数据执行-Finish')
    
    return {'out_df':check_5}