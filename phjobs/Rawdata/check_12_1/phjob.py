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
    
    def dealIDLength(df, colname='ID'):
        # ID不足7位的前面补0到6位
        # 国药诚信医院编码长度是7位数字，cpa医院编码是6位数字
        # 其他来源的ID 还有包含字母的, 所以要为字符型，不能转为 数值型
        df = df.withColumn(colname, col(colname).cast(StringType()))
        # 去掉末尾的.0
        df = df.withColumn(colname, func.regexp_replace(colname, "\\.0", ""))
        df = df.withColumn(colname, func.when(func.length(col(colname)) < 7, func.lpad(col(colname), 6, "0")).otherwise(col(colname)))
        return df
    
    Raw_data = kwargs['df_check_pretreat']
    Raw_data = dealToNull(Raw_data)
    Raw_data = dealScheme(Raw_data, {"pack_number":"int", "date":"int"})
    Raw_data_1 = Raw_data.groupby('ID', 'Date', 'min2', '通用名','商品名','Pack_ID') \
                            .agg(func.sum('Sales').alias('Sales'), func.sum('Units').alias('Units')) \
                            .withColumnRenamed('min2', 'Prod_Name')
    
 
    check_have = kwargs['df_check_12'] 
    check_have = dealToNull(check_have)
    
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
    #========== check_贡献率等级相关 ==========
    
    @udf(DoubleType())
    def mean_adj(*cols):
        # 以行为单位，去掉一个最大值和一个最小值求平均
        import numpy as np
        row_max = cols[0]
        row_min = cols[1]
        others = cols[2:]
        row_mean_list = [x for x in others if x is not None]
        if len(row_mean_list) > 3:
            row_mean = (np.sum(row_mean_list) - row_max - row_min)/(len(row_mean_list) -2)
        else:
            row_mean = 0
        return float(row_mean)
    
    @udf(DoubleType())
    def min_diff(row_max, row_min, mean_adj):
        # row_min 与 mean_adj 的差值
        import numpy as np
        if mean_adj is not None:
            # diff1 = abs(row_max - mean_adj)
            diff2 = abs(row_min - mean_adj)
            row_diff = diff2
        else:
            row_diff = 0
        return float(row_diff)
    
    def func_pandas_cumsum_level(pdf, grouplist, sumcol):
        '''
        贡献率等级计算:
        分月统计
        降序排列累加求和，占总数的比值乘10，取整   
        '''
        month = pdf['date'][0]
        pdf = pdf.groupby(grouplist)[sumcol].agg('sum').reset_index()
        pdf = pdf.sort_values(sumcol, ascending=False)
        pdf['cumsum'] = pdf[sumcol].cumsum()
        pdf['sum'] = pdf[sumcol].sum()
        pdf['con_add'] = pdf['cumsum']/pdf['sum']
        pdf['level'] = np.where(pdf['con_add']*10 > 10, 10, np.ceil(pdf['con_add']*10))
        pdf ['month'] = str(month)
        pdf = pdf[grouplist + ['level', 'month']]
        pdf['level'].astype('int')
        return pdf
    
    def colculate_diff(check_num, grouplist):
        '''
        去掉最大值和最小值求平均
        最小值与平均值的差值min_diff
        '''
        check_num_cols = check_num.columns
        check_num_cols = list(set(check_num_cols) - set(grouplist))
        for each in check_num_cols:
            check_num = check_num.withColumn(each, check_num[each].cast(IntegerType()))
    
        # 平均值计算
        check_num = check_num.withColumn("row_max", func.greatest(*check_num_cols)) \
                        .withColumn("row_min", func.least(*check_num_cols))
        check_num = check_num.withColumn("mean_adj", 
                            mean_adj(func.col('row_max'), func.col('row_min'), *(func.col(x) for x in check_num_cols)))
        check_num = check_num.withColumn("mean_adj", func.when(func.col('mean_adj') == 0, func.lit(None)).otherwise(func.col('mean_adj')))
        # row_min 与 mean_adj 的差值
        check_num = check_num.withColumn("min_diff", min_diff(func.col('row_max'), func.col('row_min'), func.col('mean_adj')))
        check_num = check_num.withColumn("min_diff", func.when(func.col('mean_adj').isNull(), func.lit(None)).otherwise(func.col('min_diff')))
    
        # 匹配PHA    
        check_num = check_num.join(cpa_pha_mapping, on='ID', how='left')    
        # 排序
        check_num = check_num.orderBy(col('min_diff').desc(), col('mean_adj').desc())
        
        return check_num
    
    def CheckTopChange(check_have, Raw_data, grouplist):
        # 1级头部医院变化倍率检查
        check_top = Raw_data.join(check_have.where(col('row_min') == 1).select('id').distinct(), on='id', how='inner') \
                                .groupby(grouplist + ['date']).agg(func.sum('sales').alias('sales'))
    
        check_top_tmp = check_top.groupby(grouplist) \
                    .agg(func.max('sales').alias('max_sales'), 
                         func.min('sales').alias('min_sales'), 
                         func.sum('sales').alias('sum_sales'),
                         func.count('sales').alias('count'))
    
        check_top = check_top.join(check_top_tmp, on=grouplist, how='left') \
                                .withColumn('mean_sales', func.when(col('count') > 2, (col('sum_sales') - col('max_sales') - col('min_sales'))/(col('count') -2) ) \
                                                        .otherwise(col('sum_sales')/col('count') )) \
                                .withColumn('mean_times', func.round(col('sales')/col('mean_sales'),1 )) \
                                .groupBy(grouplist).pivot('date').agg(func.sum('mean_times'))
    
        check_top = check_top.withColumn("max", func.greatest(*list(set(check_top.columns) - set(grouplist))))
        
        return check_top
    #========== check_12 金额==========
    if g_id_molecule == 'True':
        check_12_1 = CheckTopChange(check_have, Raw_data, grouplist=['id', 'molecule'])
    else:
        return {}
    

    # %%
    # =========== 数据输出 =============
    def lowerColumns(df):
        df = df.toDF(*[i.lower() for i in df.columns])
        return df
    
    check_12_1 = lowerColumns(check_12_1)
    
    logger.debug('数据执行-Finish')
    
    return {'out_df':check_12_1}