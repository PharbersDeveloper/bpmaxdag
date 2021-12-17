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
    # g_input_version = kwargs['g_input_version']
    # project_name = kwargs['project_name']
    # if_others = kwargs['if_others']
    # out_path = kwargs['out_path']
    # run_id = kwargs['run_id']
    # owner = kwargs['owner']
    
    # g_database_temp = kwargs['g_database_temp']
    # g_database_input = kwargs['g_database_input']
    ### input args ###
    
    ### output args ###
    # g_out_table = kwargs['g_out_table']
    ### output args ###
    
    from pyspark.sql import SparkSession, Window
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    import os
    import boto3
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col    
    import json    # %% 
    # 输入参数设置
    # dict_input_version = json.loads(g_input_version)
    # logger.debug(dict_input_version)
    # if if_others != "False" and if_others != "True":
    #     logger.error('wrong input: if_others, False or True') 
    #     raise ValueError('wrong input: if_others, False or True')
        
    # if if_others == "True":
    #     g_raw_data_type = "data_box"
    # else:
    #     g_raw_data_type = "data"
    
    # 输出
    # if if_others == "True":
    #     out_dir = out_dir + "/others_box/"
    # p_out_path = out_path + g_out_table
    # %% 
    # 输入数据读取
    df_raw_data = kwargs['product_mapping_out']
    # print(df_raw_data)
    # print(df_raw_data.count())
    
    df_universe = kwargs['df_universe_base']
    # print(df_universe)
    # print(df_universe.count())
    
    df_cpa_pha_mapping = kwargs['df_cpa_pha_mapping']
    # print(df_cpa_pha_mapping)
    # print(df_cpa_pha_mapping.count())

    # %% 
    # =========== 数据清洗 =============
    logger.debug('数据清洗-start')
    # 函数定义
    def getTrueCol(df, l_colnames, l_df_columns):
        # 检索出正确列名
        l_true_colname = []
        for i in l_colnames:
            if i.lower() in l_df_columns and df.where(col(i) != 'None').count() > 0:
                l_true_colname.append(i)
        if len(l_true_colname) > 1:
           raise ValueError('有重复列名: %s' %(l_true_colname))
        if len(l_true_colname) == 0:
           raise ValueError('缺少列信息: %s' %(l_colnames)) 
        return l_true_colname[0]  
    
    def getTrueColRenamed(df, dict_cols, l_df_columns):
        # 对列名重命名
        for i in dict_cols.keys():
            true_colname = getTrueCol(df, dict_cols[i], l_df_columns)
            logger.debug(true_colname)
            if true_colname != i:
                if i in l_df_columns:
                    # 删除原表中已有的重复列名
                    df = df.drop(i)
                df = df.withColumnRenamed(true_colname, i)
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
    
    # 1、列名清洗
    # 待清洗列名
    dict_cols_universe = {"City_Tier_2010":["City_Tier", "CITYGROUP", "City_Tier_2010"], "PHA":["Panel_ID", "PHA"]}
    
    #  执行
    df_universe = getTrueColRenamed(df_universe, dict_cols_universe, df_universe.columns)
    
    # 2、选择标准列
    # 标准列名
    std_cols_cpa_pha_mapping = ['ID', 'PHA', '推荐版本']
    std_cols_raw_data = ['Date', 'ID', 'Raw_Hosp_Name', 'Brand', 'Form', 'Specifications', 'Pack_Number', 'Manufacturer', 'Molecule', 'Source', 'Corp', 
                     'Route', 'ORG_Measure', 'Sales', 'Units', 'Units_Box', 'Path', 'Sheet']
    std_cols_universe = ["PHA", "City", "City_Tier_2010", "Province"]
    
    #  执行
    df_universe = df_universe.select(std_cols_universe)
    df_raw_data = df_raw_data.select(std_cols_raw_data)
    df_cpa_pha_mapping = df_cpa_pha_mapping.select(std_cols_cpa_pha_mapping)
    
    # 3、数据类型处理
    dict_scheme_raw_data = {"Date":"int", "Sales":"double", "Units":"double", "Units_Box":"double", "Pack_Number":"int"}
        
    df_raw_data = dealScheme(df_raw_data, dict_scheme_raw_data)
    
    # 4、ID列补位
    df_cpa_pha_mapping = dealIDLength(df_cpa_pha_mapping)
    df_raw_data = dealIDLength(df_raw_data)
    
    # 5、其他处理
    if df_raw_data.where(col('Pack_Number') != 'None').count() == 0:
        df_raw_data = df_raw_data.withColumn("Pack_Number", func.lit(0))

    # %%
    # =========== 数据执行 =============
    logger.debug('数据执行-start')
    
    # 1.2 读取CPA与PHA的匹配关系:
    df_cpa_pha_mapping = df_cpa_pha_mapping.where(col("推荐版本") == 1) \
                                        .select("ID", "PHA").distinct()
    # 1.3 读取原始样本数据:    
    df_raw_data = df_raw_data.join(df_cpa_pha_mapping, on='ID', how="left") \
                        .join(df_universe.select("PHA", "City", "Province").distinct(), on='PHA', how='left') \
                        .join(df_universe.select("PHA", "City", "City_Tier_2010").distinct(), on=["PHA", "City"], how="left")
    
    df_raw_data = df_raw_data.withColumn("Month", (col('Date') % 100).cast(IntegerType())) \
                            .withColumn("Year", ((col('Date') - col('Month')) / 100).cast(IntegerType()))
    

    # %%
    # =========== 数据输出 =============
    def lowerColumns(df):
        df = df.toDF(*[i.lower() for i in df.columns])
        return df

    df_raw_data = lowerColumns(df_raw_data)

    logger.debug('数据执行-Finish')
    
    return {'out_df':df_raw_data}

