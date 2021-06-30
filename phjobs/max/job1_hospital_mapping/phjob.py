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
    if_others = kwargs['if_others']
    out_path = kwargs['out_path']
    run_id = kwargs['run_id']
    owner = kwargs['owner']
    g_input_version = kwargs['g_input_version']
    g_database_temp = kwargs['g_database_temp']
    g_database_input = kwargs['g_database_input']
    ### input args ###
    
    ### output args ###
    g_out_table = kwargs['g_out_table']
    ### output args ###

    
    
    
    
    
    
    from pyspark.sql import SparkSession, Window
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    import os
    import boto3
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col    
    import json    # %% 
    # 输入参数设置
    dict_input_version = json.loads(g_input_version)
    logger.debug(dict_input_version)
    if if_others != "False" and if_others != "True":
        logger.error('wrong input: if_others, False or True') 
        raise ValueError('wrong input: if_others, False or True')
        
    if if_others == "True":
        g_raw_data_type = "data_box"
    else:
        g_raw_data_type = "data"
    
    # 输出
    # if if_others == "True":
    #     out_dir = out_dir + "/others_box/"
    p_out_path = out_path + g_out_table
    # %% 
    # 输入数据读取
    df_raw_data = spark.sql("SELECT * FROM %s.raw_data WHERE provider='%s' AND filetype='%s' AND version='%s'" 
                         %(g_database_input, project_name, g_raw_data_type, dict_input_version['raw_data'][g_raw_data_type]))
    # print(df_raw_data)
    # print(df_raw_data.count())
    
    df_universe =  spark.sql("SELECT * FROM %s.universe_base WHERE provider='%s' AND version='%s'" 
                             %(g_database_input, project_name, dict_input_version['universe_base']))
    # print(df_universe)
    # print(df_universe.count())
    
    df_cpa_pha_mapping =  spark.sql("SELECT * FROM %s.cpa_pha_mapping WHERE provider='%s' AND version='%s'" 
                             %(g_database_input, project_name, dict_input_version['cpa_pha_mapping']))
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
            if i.lower() in l_df_columns and df.where(~col(i).isNull()).count() > 0:
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
    def deal_ID_length(df):
        # ID不足7位的补足0到6位
        # 国药诚信医院编码长度是7位数字，cpa医院编码是6位数字，其他还有包含字母的ID
        df = df.withColumn("ID", df["ID"].cast(StringType()))
        # 去掉末尾的.0
        df = df.withColumn("ID", func.regexp_replace("ID", "\\.0", ""))
        df = df.withColumn("ID", func.when(func.length(df.ID) < 7, func.lpad(df.ID, 6, "0")).otherwise(df.ID))
        return df
    
    # 5、其他处理
    if df_raw_data.where(~col('Pack_Number').isNull()).count() == 0:
        df_raw_data = df_raw_data.withColumn("Pack_Number", func.lit(0))

    # %%
    # =========== 数据执行 =============
    logger.debug('数据执行-start')
    
    # 1.2 读取CPA与PHA的匹配关系:
    df_cpa_pha_mapping = deal_ID_length(df_cpa_pha_mapping)
    df_cpa_pha_mapping = df_cpa_pha_mapping.where(col("推荐版本") == 1) \
                                        .select("ID", "PHA").distinct()
    # 1.3 读取原始样本数据:    
    df_raw_data = deal_ID_length(df_raw_data)
    df_raw_data = df_raw_data.join(df_cpa_pha_mapping, on='ID', how="left") \
                        .join(df_universe.select("PHA", "City", "Province").distinct(), on='PHA', how='left') \
                        .join(df_universe.select("PHA", "City", "City_Tier_2010").distinct(), on=["PHA", "City"], how="left")
    
    df_raw_data = df_raw_data.withColumn("Month", (col('Date') % 100).cast(IntegerType())) \
                            .withColumn("Year", ((col('Date') - col('Month')) / 100).cast(IntegerType()))

    # %%
    # =========== 数据输出 =============    
    def createPartition(p_out):
        # 创建分区
        logger.debug('创建分区')
        Location = p_out + '/version=' + run_id + '/provider=' + project_name + '/owner=' + owner
        g_out_table = p_out.split('/')[-1]
        
        partition_input_list = [{
         "Values": [run_id, project_name,  owner], 
        "StorageDescriptor": {
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
            }, 
            "Location": Location, 
        } 
            }]    
        client = boto3.client('glue')
        glue_info = client.batch_create_partition(DatabaseName=g_database_temp, TableName=g_out_table, PartitionInputList=partition_input_list)
        logger.debug(glue_info)
        
    def outResult(df, p_out):
        df = df.withColumn('version', func.lit(run_id)) \
                .withColumn('provider', func.lit(project_name)) \
                .withColumn('owner', func.lit(owner))
        df.repartition(1).write.format("parquet") \
                 .mode("append").partitionBy("version", "provider", "owner") \
                 .parquet(p_out)
        
    # 1、hospital_mapping_out
    outResult(df_raw_data, p_out_path)
    logger.debug("输出 hospital_mapping 结果：" + p_out_path)
    createPartition(p_out_path)
    
    logger.debug('数据执行-Finish')

