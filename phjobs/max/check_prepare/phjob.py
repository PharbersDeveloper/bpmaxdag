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
    g_input_version = kwargs['g_input_version']
    project_name = kwargs['project_name']
    out_path = kwargs['out_path']
    run_id = kwargs['run_id']
    owner = kwargs['owner']
    g_database_temp = kwargs['g_database_temp']
    g_database_input = kwargs['g_database_input']
    ### input args ###
    
    ### output args ###
    g_out_ims = kwargs['g_out_ims']
    ### output args ###

    from pyspark.sql import SparkSession, Window
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    import os
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col
    import json
    import boto3    
    
    
    # %% 
    # 输入参数设置
    dict_input_version = json.loads(g_input_version)
    logger.debug(dict_input_version)
    
    # 输出
    p_out_ims = out_path + g_out_ims
    # %% 
    # 输入数据读取
    # 跟panel文件join
    df_hospital_map = spark.sql("SELECT * FROM %s.cpa_gyc_hospital_map WHERE provider='%s' AND version='%s'" 
                             %(g_database_input, 'common', dict_input_version['cpa_gyc_hospital_map']))
    
    df_ims_city_map = spark.sql("SELECT * FROM %s.ims_flat_files WHERE provider='%s' AND version='%s'" 
                             %(g_database_input, 'common', dict_input_version['ims_flat_files']['IMS_mapping_citycode']))
    
    df_ims_Sales = spark.sql("SELECT * FROM %s.ims_flat_files WHERE provider='%s' AND version='%s'" 
                             %(g_database_input, 'common', dict_input_version['ims_flat_files']['cn_IMS_Sales_Fdata']))
    
    df_ims_mol_lkp = spark.sql("SELECT * FROM %s.ims_flat_files WHERE provider='%s' AND version='%s'" 
                             %(g_database_input, 'common', dict_input_version['ims_flat_files']['cn_mol_lkp']))
    
    df_ims_mol_ref = spark.sql("SELECT * FROM %s.ims_flat_files WHERE provider='%s' AND version='%s'" 
                             %(g_database_input, 'common', dict_input_version['ims_flat_files']['cn_mol_ref']))
    
    #df_max_result_backfill = spark.sql("SELECT * FROM %s.max_result_backfill WHERE version='%s' AND provider='%s' AND  owner='%s'" 
    #                         %(g_database_temp, run_id, project_name, owner))

    # %% 
    # =========== 数据清洗 =============
    logger.debug('数据清洗-start')
    # 函数定义
    def dealIDLength(df, colname='ID'):
        # ID不足7位的前面补0到6位
        # 国药诚信医院编码长度是7位数字，cpa医院编码是6位数字
        # 其他来源的ID 还有包含字母的, 所以要为字符型，不能转为 数值型
        df = df.withColumn(colname, col(colname).cast(StringType()))
        # 去掉末尾的.0
        df = df.withColumn(colname, func.regexp_replace(colname, "\\.0", ""))
        df = df.withColumn(colname, func.when(func.length(col(colname)) < 7, func.lpad(col(colname), 6, "0")).otherwise(col(colname)))
        return df
    
    # 3、选择列
    df_hospital_map = df_hospital_map.select("医院编码", "医院名称", "等级" ,"标准化省份", "标准化城市").distinct() \
                                    .withColumnRenamed('医院编码', 'ID')
    df_ims_Sales = df_ims_Sales.select("Geography_id", "Channel_id", "Pack_ID", "Period_Code", "UN", "CU", "SU", "LC", "LCD").distinct() \
                                .withColumn('Period_Code', func.regexp_replace("Period_Code", "M", "")).distinct()
    df_ims_mol_lkp = df_ims_mol_lkp.select("Pack_ID", "Molecule_ID").distinct()
    df_ims_mol_ref = df_ims_mol_ref.select("Molecule_ID", "Molecule_Desc").distinct()
    df_ims_city_map = df_ims_city_map.select("IMS_CITY_CODE", "AUDIT_SHORT_DESC", "CITY_TYPE", "City_Tier", 
                                             "Province", "Province_S", "City", "City_S").distinct()
    
    # 5.ID列处理
    df_hospital_map = dealIDLength(df_hospital_map)
    # %% 
    # =========== 数据执行 =============
    # 匹配英文通用名
    # 合并复方分子：将 Pack_ID相同的 Molecule_Desc合并到一起
    df_ims_mol = df_ims_mol_lkp.join(df_ims_mol_ref, on='Molecule_ID', how='left')
    
    Source_window = Window.partitionBy("Pack_ID").orderBy(func.col('Molecule_Desc'))
    rank_window = Window.partitionBy("Pack_ID").orderBy(func.col('Molecule_Desc').desc())
    
    df_ims_mol = df_ims_mol.select("Pack_ID", "Molecule_Desc").distinct() \
                        .select("Pack_ID", func.collect_list(func.col('Molecule_Desc')).over(Source_window).alias('Molecule_Composition'), 
                                                        func.rank().over(rank_window).alias('rank')).persist()
    df_ims_mol = df_ims_mol.where(col('rank') == 1).drop('rank')
    df_ims_mol = df_ims_mol.withColumn('Molecule_Composition', func.concat_ws('_', func.col('Molecule_Composition'))) \
                        .orderBy('Pack_ID')
    
    # 获得 ims 大表
    df_ims_Sales_info = df_ims_Sales.join(df_ims_mol, on='Pack_ID', how='left') \
                                    .join(df_ims_city_map.select("AUDIT_SHORT_DESC", "Province", "City"), 
                                            df_ims_Sales['Geography_id']==df_ims_city_map['AUDIT_SHORT_DESC'], how='left')
    # %%
    # =========== 函数定义：输出结果 =============
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
        client = boto3.client('glue', region_name='cn-northwest-1')
        glue_info = client.batch_create_partition(DatabaseName=g_database_temp, TableName=g_out_table, PartitionInputList=partition_input_list)
        logger.debug(glue_info)
        
    def outResult(df, p_out):
        df = df.withColumn('version', func.lit(run_id)) \
                .withColumn('provider', func.lit(project_name)) \
                .withColumn('owner', func.lit(owner))
        df.repartition(1).write.format("parquet") \
                 .mode("append").partitionBy("version", "provider", "owner") \
                 .parquet(p_out)
    # %%
    # =========== 输出 =============
    outResult(df_ims_Sales_info, p_out_ims)
    createPartition(p_out_ims)
    logger.debug("输出 ims_Sales_info：" + p_out_ims)
    
    logger.debug('数据执行-Finish')
    

