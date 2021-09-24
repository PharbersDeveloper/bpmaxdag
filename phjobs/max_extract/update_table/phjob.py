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
    g_path = kwargs['g_path']
    g_info = kwargs['g_info']
    g_update = kwargs['g_update']
    g_database = kwargs['g_database']
    g_table = kwargs['g_table']
    ### input args ###
    
    ### output args ###
    c = kwargs['c']
    d = kwargs['d']
    ### output args ###

    from pyspark.sql import SparkSession, Window
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    import os
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col
    import json
    import boto3  
    import time        # %%
    dict_info = json.loads(g_info)
    g_version = dict_info['version']
    g_provider = dict_info['provider']
    g_filetype = dict_info['filetype']
    # %%
    def getScheme(df):
        file_scheme = df.dtypes
        l_dict_scheme = []
        for i in file_scheme:
            dict_scheme = {}
            dict_scheme["Name"] = i[0]
            dict_scheme["Type"] = i[1]
            l_dict_scheme.append(dict_scheme)
        return l_dict_scheme
    
    df = spark.read.parquet(g_path)
    l_dict_scheme = getScheme(df)
    
    if g_update == 'all':
        l_update_month = df.select('date').distinct().toPandas()['date'].values.tolist()
    else:
        l_update_month = g_update.replace(' ','').split(',')
    # %%
    os.environ["AWS_DEFAULT_REGION"] = "cn-northwest-1"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "3/tbzPaW34MRvQzej4koJsVQpNMNaovUSSY1yn0J"
    os.environ["AWS_ACCESS_KEY_ID"] = "AKIAWPBDTVEANKEW2XNC"
    
    client = boto3.client('glue', region_name='cn-northwest-1')
    
    def deletePartition(version, date):
        client = boto3.client('glue')
        res = client.batch_delete_partition(DatabaseName=g_database, TableName=g_table,
                                        PartitionsToDelete=[{"Values": [version, g_provider, g_filetype, str(date)]}])
        logger.debug(res)
    
            
    def PartitionInput(version, provider, filetype, date, g_path, l_dict_scheme):
        # 分区信息
        partition_input = {
        "Values": [version, provider, filetype, str(date)],
        "StorageDescriptor": {
            "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"},
            "Location": g_path + '/DATE=' + str(date),
            "Columns": l_dict_scheme,
            "Parameters": {"compressionType": "none", "classification": "parquet", "typeOfData": "file"}
        }
            } 
        return partition_input
    
    def CreatePartition(g_database, g_table, partition_input):       
        # 创建分区
        logger.debug('创建分区')    
        partition_input_list = [partition_input]
        glue_info = client.batch_create_partition(DatabaseName=g_database, TableName=g_table, PartitionInputList=partition_input_list)
        #logger.debug(glue_info)
    
    def UpdatePartition(g_database, g_table, partition_input, old_partition_value):
        # 更新分区
        logger.debug('更新分区')
        # 对哪个分区进行更新（已有分区信息）:PartitionValueList， 更新后的分区信息 :PartitionInput
        glue_info = client.update_partition(DatabaseName=g_database, TableName=g_table, PartitionInput=partition_input, PartitionValueList=old_partition_value)
        #logger.debug(glue_info)    
    
        
    def GetTableScheme(database, table_name):
        # 获取 table 信息
        table_info = client.get_table(DatabaseName=database, Name=table_name)['Table']
        table_scheme = table_info['StorageDescriptor']['Columns']
        PartitionKeys = table_info['PartitionKeys']
        # [{'Name': 'province', 'Type': 'string'}, {'Name': 'city', 'Type': 'string'}]
        return table_info
        
    def UpdateTable(database, table_name, table_info):
        # 更新 table
        response = client.update_table(
            DatabaseName=database,
            TableInput={
                'Name': table_name,
                'StorageDescriptor' : table_info['StorageDescriptor'],      
                'PartitionKeys': table_info['PartitionKeys'],
                "TableType": "EXTERNAL_TABLE"
            },
        )
        #logger.debug(response) 
        
    def GetPartitions(g_database, g_table, g_provider, g_filetype, date):
        # 不能返回所有？ 最多 500？
        all_partitions = [i['Values'] for i in client.get_partitions(DatabaseName=g_database,TableName=g_table)['Partitions']]
        find_partitions = [i for i in all_partitions if i[1] == g_provider and i[2] == g_filetype and i[3] == str(date)]
        return find_partitions
    # %%
    for date in l_update_month:
        partition_input = PartitionInput(g_version, g_provider, g_filetype, date, g_path, l_dict_scheme)
        # 待更新的 月份 数据 是否已经存在分区，如果存在则更新分区，如果不存在则创建分区
        version_old = spark.sql("SELECT DISTINCT version FROM %s.%s WHERE provider='%s' AND filetype='%s' AND date='%s'" 
                                     %(g_database, g_table, g_provider, g_filetype, str(date))).toPandas()['version'].values.tolist()    
        if len(version_old) > 0:
            if len(version_old) > 1:
                raise ValueError(f'{g_provider} {date} 有重复数据：{version_old}')
            else:
                old_partition_value = [version_old[0], g_provider, g_filetype, str(date)]
                UpdatePartition(g_database, g_table, partition_input, old_partition_value)
                logger.debug(f"{g_provider} {date} 更新分区：{version_old[0]} 更新为 {g_version}")
        else:
            CreatePartition(g_database, g_table, partition_input)
            logger.debug(f"{g_provider} {date} 创建分区：{g_version}")
            
    # 更新table
    logger.debug('更新table')
    table_info = GetTableScheme(g_database, g_table)    
    UpdateTable(g_database, g_table, table_info)
