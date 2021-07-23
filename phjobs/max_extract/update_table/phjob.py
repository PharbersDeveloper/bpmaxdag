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
    import time    
    # %%
    l_update_month = g_update.replace(' ','').split(',')
    
    g_version = g_path.split('/version=')[1].split('/')[0]
    g_provider = g_path.split('/provider=')[1].split('/')[0]
    if g_path.split('/max_result_standard')[1].split('/')[0] == '_brief':
        g_filetype = 'brief'
    else:
        g_filetype = 'all'
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
    # %%
    os.environ["AWS_DEFAULT_REGION"] = "cn-northwest-1"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "3/tbzPaW34MRvQzej4koJsVQpNMNaovUSSY1yn0J"
    os.environ["AWS_ACCESS_KEY_ID"] = "AKIAWPBDTVEANKEW2XNC"
    
    def deletePartition(version, date):
        client = boto3.client('glue')
        res = client.batch_delete_partition(DatabaseName=g_database, TableName=g_table,
                                        PartitionsToDelete=[{"Values": [version, g_provider, g_filetype, str(date)]}])
        logger.debug(res)
        
    def createPartition(version, date):       
        # 创建分区
        logger.debug('创建分区')    
        partition_input_list = [{
         "Values": [version, g_provider, g_filetype, str(date)], 
        "StorageDescriptor": 
            {
            "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"}, 
            "Location": g_path, 
            "Columns": l_dict_scheme,
            "Parameters": {"compressionType": "none", 
                        "classification": "parquet", 
                        "typeOfData": "file"}
            } 
        }]
        client = boto3.client('glue')
        glue_info = client.batch_create_partition(DatabaseName=g_database, TableName=g_table, PartitionInputList=partition_input_list)
        logger.debug(glue_info)
    # %%
    for date in l_update_month:
        try:
            version_old = spark.sql("SELECT DISTINCT version FROM %s.%s WHERE provider='%s' AND filetype='%s' AND date='%s'" 
                                     %(g_database, g_table, g_provider, g_filetype, str(date))).toPandas()['version'][0]
            print(date, ':', version_old)
            deletePartition(version_old, date)
        except:
            continue     
        createPartition(g_version, date)

