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
    g_database_result = kwargs['g_database_result']
    g_out_table = kwargs['g_out_table']
    filetype = kwargs['filetype']
    g_path = kwargs['g_path']
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
    l_paths = g_path.replace(' ','').split(',')
    # %%
    # =========== 函数定义：输出结果 =============
    
    def createPartition(path):
        df = spark.read.parquet(path)
        df = df.withColumn('filetype', func.lit(filetype))
        recordCount = df.count()
        l_dict_scheme = getScheme(df)
            
        Location = path
        run_id = path.split('/version=')[1].split('/')[0]
        provider = path.split('/provider=')[1].split('/')[0]
        #owner = path.split('/owner=')[1].split('/')[0]
        
        pdf=df.select('DATE').distinct().toPandas()
            
        for indexs in pdf.index:
            data = pdf.loc[indexs]
            i_data = data['DATE']
            #i_doi = data['DOI']      
            
            # 创建分区
            logger.debug('创建分区')    
            partition_input_list = [{
             "Values": [run_id, provider, filetype, str(i_data)], 
            "StorageDescriptor": 
                {
                "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"}, 
                "Location": Location, 
                "Columns": l_dict_scheme,
                "Parameters": {"compressionType": "none", 
                            "classification": "parquet", 
                            "typeOfData": "file"}
                } 
            }]    
            client = boto3.client('glue')
            glue_info = client.batch_create_partition(DatabaseName=g_database_result, TableName=g_out_table, PartitionInputList=partition_input_list)
            logger.debug(glue_info)
        
    def getScheme(df):
        file_scheme = df.dtypes
        l_dict_scheme = []
        for i in file_scheme:
            dict_scheme = {}
            dict_scheme["Name"] = i[0]
            dict_scheme["Type"] = i[1]
            l_dict_scheme.append(dict_scheme)
        return l_dict_scheme
            

    # %%
    for path in l_paths:
        logger.debug(path)
        createPartition(path)
