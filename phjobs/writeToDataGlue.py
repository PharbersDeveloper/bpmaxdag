"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL


def execute(**kwargs):
    logger = phs3logger(kwargs["run_id"], LOG_DEBUG_LEVEL)
    spark = kwargs['spark']
    

    data_frame = kwargs['df_input']
    dataType = kwargs['dataType']
    tempArgs = kwargs['tempArgs']
    s3Args = kwargs['s3Args']
    toTable = kwargs['toTable']
    toVersion = kwargs['toVersion']
    toProvider = kwargs['toProvider']
    
    # 输入参数
    '''
    {
    "dataType": "temp/s3",
    "tempArgs": {
        "projectName": "autorawdata",
        "table": "union_raw_data",
        "version": "autorawdata_autorawdata_developer_2022-07-25T09:40:23+00:00_李宇轩"
    },
    "s3Args": {
        "s3path": "",
        "filetype": "csv/parquet",
        "csv_encoding": "空/utf-8/GBK"
    },
    "toTable": "max_raw_data_delivery",
    "toVersion": "京新_test",
    "toProvider": "京新"
    }
    '''
   
    
    import os
    import pandas as pd
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func 
    import json
    import boto3
    from pyspark.sql.functions import lit, col, struct, to_json, json_tuple
    from functools import reduce
    import time
    import re
    
    
    def convert_union_schema(df):
        # 对多TraceId取Schema的col的并集
        rows = df.select("schema").distinct().collect()
        return list(reduce(lambda pre, next: set(pre).union(set(next)), list(map(lambda row: [schema["name"] for schema in json.loads(row["schema"])], rows))  ))

    def convert_normal_df(df, cols):
         # 将统一Schema的DF转成正常的DataFrame
        return df.select(json_tuple(col("data"), *cols)) \
        .toDF(*cols)

    def dealCol(df, version, provider):
        # 列名统一小写，类型统一字符型，点替换为-
        df = df.select(*[col(i).astype("string") for i in df.columns])
        reg = "[\r\n\n（） ， +()-./\"]"
        df = df.toDF(*(re.sub(reg, '_', c).lower() for c in df.columns))
        df = df.withColumn('version', func.lit(version)) \
                .withColumn('provider', func.lit(provider))
        return df

    def outFile(df, outpath):
        # 写出到s3
        df.write.format("parquet") \
                     .mode("append").partitionBy("version") \
                     .parquet(outpath)
        print(f"write to {outpath}")

    def getTempData(projectName, table, version):
        # 读取中间文件数据
        projectId={"autorawdata":"99a5R2kIMyInYEc", "automax":"s7nBDbpqfUShq1w", "autoweight":"xu68bxmMFJo6-o9", "autorffactor2":"2LWyqFPIIwCSZEV", "cmax":"ZyQpzttbwmvQcCf"}
        projectPath=f"s3://ph-platform/2020-11-11/lake/pharbers/{projectId[projectName]}/{table}"
        print(projectPath)
        df = spark.read.parquet(projectPath) \
                    .where(col('traceId') == version)
        dfout = convert_normal_df(df, convert_union_schema(df))
        if dfout.count() == 0:
            raise ValueError("数据为空")
        return dfout

    def getS3pData(s3path, filetype, encoding):
        if filetype == 'parquet':
            df = spark.read.parquet(s3path)
        elif filetype == 'csv':
            df = spark.read.csv(s3path, header=True, encoding=encoding)
        return df

    def getClient():
        client = boto3.client('glue', 'cn-northwest-1')
        return client

    def judgeVersionToGlue(table, version):
        # 判断写入数据目录的version是否已经存在
        #client = getClient()
        #outPartitions = client.get_partitions(DatabaseName="zudIcG_17yj8CEUoCTHg", TableName=table )
        #outPartitionsList = [i['Values'][0] for i in outPartitions['Partitions']]
        outPartitionsList = [i['version'] for i in spark.read.parquet(getOutS3Path(table)).select('version').distinct().collect()]

        if version in outPartitionsList:
            print(outPartitionsList)
            raise ValueError(f"已经存在该version:{version}")
        return outPartitionsList

    def judgeColumns(cols, table):
        client = getClient()
        columns_response = client.get_table(DatabaseName='zudIcG_17yj8CEUoCTHg', Name=table)['Table']['StorageDescriptor']['Columns']
        glueColumns = [i['Name'] for i in columns_response] + ['version']
        not_in_glue_cols = [i for i in cols if i not in glueColumns]
        if len(not_in_glue_cols) > 0:
            raise ValueError(f"列名不在{table}中:{not_in_glue_cols}")


    def getOutS3Path(table):
        projectId="zudIcG_17yj8CEUoCTHg"
        projectPath=f"s3://ph-platform/2020-11-11/lake/pharbers/{projectId}/{table}"
        print(projectPath)
        return projectPath


    def writeToDataGlue(df, gluetable, glueversion, glueprovider):
        # 写出到3s 数据目录位置
        projectPath = getOutS3Path(gluetable)
        # 判断version是否已存在
        judgeVersionToGlue(gluetable, glueversion)
        # 判断列名是否存在
        judgeColumns(df.columns, gluetable)
        # 写出
        outFile(df, outpath=projectPath)

    def runCrawler(crawlerName):
        client = getClient()
        response = client.start_crawler(
            Name = crawlerName
        )
        print(response)

    def tableUse(toTable):
        gluetables = ['max_raw_data', 'prod_mapping', 'mkt_mapping', 'cn_ims_sales_fdata', 'cpa_pha_mapping', 'id_bedsize', 'province_city_mapping', 'universe_base', 'universe_other', 'universe_outlier', 'factor', 'max_result_backfill', 'max_raw_data', 'prod_mapping', 'cn_ims_sales_fdata', 'cn_geog_dimn', 'ims_info_upload', 'ims_mapping', 'cpa_pha_mapping', 'mkt_mapping', 'universe_base', 'universe_other', 'doctor', 'bt_pha', 'ind', 'prod_mapping', 'max_raw_data_delivery', 'max_raw_data_std', 'max_raw_data', 'universe_base_common', 'universe_base', 'weight_default', 'weight', 'factor', 'universe_outlier', 'province_city_mapping_common', 'province_city_mapping', 'cpa_pha_mapping_common', 'cpa_pha_mapping', 'id_bedsize', 'product_map_all_atc', 'master_data_map', 'mkt_mapping', 'poi', 'not_arrived', 'published', 'max_raw_data_upload', 'molecule_adjust', 'cpa_pha_mapping', 'cpa_pha_mapping_common', 'max_raw_data_delivery', 'max_raw_data_std', 'max_raw_data', 'prod_mapping', 'pchc_universe', 'cn_prod_ref', 'ims_chpa', 'cn_mol_lkp', 'cn_mol_ref', 'cn_corp_ref', 'market_define', 'tianjin_packid_moleinfo', 'shanghai_packid_moleinfo', 'pchc_city_tier', 'cmax_raw_data']
        if toTable not in gluetables:
            raise ValueError("toTable不在数据目录中") 


    def addToDynamodb(table_name, version_name):
        item = {
            'id': {'S': "zudicg_17yj8ceuocthg" + '_' + table_name},
            'name': {'S': str(version_name)},
            'datasetId': {'S': str(table_name)},
            'date': {'S': str(time.time())},
            'owner': {'S': "pharbers"},
            'projectId': {'S': "zudicg_17yj8ceuocthg"}
        }
        client_dynamodb = boto3.client('dynamodb', 'cn-northwest-1')
        respose = client_dynamodb.put_item(
                TableName="version",
                Item=item)
        return respose

                   
    # ======== 执行 ======
    # 判断输出table是否在数据目录中
    tableUse(toTable)
    
    # 读取 临时目录 数据
    if dataType == 'temp':
        dfout = getTempData(tempArgs['projectName'], tempArgs['table'], tempArgs['version'])
    elif dataType == 's3':
        dfout = getS3pData(s3Args['s3path'], s3Args['filetype'], s3Args['csv_encoding'])
        
    # 列处理
    dfout = dealCol(dfout, toVersion, toProvider)

    dfout.show(2)
    # 写出到数据目录
    writeToDataGlue(dfout, toTable, toVersion, toProvider)
    # 爬取到glue
    try:
        runCrawler('ph_etl_for_max')
    except:
        print("Crawler 进行中")
    # 写入到dynamodb的version表
    time.sleep(90)
    addToDynamodb(toTable, toVersion)

    return {"out_df": dfout}
