"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL


def execute(**kwargs):
    logger = phs3logger(kwargs["run_id"], LOG_DEBUG_LEVEL)
    spark = kwargs['spark']
    

    data_frame = kwargs['df_kong']
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
    
    
    def convert_union_schema(df):
        # 对多TraceId取Schema的col的并集
        rows = df.select("schema").distinct().collect()
        return list(reduce(lambda pre, next: set(pre).union(set(next)), list(map(lambda row: [schema["name"] for schema in json.loads(row["schema"])], rows))  ))
    
    def convert_normal_df(df, cols):
         # 将统一Schema的DF转成正常的DataFrame
        return df.select(json_tuple(col("data"), *cols)) \
        .toDF(*cols)
    
    def lowCol(df):
        # 列名统一小写，类型统一字符型
        df = df.toDF(*[c.lower() for c in df.columns])
        df = df.select(*[col(i).astype("string") for i in df.columns])
        return df
    
    def outFile(df, version, provider, outpath):
        # 写出到s3
        df = lowCol(df)
        df = df.withColumn('version', func.lit(version)) \
                .withColumn('provider', func.lit(provider))
        df.write.format("parquet") \
                     .mode("append").partitionBy("version") \
                     .parquet(outpath)
    
    def getTempData(projectName, table, version):
        # 读取中间文件数据
        projectId={"autorawdata":"99a5R2kIMyInYEc", "automax":"s7nBDbpqfUShq1w"}
        projectPath=f"s3://ph-platform/2020-11-11/lake/pharbers/{projectId[projectName]}/{table}"
        print(projectPath)
        df = spark.read.parquet(projectPath) \
                    .where(col('traceId') == version)
        dfout = convert_normal_df(df, convert_union_schema(df))
        if dfout.count() < 0:
            raise ValueError("数据为空")
        return dfout
    
    def getS3pData(s3path, filetype, encoding):
        if filetype == 'parquet':
            df = spark.read.parquet(projectPath)
        elif filetype == 'csv':
            df = spark.read.csv(projectPath, header=True, encoding=encoding)
    
    def getClient():
        os.environ["AWS_DEFAULT_REGION"] = "cn-northwest-1"
        client = boto3.client('glue')
        return client
    
    def judgeVersionToGlue(projectId, table, version):
        # 判断写入数据目录的version是否已经存在
        client = getClient()
        outPartitions = client.get_partitions(DatabaseName="zudIcG_17yj8CEUoCTHg", TableName=table )
        outPartitionsList = [i['Values'][0] for i in outPartitions['Partitions']]
        if version in outPartitionsList:
            raise ValueError("已经存在该version")
        return outPartitionsList
    
    def writeToDataGlue(df, gluetable, glueversion, glueprovider):
        # 写出到3s 数据目录位置
        projectId="zudIcG_17yj8CEUoCTHg"
        projectPath=f"s3://ph-platform/2020-11-11/lake/pharbers/{projectId}/{gluetable}"
        print(projectPath)
        judgeVersionToGlue(projectId,gluetable,glueversion)
        outFile(df, glueversion, glueprovider, outpath=projectPath)
    
    def runCrawler(crawlerName):
        client = getClient()
        response = client.start_crawler(
            Name = crawlerName
        )
        print(response)
            
    # ======== 执行 ======
    # 读取 临时目录 数据
    if dataType == 'temp':
        dfout = getTempData(tempArgs['projectName'], tempArgs['table'], tempArgs['version'])
    elif dataType == 's3':
        dfout = getS3pData(s3Args['s3path'], s3Args['filetype'], s3Args['csv_encoding'])

    dfout.show(2)
    # 写出到数据目录
    writeToDataGlue(dfout, toTable, toVersion, toProvider)
    # 爬取到glue
    runCrawler('ph_etl_for_max')


    return {"out_df": data_frame}
