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
    glueArgs = kwargs['glueArgs']
    out_path = kwargs['out_path']
    out_encoding = kwargs['out_encoding']
    out_filenum = kwargs['out_filenum']

    # 输入参数
    '''
    {
    "dataType": "temp/glue",
    "tempArgs": {
        "projectName": "autorawdata",
        "table": "union_raw_data",
        "version": ["autorawdata_autorawdata_developer_2022-07-25T09:40:23+00:00_李宇轩"]
    },
    "glueArgs": {
        "table": "mkt_mapping",
        "version": ["奥鸿_20210623"]
    },
    "out_path": "s3://ph-max-auto/v0.0.1-2020-06-08/data_download/union_raw_data.csv",
    "out_encoding":"utf-8/GBK",
    "out_filenum":"1"
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

    def getTempData(projectName, table, version):
        # 读取中间文件数据
        projectId={"autorawdata":"99a5R2kIMyInYEc", "automax":"s7nBDbpqfUShq1w", "autoweight":"xu68bxmMFJo6-o9", "autorffactor2":"2LWyqFPIIwCSZEV", "cmax":"ZyQpzttbwmvQcCf"}
        projectPath=f"s3://ph-platform/2020-11-11/lake/pharbers/{projectId[projectName]}/{table}"
        print(projectPath)
        df = spark.read.parquet(projectPath) \
                    .where(col('traceId') == version)
        dfout = convert_normal_df(df, convert_union_schema(df))
        if dfout.count() < 0:
            raise ValueError("数据为空")
        return dfout

    def getGlueData(table, version):
        projectId="zudIcG_17yj8CEUoCTHg"
        data_path=f"s3://ph-platform/2020-11-11/lake/pharbers/{projectId}/{table}"
        df = spark.read.parquet(data_path).where(col('version').isin(version))
        if df.count() < 0:
            raise ValueError("数据为空")
        return df

    def writeToS3(df, out_path, out_encoding, out_filenum, out_mode="append"):
        # 写出到3s 
        df.repartition(int(out_filenum)) \
            .write.format("csv").option("header", "true").option("encoding", str(out_encoding)) \
            .mode(out_mode).save(out_path)

    # ======== 执行 ======
    # 读取数据
    if dataType == 'temp':
        dfout = getTempData(tempArgs['projectName'], tempArgs['table'], tempArgs['version'])
    elif dataType == 'glue':
        dfout = getGlueData(glueArgs['table'], glueArgs['version'])
        
    dfout.show(2)
    # 写出到S3
    writeToS3(dfout, out_path, out_encoding, out_filenum)
    
    return {"out_df": data_frame}
