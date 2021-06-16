"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job

Job流程
weight_by_company (处理每个项目或公司的的Weight存入 Balance Mapping表中)
Label参数标识文件里的内容类型
1、WEIGHT_DEFAULT
2、WEIGHT_OTHER
"""

import random
import string
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from pyspark.sql.functions import lit, udf, col
from functools import reduce
from pyspark.sql.types import StringType


def execute(**kwargs):
    logger = phs3logger(kwargs["job_id"], LOG_DEBUG_LEVEL)
    spark = kwargs["spark"]()
    result_path_prefix = kwargs["result_path_prefix"]
    depends_path = kwargs["depends_path"]
    
    _weight_input = kwargs["weight_input"]
    _version = kwargs["version"]
    _company = kwargs["company"]
    _label = kwargs["label"]
    _weight_output = kwargs["weight_output"]
    
    _tags = ["DOI", "WEIGHT", "PHA"]
    
    def general_id():
        charset = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ' + \
                  'abcdefghijklmnopqrstuvwxyz' + \
                  '0123456789-_'

        charsetLength = len(charset)

        keyLength = 3 * 5

        result = ["MP"]
        for _ in range(keyLength):
            result.append(charset[random.randint(0, charsetLength - 1)])

        return "".join(result)


    gid = udf(general_id, StringType())
    
    df = spark.read.parquet(_weight_input) \
        .selectExpr("Province AS PROVINCE", "City AS CITY", "DOI", "Weight AS WEIGHT", "PHA") \
        .withColumn("WEIGHT", col("WEIGHT").cast(StringType()))
        
    df.persist()
    
    
    def transform_weigth_df(tag):
        return df.selectExpr("PROVINCE", "CITY", tag + " AS VALUE") \
            .withColumn("ID", gid()) \
            .withColumn("CATEGORY", lit(_label)) \
            .withColumn("TAG", lit(tag)) \
            .withColumn("VERSION", lit(_version)) \
            .withColumn("COMPANY", lit(_company)) \
            .selectExpr("ID", "PROVINCE", "CITY", "CATEGORY", "TAG", "VALUE", "VERSION",  "COMPANY")
    
    
    
    if df.count() == 0:
        row = [{"PROVINCE": "NAN", "CITY": "NAN", "DOI": "NAN", "WEIGHT": "NAN", "PHA": "NAN"}]
        cf = spark.createDataFrame(row).selectExpr("PROVINCE", "CITY", "DOI", "WEIGHT", "PHA")
        df = df.union(cf)
        
    
    union_df = reduce(lambda dfl, dfr: dfl.union(dfr), list(map(transform_weigth_df, _tags)))
    union_df.write \
        .partitionBy("COMPANY", "VERSION") \
        .parquet(_weight_output, "append")
        
    return {}
