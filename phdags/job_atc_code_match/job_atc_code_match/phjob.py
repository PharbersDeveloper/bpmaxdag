# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from ph_logs.ph_logs import phs3logger
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import os
from pyspark.sql.functions import  when
from pyspark.sql.functions import col
import logging
from pyspark.sql.functions import lit
from pyspark.sql.functions import substring
from pyspark.sql.functions import sum
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.functions import first
from pyspark.sql import Window
from pyspark.sql.functions import rank, row_number
import math
'''
写入操作
'''
def write2postgres(df, pgTable):
    logging.info("write postgresql start")
    url = "jdbc:postgresql://ph-db-lambda.cngk1jeurmnv.rds.cn-northwest-1.amazonaws.com.cn/phreports"
    # spark.read.csv(s3Path, header=True) \
    df.write.format("jdbc") \
        .option("url", url) \
        .option("dbtable", pgTable) \
        .option("user", "pharbers") \
        .option("password", "Abcde196125") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()
    logging.info("write postgresql end")


@pandas_udf(LongType(), PandasUDFType.GROUPED_AGG)
def pudf_base_factor(c):
    
    def func_get_prime(n):
      return filter(lambda x: not [x%i for i in range(2, int(math.sqrt(x))+1) if x%i ==0], range(2,n+1))
  
  
    def get_divisors(number):
        """
        传入一个整数,返回其所有因子(列表)
        :param number: 整数
        :return: 该整数所有因子(列表)
        """
        prime = func_get_prime(number)
        list = []
        for i in prime:
            if number % i == 0:
                list.append(i)
        return list
 
  
    s = set([])
    for iter in c:
        
        st = set(get_divisors(iter))
        s = s | st
    
    # return list(s)
     
    r = 1
    for item in s:
        r = r * item
        
    return r

def execute(**kwargs):
    """
        please input your code below
        get spark session: spark = kwargs["spark"]()
    """
    logger = phs3logger(kwargs["job_id"])
    logger.info("当前 owner 为 " + str(kwargs["owner"]))
    logger.info("当前 run_id 为 " + str(kwargs["run_id"]))
    logger.info("当前 job_id 为 " + str(kwargs["job_id"]))
    spark = kwargs["spark"]()
   
    '''
    读取文件
    '''
    df_report = spark.read.parquet(kwargs['input_report'])
    df_csv = spark.read.csv(kwargs['input_csv'], header=True)
    
    '''
    数据去重
    '''
    df_report_distinct = df_report.select("ATC").distinct().withColumnRenamed('ATC','ACT5')
    df_csv_distinct = df_csv.select("ATC4_Code").distinct().withColumnRenamed('ATC4_Code','ACT5').withColumn('ACT3',substring('ACT5',1,3))
    
    '''
    创建视图
    '''
    df_report_distinct.createTempView("df_report")
    df_csv_distinct.createTempView("df_csv")
    
    '''
    执行sql逻辑查询
    '''
    result = spark.sql(" select df_csv.ACT3 , df_csv.ACT5 , case when df_report.ACT5 is null then 0 else 1 end as FLAG from df_csv left join  df_report on df_csv.ACT5 = df_report.ACT5 ")
    '''
    添加中文列名
    '''
    result = result.withColumn('ACT3CN', lit(None).cast(StringType())).withColumn('ACT5CN', lit(None).cast(StringType()))
    result=result.select("ACT3","ACT5","ACT3CN","ACT5CN","FLAG").orderBy("ACT3", "ACT5")
    result.show()
    
    df = spark.read.csv(kwargs['input_alfred'], header=True)
    df = df.groupBy("act3", "act5").agg(first(df.act3CN).alias("act3CN"), first(df.act5CN).alias("act5CN"), sum(df.flag).alias("flag"))
    df = df.withColumn("COUNT", lit(1)).withColumn("TMP", lit(1))


     # 1. 构建图表的整体长度
    df_act_3 = df.groupBy("act3").agg(sum(df.COUNT).alias("COUNT"))
    df_act_3_factor = df_act_3.select("COUNT").distinct().withColumn("TMP", lit(1))
    df_act_3_factor = df_act_3_factor.groupBy("TMP").agg(pudf_base_factor(df_act_3_factor.COUNT).alias("FACTOR"))
    df_act_3 = df_act_3.crossJoin(df_act_3_factor.drop("TMP"))
    df_act_3 = df_act_3.withColumn("STEP", df_act_3.FACTOR / df_act_3.COUNT)
    
    # 2. 返还图形构建result
    df = df.join(df_act_3, on="act3", how="left").drop("COUNT", "TMP")
    windowSpec = Window.partitionBy("act3").orderBy("act5", "flag")
    df = df.withColumn("RANK", rank().over(windowSpec))
    df = df.where(df.act3 != "Z95")
    df.show(truncate=False)
    write2postgres(result,kwargs['output_flag'])
    write2postgres(df,kwargs['output_actcode'])

    return {}
