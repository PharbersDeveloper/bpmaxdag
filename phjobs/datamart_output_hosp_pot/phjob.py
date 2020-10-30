# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""
import pandas as pd
import boto3
import io
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, monotonically_increasing_id
import logging
import os


def standard_to_max_universe_format(df):
    df = df.drop('_ID', "STANDARD", "REVISION") \
        .withColumnRenamed('PHA_HOSP_NAME', 'PHA Hosp name') \
        .withColumnRenamed('PHA_ID', 'PHA ID') \
        .withColumnRenamed('HOSP_LEVEL', 'Hosp_level') \
        .withColumnRenamed('REGION', 'Region') \
        .withColumnRenamed('PROVINCE', 'Province') \
        .withColumnRenamed('CITY', 'Prefecture') \
        .withColumnRenamed('CITY_TIER', 'City Tier 2010') \
        .withColumnRenamed('SPECIALTY_CATE', 'Specialty_1') \
        .withColumnRenamed('SPECIALTY_ADMIN', 'Specialty_2') \
        .withColumnRenamed('RE_SPECIALTY', 'Re-Speialty') \
        .withColumnRenamed('SPECIALTY_DETAIL', 'Specialty 3') \
        .withColumnRenamed('DRUGINCOME_RMB', 'Est_DrugIncome_RMB') \
        .withColumnRenamed('DOCTORS_NUM', '医生数') \
        .withColumnRenamed('BED_NUM', '床位数') \
        .withColumnRenamed('GENERAL_BED_NUM', '全科床位数') \
        .withColumnRenamed('INTERNAL_BED_NUM', '内科床位数') \
        .withColumnRenamed('SURG_BED_NUM', '外科床位数') \
        .withColumnRenamed('OPHTH_BED_NUM', '眼科床位数') \
        .withColumnRenamed('ANNU_DIAG_TIME', '年诊疗人次') \
        .withColumnRenamed('OUTP_DIAG_TIME', '门诊诊次') \
        .withColumnRenamed('INTERNAL_DIAG_TIME', '内科诊次') \
        .withColumnRenamed('SURG_DIAG_TIME', '外科诊次') \
        .withColumnRenamed('ADMIS_TIME', '入院人数') \
        .withColumnRenamed('SURG_TIME', '住院病人手术人次数') \
        .withColumnRenamed('MED_INCOME', '医疗收入') \
        .withColumnRenamed('OUTP_INCOME', '门诊收入') \
        .withColumnRenamed('OUTP_TREAT_INCOME', '门诊治疗收入') \
        .withColumnRenamed('OUTP_SURG_INCOME', '门诊手术收入') \
        .withColumnRenamed('IN_HOSP_INCOME', '住院收入') \
        .withColumnRenamed('BED_INCOME', '住院床位收入') \
        .withColumnRenamed('IN_HOSP_TREAT_INCOME', '住院治疗收入') \
        .withColumnRenamed('IN_HOSP_SURG_INCOME', '住院手术收入') \
        .withColumnRenamed('DRUG_INCOME', '药品收入') \
        .withColumnRenamed('OUTP_DRUG_INCOME', '门诊药品收入') \
        .withColumnRenamed('OUTP_WST_DRUG_INCOME', '门诊西药收入') \
        .withColumnRenamed('IN_HOSP_DRUG_INCOME', '住院药品收入') \
        .withColumnRenamed('IN_HOSP_WST_DRUG_INCOME', '住院西药收入')
        
    return df
    
    
def execute(**kwargs):
    """
        please input your code below
    """
    logging.basicConfig(format='%(asctime)s %(filename)s %(funcName)s %(lineno)d %(message)s')
    logger = logging.getLogger('driver_logger')
    logger.setLevel(logging.INFO)
    logger.info("Origin kwargs = {}.".format(str(kwargs)))
    
    # input required
    input_path = kwargs.get('input_path', '')
    if input_path == 'not set' or input_path == '':
        raise Exception("Invalid input_path!", input_path)
        
    revision = kwargs.get('revision', '')
    if revision == 'not set' or revision == '':
        raise Exception("Invalid revision!", revision)
        
    # output required
    output_path = kwargs.get('output_path', '')
    if output_path == 'not set' or output_path == '':
        raise Exception("Invalid output_path!", output_path)
    
    os.environ["PYSPARK_PYTHON"] = "python3"
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("datamart dimension output max hosp job") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.instance", "1") \
        .config("spark.executor.memory", "1g") \
        .config('spark.sql.codegen.wholeStage', False) \
        .getOrCreate()

    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    if access_key is not None:
        spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
        spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
        spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
        spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
        spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.cn-northwest-1.amazonaws.com.cn")
    
    df = spark.read.parquet(input_path)
    df = df.filter((df.STANDARD == 'MAX') & (df.REVISION == revision))
    df = standard_to_max_universe_format(df)
    df.repartition(1).write.format("csv").option("header", "true").mode("overwrite").save(output_path)
    
   
    
    # # df = spark.read.parquet('s3a://ph-platform/2020-08-10/datamart/standard/dimensions/hosps/v20201001_20201029_1')
    # # df = df.withColumn("_ID", df.PHA_ID)
    # # df = df.repartition("STANDARD", "REVISION")
    # # df.write.format("parquet").mode('overwrite').partitionBy("STANDARD", "REVISION").save('s3a://ph-platform/2020-08-10/datamart/standard/dimensions/hosps/v20201001_20201030_1')
    # df = spark.read.parquet('s3a://ph-platform/2020-08-10/datamart/standard/dimensions/hosps/v20201001_20201030_2')
    # # df = df.dropDuplicates(['STANDARD', 'REVISION', 'PHA_ID'])
    # # df.write.format("parquet").mode('overwrite').partitionBy("STANDARD", "REVISION").save('s3a://ph-platform/2020-08-10/datamart/standard/dimensions/hosps/v20201001_20201030_2')
    # # df.groupby('STANDARD', '_ID').count().sort(col('count').desc()).show()
    # # print(df.filter(df.REVISION == '2011').count())
    # # print(df.filter(df.REVISION == '2011').dropDuplicates(['PHA_ID']).count())
    # # df.filter(df.REVISION == '2011').dropDuplicates(['PHA_ID']).groupby('PHA_ID').count().sort(col('count').desc()).show()
    # # df.filter(df._ID == 'PHA0004258').show()
    # # df.show()
    # print(df.filter((df.STANDARD == "STANDARD") & (df.REVISION == "2011")).count())
    # print(df.filter((df.STANDARD == "STANDARD") & (df.REVISION == "2011")).select('_ID').distinct().count())
    # print(df.filter((df.STANDARD == "STANDARD") & (df.REVISION == "2013")).count())
    # print(df.filter((df.STANDARD == "STANDARD") & (df.REVISION == "2013")).select('_ID').distinct().count())
    # print(df.filter((df.STANDARD == "STANDARD") & (df.REVISION == "2019")).count())
    # print(df.filter((df.STANDARD == "STANDARD") & (df.REVISION == "2019")).select('_ID').distinct().count())
    # print(df.filter((df.STANDARD == "STANDARD") & (df.REVISION == "LATEST")).count())
    # print(df.filter((df.STANDARD == "STANDARD") & (df.REVISION == "LATEST")).select('_ID').distinct().count())
    # print(df.filter(df.STANDARD == "MAX").count())
    # print(df.filter(df.STANDARD == "MAX").select('_ID').distinct().count())
    # df.groupby('PHA_ID').count().sort(col('count').desc()).show()