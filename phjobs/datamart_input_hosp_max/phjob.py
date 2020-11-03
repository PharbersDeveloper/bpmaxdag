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


def s3excel2df(spark, source_bucket, source_path):
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        
    s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    object_file = s3_client.get_object(Bucket=source_bucket, Key=source_path)
    data = object_file['Body'].read()
    pd_df = pd.read_excel(io.BytesIO(data), encoding='utf-8')
        
    return spark.createDataFrame(pd_df.astype(str))


def max_universe_format_to_standard(df):
    df = df.drop('Panel ID') \
        .withColumnRenamed('PHA Hosp name', 'PHA_HOSP_NAME') \
        .withColumnRenamed('PHA ID', 'PHA_ID') \
        .drop('If Panel') \
        .drop('Segment') \
        .drop('Segment Description') \
        .withColumnRenamed('Hosp_level', 'HOSP_LEVEL') \
        .withColumnRenamed('Region', 'REGION') \
        .withColumnRenamed('Province', 'PROVINCE') \
        .withColumnRenamed('Prefecture', 'CITY') \
        .withColumnRenamed('City Tier 2010', 'CITY_TIER') \
        .withColumnRenamed('Specialty_1', 'SPECIALTY_CATE') \
        .withColumnRenamed('Specialty_2', 'SPECIALTY_ADMIN') \
        .withColumnRenamed('Re-Speialty', 'RE_SPECIALTY') \
        .withColumnRenamed('Specialty 3', 'SPECIALTY_DETAIL') \
        .withColumnRenamed('Est_DrugIncome_RMB', 'DRUGINCOME_RMB') \
        .withColumnRenamed('医生数', 'DOCTORS_NUM') \
        .withColumnRenamed('床位数', 'BED_NUM') \
        .withColumnRenamed('全科床位数', 'GENERAL_BED_NUM') \
        .withColumnRenamed('内科床位数', 'INTERNAL_BED_NUM') \
        .withColumnRenamed('外科床位数', 'SURG_BED_NUM') \
        .withColumnRenamed('眼科床位数', 'OPHTH_BED_NUM') \
        .withColumnRenamed('年诊疗人次', 'ANNU_DIAG_TIME') \
        .withColumnRenamed('门诊诊次', 'OUTP_DIAG_TIME') \
        .withColumnRenamed('内科诊次', 'INTERNAL_DIAG_TIME') \
        .withColumnRenamed('外科诊次', 'SURG_DIAG_TIME') \
        .withColumnRenamed('入院人数', 'ADMIS_TIME') \
        .withColumnRenamed('住院病人手术人次数', 'SURG_TIME') \
        .withColumnRenamed('医疗收入', 'MED_INCOME') \
        .withColumnRenamed('门诊收入', 'OUTP_INCOME') \
        .withColumnRenamed('门诊治疗收入', 'OUTP_TREAT_INCOME') \
        .withColumnRenamed('门诊手术收入', 'OUTP_SURG_INCOME') \
        .withColumnRenamed('住院收入', 'IN_HOSP_INCOME') \
        .withColumnRenamed('住院床位收入', 'BED_INCOME') \
        .withColumnRenamed('住院治疗收入', 'IN_HOSP_TREAT_INCOME') \
        .withColumnRenamed('住院手术收入', 'IN_HOSP_SURG_INCOME') \
        .withColumnRenamed('药品收入', 'DRUG_INCOME') \
        .withColumnRenamed('门诊药品收入', 'OUTP_DRUG_INCOME') \
        .withColumnRenamed('门诊西药收入', 'OUTP_WST_DRUG_INCOME') \
        .withColumnRenamed('住院药品收入', 'IN_HOSP_DRUG_INCOME') \
        .withColumnRenamed('住院西药收入', 'IN_HOSP_WST_DRUG_INCOME')
        
    return df
    
    
def align_schema(df, schemas):
    str_get_value = {
        'string': '',
        'long': 0,
        'double': 0.0,
        'boolean': False,
    }
    
    cur_names = df.schema.fieldNames()
    for schema in schemas:
        if schema.name not in cur_names:
            df = df.withColumn(schema.name, lit(str_get_value.get(schema.dataType.typeName(), '')))
            if 'long' == schema.dataType.typeName():
                df = df.withColumn(schema.name, df[schema.name].cast('string').cast('int'))
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
        
    output_path = kwargs.get('output_path', '')
    if output_path == 'not set' or output_path == '':
        raise Exception("Invalid output_path!", output_path)
    
    os.environ["PYSPARK_PYTHON"] = "python3"
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("datamart dimension input max hosp job") \
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
        
    input_path_lst = input_path.split('/')
    source_bucket = input_path_lst[2]
    source_path = "/".join(input_path_lst[3:])
    
    df = s3excel2df(spark, source_bucket=source_bucket, source_path=source_path)
    df = max_universe_format_to_standard(df)

    df = df.withColumn('STANDARD', lit('MAX')).repartition(1).withColumn("_ID", monotonically_increasing_id()).cache()
    df.repartition("STANDARD").write.format("parquet").mode('overwrite').partitionBy("STANDARD").save(output_path)
    