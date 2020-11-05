# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
import logging
import os
import io
import boto3
import pandas as pd


def s3excel2df(spark, path):
    path_lst = path.split('/')
    source_bucket = path_lst[2]
    source_path = "/".join(path_lst[3:])
    
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        
    s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    object_file = s3_client.get_object(Bucket=source_bucket, Key=source_path)
    data = object_file['Body'].read()
    pd_df = pd.read_excel(io.BytesIO(data), encoding='utf-8')
    
    # 	pd_df.columns = [str(index)+"#"+column for index, column in enumerate(pd_df)]
    return spark.createDataFrame(pd_df.astype(str))


def align_schema(df, schemas):
	str_get_value = {
		'string': None,
		'long': None,
		'double': None,
		'boolean': None,
	}
	
	cur_names = df.schema.fieldNames()
	for schema in schemas:
		if schema.name not in cur_names:
			df = df.withColumn(schema.name, lit(str_get_value.get(schema.dataType.typeName(), '')))
			if 'long' == schema.dataType.typeName():
				df = df.withColumn(schema.name, df[schema.name].cast('string').cast('int'))
	return df
	

def execute(**kwargs):
    
    logging.basicConfig(format='%(asctime)s %(filename)s %(funcName)s %(lineno)d %(message)s')
    logger = logging.getLogger('driver_logger')
    logger.setLevel(logging.INFO)
    logger.info("Origin kwargs = {}.".format(str(kwargs)))

    # input required
    prod_input_path = kwargs['prod_input_path']
    if prod_input_path == 'not set':
        raise Exception("Invalid prod_input_path!", prod_input_path)
    mnfs_input_path = kwargs['mnfs_input_path']
    if mnfs_input_path == 'not set':
        raise Exception("Invalid mnfs_input_path!", mnfs_input_path)
        
    # output required
    mnfs_output_path = kwargs['mnfs_output_path']
    if mnfs_output_path == 'not set':
        raise Exception("Invalid mnfs_output_path!", mnfs_output_path)

    os.environ["PYSPARK_PYTHON"] = "python3"
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("datamart dimension mnfs job") \
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

    logger.info("preparing data")
    
    mnfs_alias_df = s3excel2df(spark, mnfs_input_path)
    mnfs_alias_parent_df = align_schema(mnfs_alias_df.select(col('PARENT_MNFS').alias('MNFS_NAME')).cache(), mnfs_alias_df.schema)
    mnfs_alias_df = mnfs_alias_df.unionByName(mnfs_alias_parent_df).dropDuplicates(['MNFS_NAME']) # 438
    
    prod_df = spark.read.parquet(prod_input_path)
    mnfs_prod_df = prod_df.withColumnRenamed("MNF_TYPE_NAME", "MNF_TYPE_NAME_EN") \
        .select(
            "MNF_ID", 
            "MNF_TYPE", 
            "MNF_TYPE_NAME_EN", 
            "MNF_TYPE_NAME_CH", 
            "MNF_NAME_EN", 
            "MNF_NAME_CH"
        ).dropDuplicates(['MNF_NAME_CH']) # 2753
        
    mnfs_join_df = mnfs_prod_df.join(mnfs_alias_df, mnfs_prod_df.MNF_NAME_CH == mnfs_alias_df.MNFS_NAME, "outer") # 2940
    unmatch_mnfs_df = mnfs_join_df.filter(mnfs_join_df.MNFS_NAME.isNull()).drop('MNFS_NAME') # 2502
    match_mnfs_df = mnfs_join_df.filter((~mnfs_join_df.MNF_NAME_CH.isNull()) & (~mnfs_join_df.MNFS_NAME.isNull())).drop('MNFS_NAME') # 251
    new_mnfs_df = mnfs_join_df.filter(mnfs_join_df.MNF_NAME_CH.isNull()) \
                                .drop('MNF_NAME_CH') \
                                .withColumnRenamed('MNFS_NAME', 'MNF_NAME_CH') # 187
    mnfs_df = unmatch_mnfs_df.unionByName(match_mnfs_df).unionByName(new_mnfs_df) \
                .repartition(1).withColumn("_ID", monotonically_increasing_id()).cache() # 2940
    
    mnfs_id_df = mnfs_df.select(col('MNF_NAME_CH').alias('MI_MNF_NAME_CH'), col('_ID').alias('MI_ID'))
    mnfs_df = mnfs_df.join(mnfs_id_df, mnfs_df.PARENT_MNFS == mnfs_id_df.MI_MNF_NAME_CH, 'left') \
                        .drop('PARENT_MNFS', 'MI_MNF_NAME_CH') \
                        .withColumnRenamed('MI_ID', 'PARENT_ID')
    
    mnfs_df.write.format("parquet").mode("overwrite").save(mnfs_output_path)
