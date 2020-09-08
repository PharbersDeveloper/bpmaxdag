# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

Create Hive Table
"""
import os
import subprocess
from pyspark.sql import SparkSession, functions as F


def sava_as_table_by_scala(spark, input_path, output_path, table_name):
    cmd_str = '''
    $SPARK_HOME/bin/spark-submit \
    --name {name} \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory {driver_memory} \
    --executor-memory {executor_memory} \
    --executor-cores {executor_cores} \
    --num-executors {num_executors} \
    --conf spark.hadoop.fs.s3a.access.key={access_key} \
    --conf spark.hadoop.fs.s3a.secret.key={secret_key} \
    --conf spark.hadoop.fs.s3a.endpoint=s3.cn-northwest-1.amazonaws.com.cn \
    --class {class} \
    {jar_path} \
    {input_file_format} {input_path} \
    {output_file_format} {output_path} \
    {save_mode} {table_name}
    '''

    confs = spark.sparkContext.getConf()
    # confs_dict = dict(confs.getAll())

    cmd_str = cmd_str.format(**{'name': confs.get('spark.app.name'),
                                'driver_memory': confs.get('spark.driver.memory'),
                                'executor_memory': confs.get('spark.executor.memory'),
                                'executor_cores': confs.get('spark.executor.cores'),
                                'num_executors': confs.get('spark.executor.instance'),
                                'access_key': confs.get('spark.hadoop.fs.s3a.access.key') if confs.get('spark.hadoop.fs.s3a.access.key') else '',
                                'secret_key': confs.get('spark.hadoop.fs.s3a.secret.key') if confs.get('spark.hadoop.fs.s3a.secret.key') else '',
                                'class': 'com.pharbers.StreamEngine.BatchJobs.SaveAsTable',
                                'jar_path': 's3a://ph-platform/2020-08-10/functions/scala/BPStream/SaveAsTable-20200904.jar',
                                'input_file_format': 'parquet',
                                'input_path': input_path,
                                'output_file_format': 'parquet',
                                'output_path': output_path,
                                'save_mode': 'overwrite',
                                'table_name': table_name,
                                })

    result = subprocess.Popen(cmd_str, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    for line in iter(result.stdout.readline, b''):
        line = line.rstrip().decode('utf8')
        print(">>>", line)


def get_all_table(spark):
    return spark.sql("SHOW tables")


def delete_table(spark, table_name):
    return spark.sql("drop table {}".format(table_name))


def execute(input_path, output_path, table_name):
    os.environ["PYSPARK_PYTHON"] = "python3"
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("create_hive_table") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.instance", "1") \
        .config("spark.executor.memory", "1g") \
        .config('spark.sql.codegen.wholeStage', False) \
        .enableHiveSupport() \
        .getOrCreate()

    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    if access_key:
        spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
        spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
        spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
        spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.cn-northwest-1.amazonaws.com.cn")

    input_data_df = spark.read.parquet(input_path)
    input_data_df.coalesce(4).write.mode('overwrite') \
        .option("compression", "snappy") \
        .option('path', output_path) \
        .saveAsTable(table_name)
        
# execute('s3a://ph-stream/common/public/prod/17', 's3a://ph-stream/common/public/prod/0.0.15', 'prod')

# $SPARK_HOME/bin/spark-submit \
# --name saveAsTable-submit \
# --master yarn \
# --deploy-mode cluster \
# --driver-memory 1g \
# --executor-memory 1g \
# --executor-cores 1 \
# --num-executors 1 \
# --conf spark.hadoop.fs.s3a.access.key=$AWS_ACCESS_KEY_ID \
# --conf spark.hadoop.fs.s3a.secret.key=$AWS_SECRET_ACCESS_KEY \
# --conf spark.hadoop.fs.s3a.endpoint=s3.cn-northwest-1.amazonaws.com.cn \
# main.py
