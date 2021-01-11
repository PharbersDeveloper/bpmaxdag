# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is gen cube job 

"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as func
from phlogs.phlogs import phlogger
from pyspark.sql.functions import lit
from pyspark.sql.functions import floor
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import udf
import string


def execute(start, end, replace): 
    # 2. check the start year, and end year
    sd = int(start)
    ed = int(end)
  
    if replace == "False":
        return

     spark = SparkSession.builder \
        .master("yarn") \
        .appName("data cube job") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.instance", "1") \
        .config("spark.executor.memory", "1g") \
        .config('spark.sql.codegen.wholeStage', False) \
        .enableHiveSupport() \
        .getOrCreate()

    # access_key = os.getenv("AWS_ACCESS_KEY_ID")
    # secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    access_key = "AKIAWPBDTVEAJ6CCFVCP"
    secret_key = "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"
    if access_key is not None:
        spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
        spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
        spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
        spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
        # spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
        spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.cn-northwest-1.amazonaws.com.cn")

    phlogger.info("preparing data from hive")
  
    df = spark.sql("select * from max_result")
    df = df.where((df.Date > sd) & (df.Date < ed))
    
    # 1. id
    df = df.withColumn("ID", monotonically_increasing_id())
    
    df = df.withColumnRenamed("Province", "PROVINCE_NAME")
    df = df.withColumnRenamed("City", "CITY_NAME")
    df = df.withColumnRenamed("Molecule", "MOLE_NAME")
    # df = df.withColumnRenamed("Prod_Name", "PRODUCT_NAME")
    df = df.withColumnRenamed("company", "COMPANY")
    df = df.withColumnRenamed("Prod_Name", "MIN")
    
    # 2.5 fill the product name with the min
    min2prod_udf = udf(lambda x: string.split(x, "|")[0], StringType())
    df = df.withColumn("PRODUCT_NAME", min2prod_udf(df.MIN))

    # 3. fill the QUARTER
    df = df.withColumn("YEAR", floor(df.Date / 100))
    df = df.withColumn("MONTH", floor(df.Date - df.YEAR * 100))
    df = df.withColumn("QUARTER", floor((df.MONTH - 1) / 3 + 1))

    # 4. fill the MKT data with company name
    df = df.withColumn("MKT", df.COMPANY)

    # 5. fill na and NaN
    df = df.fillna(0.0)

    # 6. for every measure should not have null or NaN
    df = df.withColumnRenamed("Predict_Sales", "SALES_VALUE")
    df = df.withColumnRenamed("Predict_Unit", "SALES_QTY")

    # 7. fill a column with COUNTRY, which represents the highest level of geo dimension
    df = df.withColumn("COUNTRY_NAME", lit("CHINA"))

    # 8. prune the data with only which need
    df = df.select("YEAR", "QUARTER", "MONTH", "COUNTRY_NAME", "PROVINCE_NAME", "CITY_NAME", "COMPANY", "MKT", "MOLE_NAME", "PRODUCT_NAME", "SALES_QTY", "SALES_VALUE")

    df = df.withColumn("apex", lit("alfred"))
    df = df.withColumn("dimension.name", lit("*"))
    df = df.withColumn("dimension.value", lit("*"))
    df.show()
    
    df.repartition(2).write.format("parquet").mode("overwrite").save("s3a://ph-max-auto/v0.0.2-2020-08-11/cube/origin")
    