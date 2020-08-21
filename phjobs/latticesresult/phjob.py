# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import lit
from pyspark.sql.functions import desc
from pyspark.sql.functions import monotonically_increasing_id
from phlogs.phlogs import phlogger
import pandas as pd
import urllib


def execute(a, b):
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("data cube lattices result job") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.instance", "1") \
        .config("spark.executor.memory", "1g") \
        .config('spark.sql.codegen.wholeStage', False) \
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

    phlogger.info("group by lattices data")
    
    meta_lattices_df = spark.read.parquet("s3a://ph-max-auto/2020-08-11/cube/dest/8cd67399-3eeb-4f47-aaf9-9d2cc4258d90/meta/lattices").where(col("CUBOIDS_ID") == 3).toPandas()
    
    columns = ["YEAR", "MONTH", "QUARTER", "COUNTRY_NAME", "PROVINCE_NAME", "CITY_NAME", "MKT", "COMPANY", "MOLE_NAME", "PRODUCT_NAME", "CUBOIDS_ID", "CUBOIDS_NAME", "LATTLES", "apex", "dimension_name", "dimension_value"]
    sch_columns = ["YEAR", "MONTH", "QUARTER", "COUNTRY_NAME", "PROVINCE_NAME", "CITY_NAME", "MKT", "COMPANY", "MOLE_NAME", "PRODUCT_NAME", "CUBOIDS_ID", "CUBOIDS_NAME", "LATTLES", "apex", "dimension_name", "dimension_value", "SALES_QTY", "SALES_VALUE"]

    schema = \
        StructType([ \
            StructField("QUARTER", LongType()), \
            StructField("COUNTRY_NAME", StringType()), \
            StructField("PROVINCE_NAME", StringType()), \
            StructField("CITY_NAME", StringType()), \
            StructField("MKT", StringType()), \
            StructField("MOLE_NAME", StringType()), \
            StructField("PRODUCT_NAME", StringType()), \
            StructField("SALES_QTY", DoubleType()), \
            StructField("SALES_VALUE", DoubleType()), \
            StructField("apex", StringType()), \
            StructField("dimension.name", StringType()), \
            StructField("dimension.value", StringType()), \
            StructField("YEAR", IntegerType()), \
            StructField("MONTH", IntegerType()), \
            StructField("COMPANY", StringType()), \
            StructField("CUBOIDS_ID", LongType()), \
            StructField("CUBOIDS_NAME", StringType()), \
            StructField("LATTLES", ArrayType(StringType()))
        ])
    
    # years = [2018, 2019]
    # months = range(1, 13)
    years = [2019]
    months = range(10, 13)
   
    dim = spark.read.parquet("s3a://ph-max-auto/2020-08-11/cube/dest/8cd67399-3eeb-4f47-aaf9-9d2cc4258d90/meta/dimensions") \
            .repartition(1).withColumn("LEVEL", monotonically_increasing_id())
    dim_group_level = dim.groupBy("DIMENSION").agg({"LEVEL":"min"}).withColumnRenamed("min(LEVEL)", "EDGE")
    dim = dim.join(dim_group_level, how="left", on="DIMENSION").toPandas()

    def leaf2Condi(df, lts):
        res = []
        for tmp in lts:
            level = df[df["HIERARCHY"] == tmp].iloc[0]["LEVEL"]
            edge = df[df["HIERARCHY"] == tmp].iloc[0]["EDGE"]
            res.extend(list(df[(df["LEVEL"] >= edge) & (df["LEVEL"] <= level)]["HIERARCHY"]))
        return res
    
    for year in years:
        for month in months:
            for index, row in meta_lattices_df.iterrows():
                cid = row["CUBOIDS_ID"]
            	lts = []
            	for tmp in row["LATTLES"]:
            		lts.append(str(tmp))
            	path = str(lts).replace("'", "").replace("[", "%5B").replace("]", "%5D")
            	phlogger.info(year)
            	phlogger.info(month)
            	phlogger.info(cid)
            	phlogger.info(path)
            	df = spark.read.parquet("s3a://ph-max-auto/2020-08-11/cube/dest/8cd67399-3eeb-4f47-aaf9-9d2cc4258d90/lattices-buckets/content/YEAR=" + str(year) + "/MONTH=" + str(month) + "/CUBOIDS_ID=" + str(cid) + "/LATTLES=" + path)
            	condi = ["YEAR", "MONTH", "CUBOIDS_ID"]
            	condi.extend(leaf2Condi(dim, lts))
            	print "===> alfred test"
            	print condi
            	print "===> alfred test"
            	df = df.withColumn("YEAR", lit(year)) \
            	        .withColumn("MONTH", lit(month)) \
            	        .withColumn("CUBOIDS_ID", lit(cid)) \
            	        .groupBy(condi).agg({"SALES_VALUE": "sum", "SALES_QTY": "sum"}) \
            	        .withColumnRenamed("sum(SALES_VALUE)", "SALES_VALUE") \
            	        .withColumnRenamed("sum(SALES_QTY)", "SALES_QTY")
            	ad_col = list(set(columns).difference(set(condi)))
            	for tc in ad_col:
            	    if tc is "LATTLES":
            	        df = df.withColumn(tc, lit(path))
            	    elif tc is "dimension_name":
            	        df = df.withColumn(tc, lit(row["CUBOIDS_NAME"]))
            	    elif tc is "CUBOIDS_NAME":
            	        df = df.withColumn(tc, lit(row["CUBOIDS_NAME"]))
            	   # elif tc is "dimension_value":
            	       # df = df.withColumn(tc, map(lts))
            	    else:
            	        df = df.withColumn(tc, lit("*"))
            
            	df = df.select(sch_columns).orderBy(desc("SALES_VALUE")).repartition(1)
            	df.persist()
                # full lattices	
            	# 这时数据量偏小的情况下，不需要在分桶了
            	df.write.mode("append") \
            	    .partitionBy("YEAR", "MONTH", "CUBOIDS_ID", "LATTLES") \
        			.parquet("s3a://ph-max-auto/2020-08-11/cube/dest/8cd67399-3eeb-4f47-aaf9-9d2cc4258d90/result2/lattices-result")
        		
                '''
        		    这个地方也是一个严重的bug, 要的是top n per group，而不是单纯的top n
                '''
                # ice cube
                # df.limit(10).write.mode("append") \
        		  #  .partitionBy("YEAR", "MONTH", "CUBOIDS_ID", "LATTLES") \
        		  #  .parquet("s3a://ph-max-auto/2020-08-11/cube/dest/8cd67399-3eeb-4f47-aaf9-9d2cc4258d90/result/ice-cube-lattices")
                
                df.unpersist()
    