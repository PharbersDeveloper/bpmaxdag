# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as func
from phlogs.phlogs import phlogger
from pyspark.sql.functions import explode
from pyspark.sql.functions import col
import string
from uuid import uuid4
import pandas as pd
import numpy as np
import itertools


def execute(a, b):
   
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("data cube create dimension") \
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

    phlogger.info("create data cube dimensions")
 
    # init dimensions
    dim = [
        ("time", ["YEAR","QUARTER","MONTH"]),
        ("geo", ["COUNTRY_NAME","PROVINCE_NAME","CITY_NAME"]),
        ("prod", ["COMPANY","MKT","MOLE_NAME","PRODUCT_NAME"])
    ]
 
    schema = \
        StructType([ \
            StructField("DIMENSION", StringType()), \
            StructField("HIERARCHYS", ArrayType(StringType()))
        ])
    
    df = spark.createDataFrame(dim, schema)
    df = df.withColumn("HIERARCHY",explode(col("HIERARCHYS"))).select("DIMENSION", "HIERARCHY")
    
    df.repartition(1).write.mode("overwrite").parquet("s3a://ph-max-auto/2020-08-11/cube/dest/8cd67399-3eeb-4f47-aaf9-9d2cc4258d90/dimensions")
    
    phlogger.info("create data cube cuboids frame")
   
    # time is the bucket line, not considered 
    dimensions = df.select("DIMENSION").distinct()
    dimensions.show()
    hierarchy = df.select("HIERARCHY").distinct()
    hierarchy.show()
   
    local_dimensions = dimensions.toPandas()["DIMENSION"]
    phlogger.info(local_dimensions)
    
    
    # init the cuboids
    cuboids = []
    crs = []
    
    for ld in range(len(local_dimensions) + 1):
        # 1. for all dimensions in these example is 3-D cuboid or base cuboid
        # 1.1 for n-dimension should have 2^n cuboids (or panels)
        #     in this place I am using the combination (排列组合)
        for cuboid in itertools.combinations(local_dimensions, ld):
            # 1.2 construct bitmap dimension indexing
            #     also can be save the last when you want to build indexing of the cuboids
            l = list(cuboid)
           
            # latice condition
            la = []
            for a in l:
                for index in range(len(dim)):
                    if dim[index][0] == a:
                        la.append(dim[index][1])
            tcrs = []
            if len(la) is not 0:
                for tmp in cartesian(la):
                    tcrs.append(tmp.tolist())
                    
            cuboids.append(l)
            crs.append(tcrs)

    pdf = pd.DataFrame()
    pdf.loc[:, "CUBOIDS"] = cuboids
    pdf.loc[:, "DIMENSION_COUNT"] = pdf["CUBOIDS"].apply(lambda x: len(x))
    pdf.loc[:, "CUBOIDS_NAME"] = pdf["DIMENSION_COUNT"].apply(lambda x: str(x)) + "-D-" + pdf["CUBOIDS"].apply(lambda x: "-".join(x))
    pdf.loc[:, "LATTLCES_CONDIS"] = crs
    
    schema = \
        StructType([ \
            StructField("CUBOIDS", ArrayType(StringType())),
            StructField("DIMENSION_COUNT", IntegerType()),
            StructField("CUBOIDS_NAME", StringType()),
            StructField("LATTLCES_CONDIS", ArrayType(ArrayType(StringType())))
        ])
    df = spark.createDataFrame(pdf[1:], schema)
    df.show()
    
    df.repartition(1).write.mode("overwrite").parquet("s3a://ph-max-auto/2020-08-11/cube/dest/8cd67399-3eeb-4f47-aaf9-9d2cc4258d90/cuboids")

    # init lattice condition
    lattice_df = df.withColumn("LATTLES", explode(col("LATTLCES_CONDIS")))
    lattice_df.show()
    lattice_df.repartition(1).write.mode("overwrite").parquet("s3a://ph-max-auto/2020-08-11/cube/dest/8cd67399-3eeb-4f47-aaf9-9d2cc4258d90/lattices")
    
    

def cartesian(arrays, out=None):
    """
    Generate a cartesian product of input arrays.

    Parameters
    ----------
    arrays : list of array-like
        1-D arrays to form the cartesian product of.
    out : ndarray
        Array to place the cartesian product in.

    Returns
    -------
    out : ndarray
        2-D array of shape (M, len(arrays)) containing cartesian products
        formed of input arrays.

    Examples
    --------
    >>> cartesian(([1, 2, 3], [4, 5], [6, 7]))
    array([[1, 4, 6],
           [1, 4, 7],
           [1, 5, 6],
           [1, 5, 7],
           [2, 4, 6],
           [2, 4, 7],
           [2, 5, 6],
           [2, 5, 7],
           [3, 4, 6],
           [3, 4, 7],
           [3, 5, 6],
           [3, 5, 7]])

    """

    arrays = [np.asarray(x) for x in arrays]
    dtype = np.result_type(*arrays)

    n = np.prod([x.size for x in arrays])
    if out is None:
        out = np.zeros([n, len(arrays)], dtype=dtype)

    m = n / arrays[0].size
    out[:, 0] = np.repeat(arrays[0], m)
    if arrays[1:]:
        cartesian(arrays[1:], out=out[0:m, 1:])
        for j in xrange(1, arrays[0].size):
            out[j * m:(j + 1) * m, 1:] = out[0:m, 1:]
    return out
