# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
from phlogs.phlogs import phlogger
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql import functions as func

def execute(max_path, project_name, minimum_product_columns, minimum_product_sep, minimum_product_newname, need_cleaning_cols, out_path, out_dir, need_test):
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("data from s3") \
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
        # spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
        spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.cn-northwest-1.amazonaws.com.cn")

    phlogger.info('job2_product_mapping')
    
    out_path_dir = out_path + "/" + project_name + '/' + out_dir

    # 输入
    if project_name == "Sanofi" or project_name == "AZ":
        product_map_path = max_path + u"/AZ_Sanofi/az_sanofi清洗_ma"
    else:
        product_map_path = max_path + "/" + project_name + "/prod_mapping"
    hospital_mapping_out_path = out_path_dir + "/hospital_mapping_out"
    need_cleaning_cols = need_cleaning_cols.replace(", ",",").split(",")
    minimum_product_columns = minimum_product_columns.replace(", ",",").split(",")
    if minimum_product_sep == "kong":
        minimum_product_sep = ""
    # 输出
    product_mapping_out_path = out_path_dir + "/product_mapping_out"
    need_cleaning_path = out_path_dir + "/need_cleaning"

    # =========== 数据检查 =============
    phlogger.info('数据检查-start')

    # 存储文件的缺失列
    misscols_dict = {}

    # product_map file
    # product_map_path = "/common/projects/max/Sankyo/prod_mapping"
    product_map = spark.read.parquet(product_map_path)
    colnames_product_map = product_map.columns
    misscols_dict.setdefault("product_map", [])
    if ("标准通用名" not in colnames_product_map) and ("通用名_标准"  not in colnames_product_map) and ("通用名"  not in colnames_product_map):
        misscols_dict["product_map"].append("标准通用名")
    if "min1" not in colnames_product_map:
        misscols_dict["product_map"].append("min1")
    if "min2" not in colnames_product_map:
        misscols_dict["product_map"].append("min2")
    if ("标准商品名" not in colnames_product_map) and ("商品名_标准"  not in colnames_product_map) :
        misscols_dict["product_map"].append("标准商品名")

    # 判断输入文件是否有缺失列
    misscols_dict_final = {}
    for eachfile in misscols_dict.keys():
        if len(misscols_dict[eachfile]) != 0:
            misscols_dict_final[eachfile] = misscols_dict[eachfile]
    # 如果有缺失列，则报错，停止运行
    if misscols_dict_final:
        phlogger.error('miss columns: %s' % (misscols_dict_final))
        raise ValueError('miss columns: %s' % (misscols_dict_final))

    phlogger.info('数据检查-Pass')

    # =========== 数据执行 =============
    phlogger.info('数据执行-start')

    # raw_data_job1_out_path = "/user/ywyuan/max/Sankyo/raw_data_job1_out"
    raw_data = spark.read.parquet(hospital_mapping_out_path)
    raw_data = raw_data.withColumn("Brand", func.when(func.isnull(raw_data.Brand), raw_data.Molecule).
                                   otherwise(raw_data.Brand))

    # concat_multi_cols
    # minimum_product_columns = ["Brand", "Form", "Specifications", "Pack_Number", "Manufacturer"]
    # minimum_product_sep = ""
    # minimum_product_newname = "min1"
    for colname, coltype in raw_data.dtypes:
        if coltype == "logical":
            raw_data = raw_data.withColumn(colname, raw_data[colname].cast(StringType()))

    raw_data = raw_data.withColumn("tmp", func.when(func.isnull(raw_data[minimum_product_columns[0]]), func.lit("NA")).
                                   otherwise(raw_data[minimum_product_columns[0]]))

    for col in minimum_product_columns[1:]:
        raw_data = raw_data.withColumn("tmp", func.concat(
            raw_data["tmp"],
            func.lit(minimum_product_sep),
            func.when(func.isnull(raw_data[col]), func.lit("NA")).otherwise(raw_data[col])))

    raw_data = raw_data.withColumnRenamed("tmp", minimum_product_newname)

    # product_map
    for col in product_map.columns:
        if col in ["标准通用名", "通用名_标准"]:
            product_map = product_map.withColumnRenamed(col, "通用名")
        if col in ["商品名_标准"]:
            product_map = product_map.withColumnRenamed(col, "标准商品名")
        if col in ["标准途径"]:
            product_map = product_map.withColumnRenamed(col, "std_route")
    if "std_route" not in product_map.columns:
        product_map = product_map.withColumn("std_route", func.lit(''))

    product_map_for_needclean = product_map.select("min1").distinct()
    product_map_for_rawdata = product_map.select("min1", "min2", "通用名", "std_route", "标准商品名").distinct()

    # 输出待清洗
    need_cleaning_cols[1:1] = minimum_product_columns
    need_cleaning = raw_data.join(product_map_for_needclean, on="min1", how="left_anti") \
        .select(need_cleaning_cols) \
        .distinct()
    phlogger.info('待清洗行数: ' + str(need_cleaning.count()))

    if need_cleaning.count() > 0:
        need_cleaning = need_cleaning.repartition(2)
        need_cleaning.write.format("parquet") \
            .mode("overwrite").save(need_cleaning_path)
        phlogger.info("已输出待清洗文件至:  ".decode("utf-8") + need_cleaning_path)

    raw_data = raw_data.join(product_map_for_rawdata, on="min1", how="left") \
        .drop("S_Molecule") \
        .withColumnRenamed("通用名", "S_Molecule")

    product_mapping_out = raw_data.repartition(2)
    product_mapping_out.write.format("parquet") \
        .mode("overwrite").save(product_mapping_out_path)

    phlogger.info("输出 product_mapping 结果：".decode("utf-8") + product_mapping_out_path)

    phlogger.info('数据执行-Finish')

    # =========== 数据验证 =============
    # 与原R流程运行的结果比较正确性
    if int(need_test) > 0:
        phlogger.info('数据验证-start')

        my_out = raw_data

        if project_name == "Sanofi":
            R_out_path = "/common/projects/max/AZ_Sanofi/product_mapping/raw_data_with_std_product"
        elif project_name == "AZ":
            R_out_path = "/common/projects/max/AZ_Sanofi/product_mapping/raw_data_with_std_product_az"
        elif project_name == "Sankyo":
            R_out_path = "/user/ywyuan/max/Sankyo/Rout/product_mapping_out"
        R_out = spark.read.parquet(R_out_path)

        # 检查内容：列缺失，列的类型，列的值
        for colname, coltype in R_out.dtypes:
            # 列是否缺失
            if colname not in my_out.columns:
                phlogger.warning ("miss columns:", colname)
            else:
                # 数据类型检查
                if my_out.select(colname).dtypes[0][1] != coltype:
                    phlogger.warning("different type columns: " + colname + ", " + my_out.select(colname).dtypes[0][1] + ", " + "right type: " + coltype)

                # 数值列的值检查
                if coltype == "double" or coltype == "int":
                    sum_my_out = my_out.groupBy().sum(colname).toPandas().iloc[0, 0]
                    sum_R = R_out.groupBy().sum(colname).toPandas().iloc[0, 0]
                    # phlogger.info(colname, sum_raw_data, sum_R)
                    if (sum_my_out - sum_R) != 0:
                        phlogger.warning("different value(sum) columns: " + colname + ", " + str(sum_my_out) + ", " + "right value: " + str(sum_R))

        phlogger.info('数据验证-Finish')

    # =========== return =============
    return raw_data
