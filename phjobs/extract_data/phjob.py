# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
from ph_logs.ph_logs import phlogger
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql import functions as func
import os
from pyspark.sql.functions import pandas_udf, PandasUDFType
import time

def execute(max_path, out_path, out_suffix, extract_file, time_left, time_right, molecule, atc, project, doi):
    os.environ["PYSPARK_PYTHON"] = "python3"
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
    
    '''
    path_for_extract_path = "Empty"
    time_left = 201610
    time_right = 202002
    molecule = "米格列奈, 那格列奈, 瑞格列奈"
    #molecule = "多西他赛, 曲司氯铵"
    #atc = "A10M1"
    atc = "Empty"
    project = "AZ"
    #doi = "Empty"
    doi="SNY1"
    out_suffix = "test_doi_Molecule_project"
    '''
    
    # 输入文件
    max_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/"
    extrac_path = "s3a://ph-stream/common/public/max_result/0.0.5/"
    out_path = "s3a://ph-stream/common/public/max_result/0.0.5/extract_data_out"
    
    max_standard_brief_all_path = extrac_path + "/max_standard_brief_all"
    if  extract_file == "Empty":
        path_for_extract_path = extrac_path + "/path_for_extract.csv"
    else:
        path_for_extract_path = extract_file
    
    if out_suffix == "Empty":
        raise ValueError('out_suffix: missing')
    
    # 满足率计算输入文件
    
    ims_mapping_path = max_path + "/Common_files/extract_data_files/ims_mapping_202007.csv"
    ims_sales_path = max_path + "/Common_files/extract_data_files/cn_IMS_Sales_Fdata_202007.csv"
    molecule_ACT_path = max_path  + "/Common_files/extract_data_files/product_map_all_ATC.csv"
    packID_ACT_map_path = max_path  + "/Common_files/extract_data_files/packID_ATC_map.csv"
    
    
    # 输出文件
    timenow = time.strftime("%Y-%m-%d", time.localtime()).replace("-", "_")
    outdir = "out_" + timenow + "_" + out_suffix
    out_extract_data_path = out_path + "/" + outdir + "/out_" + timenow + "_" + out_suffix + '.csv'
    report_a_path = out_path + "/" + outdir + "/report_a.csv"
    
    # 提数条件
    time_left = int(time_left)
    time_right = int(time_right)
    
    if molecule == "Empty":
        molecule = [] 
    else:    
        molecule = molecule.replace(" ","").split(",")
    
    if atc == "Empty":
        atc = []
    else:
        atc = atc.replace(" ","").split(",")
        
    if project == "Empty":
        project = []
    else:
        project = project.replace(" ","").split(",")
        
    if doi == "Empty":
        doi = []
    else:
        doi = doi.replace(" ","").split(",")
    
    # ================ 数据执行 ==================
    path_for_extract = spark.read.csv(path_for_extract_path, header=True)
    max_standard_brief_all = spark.read.parquet(max_standard_brief_all_path)
    
    
    # 1. 根据提数需求获取 max_filter_path_month
    
    # 筛选,获取符合条件的项目和月份
    max_filter_list = max_standard_brief_all.where((max_standard_brief_all.Date >= time_left) & (max_standard_brief_all.Date <= time_right))
    if project:
        max_filter_list = max_filter_list.where(max_standard_brief_all.project.isin(project))
    if doi:
        max_filter_list = max_filter_list.where(max_standard_brief_all.DOI.isin(doi))
    if atc:
        max_filter_list = max_filter_list.where(max_standard_brief_all.ATC.isin(atc))
    if molecule:
        max_filter_list = max_filter_list.where(max_standard_brief_all['标准通用名'].isin(molecule))
    
    
    project_Date_list = max_filter_list.select("project", "Date").distinct()
    
    #         
    max_filter_path = project_Date_list.join(path_for_extract, on="project", how="left")
    max_filter_path = max_filter_path.withColumn("path_month", func.concat(max_filter_path.path, func.lit("/Date_copy="), max_filter_path.Date))
    max_filter_path_month = max_filter_path.select("path_month").distinct().toPandas()["path_month"].tolist()
    
    # 2. 根据 max_filter_path_month 汇总max结果
    index = 0
    for eachpath in max_filter_path_month:
        df = spark.read.parquet(eachpath)
        # 过滤数据
        if doi:
            df = df.where(df.DOI.isin(doi))
        if atc:
            df = df.where(df.ATC.isin(atc))
        if molecule:
            df = df.where(df['标准通用名'].isin(molecule))
        # 汇总    
        if index ==0:
            max_filter_raw = df
        else:
            max_filter_raw = max_filter_raw.union(df)
        index += 1
    
    
    # 3. 注释项目排名
    project_rank_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/Common_files/project_rank.csv"
    project_rank = spark.read.csv(project_rank_path, header=True)
    project_rank = project_rank.withColumnRenamed("项目", "project") \
                        .withColumn("排名", project_rank["排名"].cast(IntegerType())) \
                        .withColumnRenamed("排名", "rank_score")
                        
    max_filter_out = max_filter_raw.join(project_rank, on="project", how="left")
    
    
    
    # 原始提数结果
    max_filter_out = max_filter_out.select("project", "rank_score", "Date", "ATC", "标准通用名", "标准商品名", "标准剂型", "标准规格", 
                    "标准包装数量", "标准生产企业", "标准省份名称", "标准城市名称", "DOI", "Predict_Sales", "Predict_Unit")
    max_filter_out = max_filter_out.withColumnRenamed("Predict_Sales", "Sales") \
                            .withColumnRenamed("Predict_Unit", "Units")
                            
    # 提数报告
    report = max_filter_out.select("project","rank_score","标准通用名", "ATC", "Date") \
                            .distinct() \
                            .groupby(["标准通用名", "ATC", "project","rank_score"]).count() \
                            .withColumnRenamed("count", "months_num") \
                            .persist()
    
    # 分子最大月份数, 月份最全-得分
    months_max = report.groupby("标准通用名").agg(func.max("months_num").alias("max_month"))
    
    report = report.join(months_max, on="标准通用名", how="left")
    report = report.withColumn("drop_for_months", func.when(report.months_num == report.max_month, func.lit(0)).otherwise(func.lit(1)))
    
    score_max = report.where(report.drop_for_months == 0) \
                .groupby("标准通用名").agg(func.min("rank_score").alias("max_score"))
                
    report = report.join(score_max, on="标准通用名", how="left")
                    
    report = report.withColumn("drop_for_score", func.when(report.rank_score == report.max_score, func.lit(0)).otherwise(func.lit(1)))
    report = report.withColumn("drop_for_score", func.when(report.drop_for_months == 0, report.drop_for_score).otherwise(None))
    
    report_a = report.drop("max_score", "max_month") \
                .withColumn("time_range", func.lit(str(time_left) + '_' + str(time_right)))
    report_a = report_a.repartition(1)
    report_a.write.format("csv").option("header", "true") \
        .mode("overwrite").save(report_a_path)
    
    # 根据report_a去重
    out_extract_data = max_filter_out.join(report_a.where(report_a.drop_for_score == 0).select("标准通用名", "project"), 
                                        on=["标准通用名", "project"], 
                                        how="inner").persist()
    out_extract_data = out_extract_data.repartition(1)
    out_extract_data.write.format("csv").option("header", "true") \
        .mode("overwrite").save(out_extract_data_path)
        
    # report_b
    report_b = out_extract_data.groupby("project", "ATC").agg({"Sales":"sum", "Units":"sum"}) \
                        .withColumnRenamed("sum(Sales)", "Sales") \
                        .withColumnRenamed("sum(Units)", "Units") \
                        .withColumn("time_range", func.lit(str(time_left) + '_' + str(time_right)))
    
    # report_c
    report_c = out_extract_data.groupby("project", "标准通用名", "ATC").agg({"Sales":"sum", "Units":"sum"}) \
                    .withColumnRenamed("sum(Sales)", "Sales") \
                    .withColumnRenamed("sum(Units)", "Units") \
                    .join(report_a.select("project", "标准通用名", "ATC", "months_num", "time_range").distinct(), 
                                on=["project", "标准通用名", "ATC"], 
                                how="left")

