# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from ph_logs.ph_logs import phlogger
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql import functions as func
import os
from pyspark.sql.functions import pandas_udf, PandasUDFType

def execute(extract_path, extract_file, data_type):
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

    
    # 输入
    project = []
    doi = []
    atc = []
    molecule = []

    if extract_file == "Empty":
        path_for_extract_path = extract_path + "/path_for_extract.csv"
    else:
        path_for_extract_path = extract_file
        
    if data_type == 'max':
        extract_path = extract_path + '/max_standard'
    elif data_type == 'raw':
        extract_path = extract_path + '/rawdata_standard'
    
    project_rank_path =  "s3a://ph-max-auto/v0.0.1-2020-06-08/" + "/Common_files/extract_data_files/project_rank.csv"
    
    # 输出
    max_standard_brief_all_path = extract_path + "/max_standard_brief_all.csv"
    report_a_path = extract_path + "/all_report_a.csv"
    
    # =============== 数据执行 =============
    
    project_rank = spark.read.csv(project_rank_path, header=True)
    project_rank = project_rank.withColumnRenamed("项目", "project") \
                        .withColumn("排名", project_rank["排名"].cast(IntegerType())) \
                        .withColumnRenamed("排名", "project_score")

    # 二. 根据 max_standard_brief_all 确定最终提数来源
        
    # 1. 根据path_for_extract， 合并项目brief，生成max_standard_brief_all
    path_for_extract = spark.read.csv(path_for_extract_path, header=True)
    project_all = path_for_extract.toPandas()["project"].tolist()
    if data_type == 'raw':
        path_all_brief = [extract_path + '/' + i + "_rawdata_standard_brief" for i in project_all]
    elif data_type == 'max':
        path_all_brief = [extract_path + '/' + i + "_max_standard_brief" for i in project_all]
    
    # "project", "Date", "标准通用名", "ATC", "DOI"  ("PHA", "Source")
    index = 0
    for eachpath in path_all_brief:
        df = spark.read.parquet(eachpath)
        if index ==0:
            max_standard_brief_all = df
        else:
            max_standard_brief_all = max_standard_brief_all.union(df)
        index += 1
        

    max_standard_brief_all = max_standard_brief_all.repartition(1)
    max_standard_brief_all.write.format("csv").option("header", "true") \
        .mode("overwrite").save(max_standard_brief_all_path)


    
    # 2. 根据提数需求获取 max_filter_path_month
    # 筛选,获取符合条件的项目和月份
    max_filter_list = max_standard_brief_all
    if project:
        max_filter_list = max_filter_list.where(max_filter_list.project.isin(project))
    if doi:
        max_filter_list = max_filter_list.where(max_filter_list.DOI.isin(doi))
    if atc:
        if max([len(i) for i in atc]) == 3:
            max_filter_list = max_filter_list.withColumn("ATC", func.substring(max_filter_list.ATC, 0, 3)).distinct()
        elif max([len(i) for i in atc]) == 4:
            max_filter_list = max_filter_list.withColumn("ATC", func.substring(max_filter_list.ATC, 0, 4)).distinct()
        max_filter_list = max_filter_list.where(max_filter_list.ATC.isin(atc))
    if molecule:
        max_filter_list = max_filter_list.where(max_filter_list['标准通用名'].isin(molecule))
    
    # 3. 注释项目排名
    max_filter_list = max_filter_list.join(project_rank, on="project", how="left").persist()
    
    # 4. 根据月份数以及项目排名进行去重，确定最终提数来源，生成报告 report_a 
    report = max_filter_list.select("project","project_score","标准通用名", "ATC", "Date") \
                            .distinct() \
                            .groupby(["标准通用名", "ATC", "project","project_score"]).count() \
                            .withColumnRenamed("count", "months_num") \
                            .persist()
    
    # 分子最大月份数, 月份最全-得分
    months_max = report.groupby("标准通用名", "ATC").agg(func.max("months_num").alias("max_month"))
    
    report = report.join(months_max, on=["标准通用名", "ATC"], how="left")
    report = report.withColumn("drop_for_months", func.when(report.months_num == report.max_month, func.lit(0)).otherwise(func.lit(1)))
    
    # 对于raw_data 医院数量作为第二去重条件
    if data_type == 'raw':
        # 数据来源
        Source_window = Window.partitionBy("project", "标准通用名").orderBy(func.col('Source'))
        rank_window = Window.partitionBy("project", "标准通用名").orderBy(func.col('Source').desc())
        
        Source = max_filter_list.select("project", "标准通用名", 'Source').distinct() \
                                .select("project", "标准通用名",
                                         func.collect_list(func.col('Source')).over(Source_window).alias('Source'),
                                         func.rank().over(rank_window).alias('rank')).persist()
        Source = Source.where(Source.rank == 1).drop('rank')
        Source = Source.withColumn('Source', func.concat_ws(',', func.col('Source')))
        
        report = report.join(Source, on=["project", "标准通用名"], how='left').persist()                                
        
        # 医院数统计                    
        PHA_num = max_filter_list.select("project","project_score","标准通用名", "ATC", "PHA") \
                            .distinct() \
                            .groupby(["标准通用名", "ATC", "project","project_score"]).count() \
                            .withColumnRenamed("count", "PHA_num") \
                            .persist()
        report = report.join(PHA_num, on=["标准通用名", "ATC", "project", "project_score"], how="left").persist()
        
        # 月份相同的 计算医院数量最大的
        PHA_num_max = report.where(report.drop_for_months == 0) \
                    .groupby("标准通用名").agg(func.max("PHA_num").alias("max_PHA_num"))
        report = report.join(PHA_num_max, on="标准通用名", how="left")
        
        report = report.withColumn("drop_for_PHA", func.when(report.PHA_num == report.max_PHA_num, func.lit(0)).otherwise(func.lit(1)))
        report = report.withColumn("drop_for_PHA", func.when(report.drop_for_months == 0, report.drop_for_PHA).otherwise(None))
        
        # 项目得分
        score_max = report.where(report.drop_for_PHA == 0) \
                    .groupby("标准通用名").agg(func.min("project_score").alias("max_score"))
        report = report.join(score_max, on="标准通用名", how="left")
        report = report.withColumn("drop_for_score", func.when(report.project_score == report.max_score, func.lit(0)).otherwise(func.lit(1)))
        report = report.withColumn("drop_for_score", func.when(report.drop_for_PHA == 0, report.drop_for_score).otherwise(None))
    elif data_type == 'max':
        # 项目得分
        score_max = report.where(report.drop_for_months == 0) \
                    .groupby("标准通用名").agg(func.min("project_score").alias("max_score"))
        report = report.join(score_max, on="标准通用名", how="left")
        report = report.withColumn("drop_for_score", func.when(report.project_score == report.max_score, func.lit(0)).otherwise(func.lit(1)))
        report = report.withColumn("drop_for_score", func.when(report.drop_for_months == 0, report.drop_for_score).otherwise(None))
    
    # 时间范围range，最小月-最大月
    time_range = max_filter_list.select("project","标准通用名", "ATC", "Date") \
                    .distinct() \
                    .groupby(["project","标准通用名", "ATC"]).agg(func.min("Date").alias("min_time"), func.max("Date").alias("max_time"))
    time_range = time_range.withColumn("time_range", func.concat(time_range.min_time, func.lit("_"), time_range.max_time))
    
    # report_a生成
    # report_a.withColumn("time_range", func.lit(str(time_left) + '_' + str(time_right)))
    report_a = report.drop("max_score", "max_month") \
                .join(time_range.drop("min_time", "max_time"), on=["project","标准通用名", "ATC"], how="left")
    # 列名顺序调整
    if data_type == 'raw':
        report_a = report_a.select("project", "ATC", "标准通用名", 'Source', "time_range", "months_num", "drop_for_months", 
                                    "PHA_num", "drop_for_PHA", "project_score", "drop_for_score")
        report_a = report_a.orderBy(["标准通用名", "months_num", "PHA_num", "project_score"], ascending=[0, 0, 0, 1])
    else:
        report_a = report_a.select("project", "ATC", "标准通用名", "time_range", "months_num", "drop_for_months", "project_score", "drop_for_score")
        report_a = report_a.orderBy(["标准通用名", "months_num", "project_score"], ascending=[0, 0, 1])               
    
    report_a = report_a.withColumn("flag", func.when(report_a.drop_for_score == 0, func.lit(1)).otherwise(func.lit(None)))
    # 输出report_a
    report_a = report_a.withColumnRenamed('标准通用名', 'molecule') \
                        .withColumn('time_left', func.split(func.col("time_range"), "_").getItem(0)) \
                        .withColumn('time_right', func.split(func.col("time_range"), "_").getItem(1))
    
    report_a = report_a.repartition(1)
    report_a.write.format("csv").option("header", "true") \
        .mode("overwrite").save(report_a_path)
    
    report_a_path = extract_path + "/all_report_a"    
    report_a = report_a.repartition(1)
    report_a.write.format("parquet") \
        .mode("overwrite").save(report_a_path)
