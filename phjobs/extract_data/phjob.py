# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
# from ph_logs.ph_logs import phlogger
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql import functions as func
import os
from pyspark.sql.functions import pandas_udf, PandasUDFType
import time

def execute(max_path, extract_path, out_path, out_suffix, extract_file, time_left, time_right, molecule, atc, 
project, doi, molecule_sep, data_type):
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
            
        
    # a. 输入
    if data_type != "max" and data_type != "raw":
        phlogger.error('wrong input: data_type, max or raw') 
        raise ValueError('wrong input: data_type, max or raw')
    
    if extract_file == "Empty":
        path_for_extract_path = extract_path + "/path_for_extract.csv"
    else:
        path_for_extract_path = extract_file
        
    if data_type == 'max':
        extract_path = extract_path + '/max_standard'
    elif data_type == 'raw':
        extract_path = extract_path + '/rawdata_standard'
    
    if out_suffix == "Empty":
        raise ValueError('out_suffix: missing')
        
    # 提数条件
    time_left = int(time_left)
    time_right = int(time_right)
    
    if molecule == "Empty":
        molecule = [] 
    else:
        if molecule_sep == "Empty": 
            molecule = molecule.replace(" ","").split(",")
        else:
            molecule = molecule.replace(" ","").split(molecule_sep)
    
    if atc == "Empty":
        atc = []
    else:
        atc = atc.replace(" ","").split(",")
    
    if len(set(len(i) for i in atc)) >1 :
        raise ValueError('atc length diff')
        
    if project == "Empty":
        project = []
    else:
        project = project.replace(" ","").split(",")
        
    if doi == "Empty":
        doi = []
    else:
        doi = doi.replace(" ","").split(",")
    
    # project_rank 文件
    project_rank_path =  max_path + "/Common_files/extract_data_files/project_rank.csv"
    
    # 满足率计算输入文件
    ims_mapping_path = max_path + "/Common_files/extract_data_files/ims_mapping_202007.csv"
    ims_sales_path = max_path + "/Common_files/extract_data_files/cn_IMS_Sales_Fdata_202007.csv"
    molecule_ACT_path = max_path  + "/Common_files/extract_data_files/product_map_all_ATC.csv"
    packID_ACT_map_path = max_path  + "/Common_files/extract_data_files/packID_ATC_map.csv"
    
    # b. 输出
    timenow = time.strftime("%Y-%m-%d", time.localtime()).replace("-", "_")
    if data_type == 'raw':
        outdir = "raw_out_" + timenow + "_" + out_suffix
    elif data_type == 'max':
        outdir = "out_" + timenow + "_" + out_suffix
    out_extract_data_path = out_path + "/" + outdir + "/out_" + timenow + "_" + out_suffix + '.csv'
    report_a_path = out_path + "/" + outdir + "/report_a.csv"
    report_b_path = out_path + "/" + outdir + "/report_ATC.csv"
    report_c_path = out_path + "/" + outdir + "/report_c.csv"
    report_d_path = out_path + "/" + outdir + "/report_molecule.csv"
    
    out_tmp_path = out_path + "/" + outdir + "/out_tmp"
    max_filter_raw_path = out_path + "/" + outdir + "/max_filter_raw_tmp"
    
    # ================ 数据执行 ==================
    '''
    raw 和 max 区别：
    1. raw：Sales，Units 不处理，就用原始的数据
    2. report A 去重依据：
        max：月份数-项目排名
        raw: 月份数-医院数-项目排名；增加了source来源统计
    3. 提数结果
        raw：增加了医院ID，原始医院名，PHA，PHA医院名
    '''
    
    # 一. 文件准备
    
    # 1. 满足率文件准备
    # 通用名中英文对照
    molecule_ACT = spark.read.csv(molecule_ACT_path, header=True)
    packID_ACT_map = spark.read.csv(packID_ACT_map_path, header=True)
    molecule_ACT = molecule_ACT.select("MOLE_NAME_EN", "MOLE_NAME_CH").distinct()
    packID_ACT_map = packID_ACT_map.select("MOLE_NAME_EN", "MOLE_NAME_CH").distinct()
    molecule_name_map = molecule_ACT.union(packID_ACT_map).distinct()
    
    # ims mapping:ATC - Molecule - Pack_Id
    ims_mapping = spark.read.csv(ims_mapping_path, header=True)
    ims_mapping = ims_mapping.select("Pack_Id0", "ATC4_Code", "Molecule_Composition").distinct() \
                        .withColumn("Pack_Id0", ims_mapping.Pack_Id0.cast(IntegerType())) \
                        .withColumnRenamed("Pack_Id0", "PACK_ID") \
                        .withColumnRenamed("ATC4_Code", "ATC") \
                        .withColumnRenamed("Molecule_Composition", "MOLE_NAME_EN")
    
    # 2019年全国的ims销售数据
    ims_sales = spark.read.csv(ims_sales_path, header=True)
    ims_sales = ims_sales.where(func.substring(ims_sales.Period_Code, 0, 4) == '2019') \
                        .where(ims_sales.Geography_id == 'CHT') \
                        .groupby("Pack_ID").agg(func.sum("LC").alias("Sales_ims")) \
                        .withColumn("Pack_ID", ims_sales.Pack_ID.cast(IntegerType())) \
                        .withColumnRenamed("Pack_ID", "PACK_ID")
    
    ims_sales = ims_sales.join(ims_mapping, on="Pack_ID", how="left")
    ims_sales = ims_sales.join(molecule_name_map, on="MOLE_NAME_EN", how="left").distinct()
    
    if atc and max([len(i) for i in atc]) == 3:
        ims_sales = ims_sales.withColumn("ATC", func.substring(ims_sales.ATC, 0, 3)).distinct()
    elif atc and max([len(i) for i in atc]) == 4:
        ims_sales = ims_sales.withColumn("ATC", func.substring(ims_sales.ATC, 0, 4)).distinct()
        
    # 2. 项目排名文件
    project_rank = spark.read.csv(project_rank_path, header=True)
    project_rank = project_rank.withColumnRenamed("项目", "project") \
                        .withColumn("排名", project_rank["排名"].cast(IntegerType())) \
                        .withColumnRenamed("排名", "project_score")
    
    # 二. 根据 max_standard_brief_all 确定最终提数来源
        
    # 1. 根据path_for_extract， 合并项目brief，生成max_standard_brief_all
    path_for_extract = spark.read.csv(path_for_extract_path, header=True)
    # 如果指定了project，那么只读取该项目的brief文件即可        
    if project:
        project_all = project
    else:
        project_all = path_for_extract.toPandas()["project"].tolist()
        
    if data_type == 'raw':
        path_all_brief = [extract_path + '/' + i + "_rawdata_standard_brief" for i in project_all]
    elif data_type == 'max':
        path_all_brief = [extract_path + '/' + i + "_max_standard_brief" for i in project_all]
        
    # "project", "Date", "标准通用名", "ATC", "DOI"  ("PHA", "Source")
    index = 0
    for eachpath in path_all_brief:
        df = spark.read.parquet(eachpath)
        if 'PACK_ID' not in df.columns:
            df = df.withColumn('PACK_ID', func.lit(0))
        if index ==0:
            max_standard_brief_all = df
        else:
            max_standard_brief_all = max_standard_brief_all.union(df)
        index += 1    
    
    # 2. 根据提数需求获取 max_filter_path_month
    # 筛选,获取符合条件的项目和月份
    max_filter_list = max_standard_brief_all.where((max_standard_brief_all.Date >= time_left) & (max_standard_brief_all.Date <= time_right))
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
    report_a = report_a.repartition(1)
    report_a.write.format("csv").option("header", "true") \
        .mode("overwrite").save(report_a_path)
    # 重新读入，否则当数据量大的时候后面的join report_a 报错
    report_a = spark.read.csv(report_a_path, header=True)
    
    # 根据 report_a 去重
    if atc:
        max_filter_list = max_filter_list.join(report_a.where(report_a.flag == 1).select("ATC", "标准通用名", "project").distinct(), 
                                    on=["ATC", "标准通用名", "project"], 
                                    how="inner").persist()
    else:
        max_filter_list = max_filter_list.join(report_a.where(report_a.flag == 1).select("标准通用名", "project").distinct(), 
                                    on=["标准通用名", "project"], 
                                    how="inner").persist()
    
    # 三. 原始数据提取
    
    project_Date_list = max_filter_list.select("project", "Date").distinct()
    
    # 获取要读取的文件路径
    if data_type == 'raw':
        max_filter_path = project_Date_list.withColumn('path', func.concat(func.lit(extract_path + '/'), 
                                                        project_Date_list.project, func.lit("_rawdata_standard")))
    elif data_type == 'max':
        max_filter_path = project_Date_list.withColumn('path', func.concat(func.lit(extract_path + '/'), 
                                                        project_Date_list.project, func.lit("_max_standard")))
                                                        
    # max_filter_path = project_Date_list.join(path_for_extract, on="project", how="left")
    max_filter_path = max_filter_path.withColumn("path_month", func.concat(max_filter_path.path, func.lit("/Date_copy="), max_filter_path.Date))
    max_filter_path_month = max_filter_path.select("path_month").distinct().toPandas()["path_month"].tolist()
    
    # 3. 根据 max_filter_path_month 汇总max结果        
    index = 0
    for eachpath in max_filter_path_month:
        df = spark.read.parquet(eachpath)
        # 过滤数据
        if doi:
            df = df.where(df.DOI.isin(doi))
        if atc:
            if max([len(i) for i in atc]) == 3:
                df = df.withColumn("ATC", func.substring(df.ATC, 0, 3)).distinct()
            elif max([len(i) for i in atc]) == 4:
                df = df.withColumn("ATC", func.substring(df.ATC, 0, 4)).distinct()
            df = df.where(df.ATC.isin(atc))
        if molecule:
            df = df.where(df['标准通用名'].isin(molecule))
        
        # 汇总
        if index ==0:
            # max_filter_raw = df
            df = df.repartition(1)
            df.write.format("parquet") \
                .mode("overwrite").save(max_filter_raw_path)
        else:
            # max_filter_raw = max_filter_raw.union(df)
            df = df.repartition(1)
            df.write.format("parquet") \
                .mode("append").save(max_filter_raw_path)
        index += 1
    
    # max_filter_raw
    max_filter_raw = spark.read.parquet(max_filter_raw_path)
    
    # 4. 注释项目排名
    max_filter_out = max_filter_raw.join(project_rank, on="project", how="left").persist()
    
    # 5. 原始提数结果
    if data_type == 'max':
        out_cols = ["project", "project_score", "Date", "ATC", "标准通用名", "标准商品名", "标准剂型", "标准规格", 
                "标准包装数量", "标准生产企业", "标准省份名称", "标准城市名称", "DOI", "PACK_ID", "Predict_Sales", "Predict_Unit", "PANEL"]
    elif data_type == 'raw':
        out_cols = ["project", "project_score", "ID", "Raw_Hosp_Name", "PHA", "PHA医院名称" ,"Date", "ATC", "标准通用名", 
                    "标准商品名", "标准剂型", "标准规格", "标准包装数量", "标准生产企业", "标准省份名称", "标准城市名称", 
                    "DOI", "PACK_ID", "Sales", "Units", "Units_Box"]
    max_filter_out = max_filter_out.select(out_cols).distinct()
                    
    if data_type == 'max':
        # Sales，Units 处理
        '''
        包装数量为空的是others， Sales 或者 Units 可以为0
        包装数量不为空的，Sales和Units只要有一列为0，那么都调整为0；Units先四舍五入为整数，然后变化的系数乘以Sales获得新的Sales
        Sales 保留两位小数
        负值调整为0
        去掉 Sales，Units 同时为0的行
        '''
        max_filter_out = max_filter_out.withColumn("Predict_Sales", max_filter_out["Predict_Sales"].cast(DoubleType())) \
                                .withColumn("Predict_Unit", max_filter_out["Predict_Unit"].cast(DoubleType()))
        
        max_filter_out = max_filter_out.withColumn("Units", func.when((~max_filter_out["标准包装数量"].isNull()) & (max_filter_out.Predict_Unit <= 0), func.lit(0)) \
                                                                        .otherwise(func.round(max_filter_out.Predict_Unit, 0)))
                                            
        max_filter_out = max_filter_out.withColumn("p", max_filter_out.Units/max_filter_out.Predict_Unit)
        max_filter_out = max_filter_out.withColumn("p", func.when((~max_filter_out["标准包装数量"].isNull()) & (max_filter_out["p"].isNull()), func.lit(0)) \
                                                            .otherwise(max_filter_out.p))
        max_filter_out = max_filter_out.withColumn("p", func.when((max_filter_out["标准包装数量"].isNull()) & (max_filter_out["p"].isNull()), func.lit(1)) \
                                                            .otherwise(max_filter_out.p))
        
        max_filter_out = max_filter_out.withColumn("Sales", max_filter_out.Predict_Sales * max_filter_out.p)
        
        max_filter_out = max_filter_out.withColumn("Sales", func.round(max_filter_out.Sales, 2)) \
                                    .withColumn("Units", max_filter_out["Units"].cast(IntegerType())) \
                                    .drop("Predict_Unit", "Predict_Sales", "p")
        
        # 负值调整为0
        max_filter_out = max_filter_out.withColumn("Sales", func.when(max_filter_out.Sales < 0 , func.lit(0)).otherwise(max_filter_out.Sales))
        max_filter_out = max_filter_out.withColumn("Units", func.when(max_filter_out.Sales == 0, func.lit(0)).otherwise(max_filter_out.Units))

        # 去掉 Sales，Units 同时为0的行
        max_filter_out_1 = max_filter_out.where(max_filter_out["标准包装数量"].isNull())
        max_filter_out_2 = max_filter_out.where((~max_filter_out["标准包装数量"].isNull()) & (max_filter_out.Sales != 0) & (max_filter_out.Units != 0))
        
        max_filter_out =  max_filter_out_1.union(max_filter_out_2)   
    
    # 根据 report_a 去重
    if atc:
        out_extract_data = max_filter_out.join(report_a.where(report_a.flag == 1).select("ATC", "标准通用名", "project").distinct(), 
                                    on=["ATC", "标准通用名", "project"], 
                                    how="inner").persist()
    else:
        out_extract_data = max_filter_out.join(report_a.where(report_a.flag == 1).select("标准通用名", "project").distinct(), 
                                    on=["标准通用名", "project"], 
                                    how="inner").persist()
    
    # 输出提数结果
    if data_type == 'max':
        out_cols = ["project", "project_score", "Date", "ATC", "标准通用名", "标准商品名", "标准剂型", "标准规格", 
                    "标准包装数量", "标准生产企业", "标准省份名称", "标准城市名称", "PACK_ID", "Sales", "Units", "PANEL"]
    elif data_type == 'raw':
        out_cols = ["project", "project_score", "ID", "Raw_Hosp_Name", "PHA", "PHA医院名称" ,"Date", "ATC", "标准通用名", "标准商品名", "标准剂型", "标准规格", "标准包装数量", "标准生产企业", "标准省份名称", "标准城市名称", "PACK_ID", "Sales", "Units", "Units_Box"]
                    
    out_extract_data_final = out_extract_data.select(out_cols).distinct()
    out_extract_data_final = out_extract_data_final.repartition(1)
    out_extract_data_final.write.format("csv").option("header", "true") \
        .mode("overwrite").save(out_extract_data_path)
    
    # 缓解存储
    # out_extract_data = out_extract_data.repartition(4)
    # out_extract_data.write.format("parquet") \
    #          .mode("overwrite").save(out_tmp_path)
    # out_extract_data = spark.read.parquet(out_tmp_path)
        
    # report_c
    extract_sales = out_extract_data.select("project", "PACK_ID", "ATC", "标准通用名").distinct() \
                            .join(ims_sales.select("PACK_ID", "Sales_ims").distinct(), on="PACK_ID", how="left") \
                            .groupby("project", "ATC", "标准通用名").agg(func.sum("Sales_ims").alias("Sales_ims_extract")).persist()
    
    molecule_names = out_extract_data.select("标准通用名").distinct().toPandas()["标准通用名"].values.tolist()
    
    molecule_sales = ims_sales.select("PACK_ID", "ATC", "MOLE_NAME_CH", "Sales_ims").distinct() \
                        .where(ims_sales.MOLE_NAME_CH.isin(molecule_names)) \
                        .groupby("ATC", "MOLE_NAME_CH").agg(func.sum("Sales_ims").alias("Sales_ims_molecule")) \
                        .withColumnRenamed("MOLE_NAME_CH", "标准通用名").persist()
                        
    report_c = extract_sales.join(molecule_sales, on=["标准通用名", "ATC"], how="left")
    report_c = report_c.withColumn("Sales_rate", report_c.Sales_ims_extract/report_c.Sales_ims_molecule) \
                    .join(report_a.where(report_a.flag == 1).select("标准通用名", "ATC", "project", "months_num", "time_range"), 
                            on=["标准通用名", "ATC", "project"], how="left") \
                    .drop("Sales_ims_extract", "Sales_ims_molecule").persist()
    # 列名顺序调整
    report_c = report_c.select("project", "ATC", "标准通用名", "time_range", "months_num", "Sales_rate") \
                        .orderBy(["标准通用名"]) 
    
    report_c = report_c.repartition(1)
    report_c.write.format("csv").option("header", "true") \
        .mode("overwrite").save(report_c_path)
    
    # report_atc
    if atc:
        time_range_act = out_extract_data.select("ATC", "Date").distinct() \
                            .groupby(["ATC"]).agg(func.min("Date").alias("min_time"), func.max("Date").alias("max_time"))
        time_range_act = time_range_act.withColumn("time_range", func.concat(time_range_act.min_time, func.lit("_"), time_range_act.max_time))
        
        extract_sales = out_extract_data.select("PACK_ID", "ATC").distinct() \
                            .join(ims_sales.select("PACK_ID", "Sales_ims").distinct(), on="PACK_ID", how="left") \
                            .groupby("ATC").agg(func.sum("Sales_ims").alias("Sales_ims_extract")).persist()
        atc_sales = ims_sales.select("PACK_ID", "ATC", "Sales_ims").distinct() \
                        .where(ims_sales.ATC.isin(atc)) \
                        .groupby("ATC").agg(func.sum("Sales_ims").alias("Sales_ims_atc")).persist()
        report_b = extract_sales.join(atc_sales, on="ATC", how="left")
        report_b = report_b.withColumn("Sales_rate", report_b.Sales_ims_extract/report_b.Sales_ims_atc) \
                        .join(time_range_act.select("ATC", "time_range"), on="ATC", how="left") \
                        .drop("Sales_ims_extract", "Sales_ims_atc")
        # 列名顺序调整
        report_b = report_b.select("ATC", "time_range", "Sales_rate")
        
        report_b = report_b.repartition(1)
        report_b.write.format("csv").option("header", "true") \
            .mode("overwrite").save(report_b_path)
    
    # report_molecule        
    if molecule:
        time_range = out_extract_data.select("标准通用名", "Date").distinct() \
                            .groupby(["标准通用名"]).agg(func.min("Date").alias("min_time"), func.max("Date").alias("max_time"))
        time_range = time_range.withColumn("time_range", func.concat(time_range.min_time, func.lit("_"), time_range.max_time))
        
        extract_sales = out_extract_data.select("PACK_ID", "标准通用名").distinct() \
                            .join(ims_sales.select("PACK_ID", "Sales_ims").distinct(), on="PACK_ID", how="left") \
                            .groupby("标准通用名").agg(func.sum("Sales_ims").alias("Sales_ims_extract")).persist()
        molecule_sales = ims_sales.select("PACK_ID", "MOLE_NAME_CH", "Sales_ims").distinct() \
                        .where(ims_sales.MOLE_NAME_CH.isin(molecule)) \
                        .groupby("MOLE_NAME_CH").agg(func.sum("Sales_ims").alias("Sales_ims_molecule")).persist()
        report_d = extract_sales.join(molecule_sales, extract_sales['标准通用名']==molecule_sales['MOLE_NAME_CH'], how="left")
        report_d = report_d.withColumn("Sales_rate", report_d.Sales_ims_extract/report_d.Sales_ims_molecule) \
                        .join(time_range.select("标准通用名", "time_range"), on="标准通用名", how="left") \
                        .drop("Sales_ims_extract", "Sales_ims_molecule", "MOLE_NAME_CH")
        # 列名顺序调整
        report_d = report_d.select("标准通用名", "time_range", "Sales_rate")
        
        report_d = report_d.repartition(1)
        report_d.write.format("csv").option("header", "true") \
            .mode("overwrite").save(report_d_path)
