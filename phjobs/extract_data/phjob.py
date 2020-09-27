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

#def execute(max_path, out_path, out_suffix, extract_file, time_left, time_right, molecule, atc, project, doi):
#    os.environ["PYSPARK_PYTHON"] = "python3"
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
extract_file = "Empty"
time_left = 201610
time_right = 202002
molecule = "米格列奈, 那格列奈, 瑞格列奈"
#molecule = "多西他赛, 曲司氯铵"
atc = "A10M1"
atc = "Empty"
project = "AZ"
#doi = "Empty"
doi="SNY1"
out_suffix = "test_doi_Molecule_project"
'''

extract_file = "Empty"
'''
# 方案1
time_left = 201801
time_right = 201912
molecule = "米格列奈, 那格列奈, 瑞格列奈, 多西他赛, 曲司氯铵"
atc = "Empty"
doi = "Empty"
project = "Empty"
out_suffix = "test1_molecule"

# 方案2
time_left = 201801
time_right = 201912
molecule = "Empty"
atc = "A10M1, L01C2"
doi = "Empty"
project = "Empty"
out_suffix = "test2_atc"
'''

# 方案3
time_left = 201801
time_right = 201912
molecule = "二甲双胍, 格列喹酮"
atc = "Empty"
doi = "Empty"
project = "Servier"
out_suffix = "test3_project_molecule"

# 输入文件
max_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/"
extrac_path = "s3a://ph-stream/common/public/max_result/0.0.5/"
out_path = "s3a://ph-stream/common/public/max_result/0.0.5/extract_data_out"
project_rank_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/Common_files/extract_data_files/project_rank.csv"

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
report_b_path = out_path + "/" + outdir + "/report_b.csv"
report_c_path = out_path + "/" + outdir + "/report_c.csv"

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

# 0. 满足率文件准备
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


# 1. 根据提数需求获取 max_filter_path_month
path_for_extract = spark.read.csv(path_for_extract_path, header=True)
max_standard_brief_all = spark.read.parquet(max_standard_brief_all_path)

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

# 获取要读取的文件路径        
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
project_rank = spark.read.csv(project_rank_path, header=True)
project_rank = project_rank.withColumnRenamed("项目", "project") \
                    .withColumn("排名", project_rank["排名"].cast(IntegerType())) \
                    .withColumnRenamed("排名", "project_score")
                    
max_filter_out = max_filter_raw.join(project_rank, on="project", how="left")


# 4. 原始提数结果
max_filter_out = max_filter_out.select("project", "project_score", "Date", "ATC", "标准通用名", "标准商品名", "标准剂型", "标准规格", 
                "标准包装数量", "标准生产企业", "标准省份名称", "标准城市名称", "DOI", "Predict_Sales", "Predict_Unit", "PACK_ID")
max_filter_out = max_filter_out.withColumnRenamed("Predict_Sales", "Sales") \
                        .withColumnRenamed("Predict_Unit", "Units")
                        
# 5. 提数报告以及提数去重
report = max_filter_out.select("project","project_score","标准通用名", "ATC", "Date") \
                        .distinct() \
                        .groupby(["标准通用名", "ATC", "project","project_score"]).count() \
                        .withColumnRenamed("count", "months_num") \
                        .persist()

# 分子最大月份数, 月份最全-得分
months_max = report.groupby("标准通用名").agg(func.max("months_num").alias("max_month"))

report = report.join(months_max, on="标准通用名", how="left")
report = report.withColumn("drop_for_months", func.when(report.months_num == report.max_month, func.lit(0)).otherwise(func.lit(1)))

score_max = report.where(report.drop_for_months == 0) \
            .groupby("标准通用名").agg(func.min("project_score").alias("max_score"))
            
report = report.join(score_max, on="标准通用名", how="left")
                
report = report.withColumn("drop_for_score", func.when(report.project_score == report.max_score, func.lit(0)).otherwise(func.lit(1)))
report = report.withColumn("drop_for_score", func.when(report.drop_for_months == 0, report.drop_for_score).otherwise(None))

report_a = report.drop("max_score", "max_month") \
            .withColumn("time_range", func.lit(str(time_left) + '_' + str(time_right)))
# 列名顺序调整
report_a = report_a.select("project", "ATC", "标准通用名", "time_range", "months_num", "drop_for_months", "project_score", "drop_for_score")

# 输出report_a            
report_a = report_a.repartition(1)
report_a.write.format("csv").option("header", "true") \
    .mode("overwrite").save(report_a_path)

# 根据report_a去重
out_extract_data = max_filter_out.join(report_a.where(report_a.drop_for_score == 0).select("标准通用名", "project"), 
                                    on=["标准通用名", "project"], 
                                    how="inner").persist()

# 输出提数结果                                    
out_extract_data_final = out_extract_data.select("project", "project_score", "Date", "ATC", "标准通用名", "标准商品名", "标准剂型", 
                                "标准规格", "标准包装数量", "标准生产企业", "标准省份名称", "标准城市名称", "Sales", "Units")
out_extract_data_final = out_extract_data_final.repartition(1)
out_extract_data_final.write.format("csv").option("header", "true") \
    .mode("overwrite").save(out_extract_data_path)
    
# report_b
if atc:
    extract_sales = out_extract_data.select("PACK_ID", "ATC").distinct() \
                        .join(ims_sales.select("PACK_ID", "Sales_ims").distinct(), on="PACK_ID", how="left") \
                        .groupby("ATC").agg(func.sum("Sales_ims").alias("Sales_ims_extract")).persist()
    atc_sales = ims_sales.select("PACK_ID", "ATC", "Sales_ims").distinct() \
                    .where(ims_sales.ATC.isin(atc)) \
                    .groupby("ATC").agg(func.sum("Sales_ims").alias("Sales_ims_atc")).persist()
    report_b = extract_sales.join(atc_sales, on="ATC", how="left")
    report_b = report_b.withColumn("Sales_rate", report_b.Sales_ims_extract/report_b.Sales_ims_atc) \
                    .withColumn("time_range", func.lit(str(time_left) + '_' + str(time_right))) \
                    .drop("Sales_ims_extract", "Sales_ims_atc")
    # 列名顺序调整
    report_b = report_b.select("ATC", "time_range", "Sales_rate")
    
    report_b = report_b.repartition(1)
    report_b.write.format("csv").option("header", "true") \
        .mode("overwrite").save(report_b_path)


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
                .join(report_a.where(report_a.drop_for_score == 0).select("标准通用名", "ATC", "project", "months_num", "time_range"), 
                        on=["标准通用名", "ATC", "project"], how="left") \
                .drop("Sales_ims_extract", "Sales_ims_molecule").persist()
# 列名顺序调整
report_c = report_c.select("project", "ATC", "标准通用名", "time_range", "months_num", "Sales_rate")

report_c = report_c.repartition(1)
report_c.write.format("csv").option("header", "true") \
    .mode("overwrite").save(report_c_path)
        
