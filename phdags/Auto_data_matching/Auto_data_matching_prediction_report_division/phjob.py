# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from ph_logs.ph_logs import phs3logger
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import desc, udf, col
from pyspark.sql.functions import rank, lit, when, row_number
from pyspark.sql import Window
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.classification import MultilayerPerceptronClassificationModel
from pyspark.ml.classification import DecisionTreeClassificationModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import PipelineModel
from pyspark.ml.feature import VectorAssembler

def execute(**kwargs):
    """
        please input your code below
        get spark session: spark = kwargs["spark"]()
    """
    logger = phs3logger(kwargs["job_id"])
    spark = kwargs["spark"]()
    logger.info(kwargs)

    # input
    positive_result_path = kwargs["positive_result_path"]
    negative_result_path = kwargs["negative_result_path"]
    split_data_path = kwargs["split_data_path"]
    training_data_path = kwargs["training_data_path"]

    # output
    job_id = kwargs["job_id"]
    if not job_id:
        job_id = str(int(time.time()))
    true_positive_result_path = kwargs["true_positive_result_path"] + "/" + job_id
    true_negative_result_path = kwargs["true_negative_result_path"] + "/" + job_id
    false_positive_result_path = kwargs["false_positive_result_path"] + "/" + job_id
    false_negative_result_path = kwargs["false_negative_result_path"] + "/" + job_id
    true_positive_result_path_csv = kwargs["true_positive_result_path_csv"] + "/" + job_id
    true_negative_result_path_csv = kwargs["true_negative_result_path_csv"] + "/" + job_id
    false_positive_result_path_csv = kwargs["false_positive_result_path_csv"] + "/" + job_id
    false_negative_result_path_csv = kwargs["false_negative_result_path_csv"] + "/" + job_id
    lost_data_path = kwargs["lost_data_path"] + "/" + job_id
    final_report_path = kwargs["final_report_path"] + "/" + job_id
    mnf_check_path = kwargs["mnf_check_path"] + "/" + job_id
    spec_check_path = kwargs["spec_check_path"] + "/" + job_id
    dosage_check_path = kwargs["dosage_check_path"] + "/" + job_id

    # 1. load the data
    df_all = spark.read.parquet(split_data_path)  # 带id的所有数据
    df_result = spark.read.parquet(training_data_path)  # 进入清洗流程的所有数据
    ph_total = df_result.groupBy("id").agg({"label": "first"}).count()
    resultid = df_result.select("id").distinct()
    resultid_lst = resultid.toPandas()["id"].tolist()
    df_lost = df_all.where(~df_all.id.isin(resultid_lst))  # 第一步就丢失了的数据
    # df_lost.write.mode("overwrite").parquet(lost_data_path)
    # logger.warn("匹配第一步丢失条目写入完成")

    # 2. load predictions(positive and negative)
    df_positive = spark.read.parquet(positive_result_path)  # pre=1&rank=1 机器认为一定正确
    df_negative = spark.read.parquet(negative_result_path)  # pre=0 | pre=1&rank>1
    
    # 3. 拆成四个文件
    df_true_positive = df_positive.where(df_positive.label == 1)
    df_false_positive = df_positive.where(df_positive.label == 0)
    df_true_negative = df_negative.where(df_negative.label == 0)
    df_false_negative = df_negative.where(df_negative.label == 1)  # pre=1 label=0, 重点关注的部分
    
    # 四个parquet文件&四个csv文件分别写入s3
    df_true_positive.write.mode("overwrite").parquet(true_positive_result_path)
    join_and_write(df_true_positive, true_positive_result_path_csv)
    
    df_false_positive.write.mode("overwrite").parquet(false_positive_result_path)
    join_and_write(df_false_positive, false_positive_result_path_csv)
    
    df_true_negative.write.mode("overwrite").parquet(true_negative_result_path)
    join_and_write(df_true_negative, true_negative_result_path_csv)
    
    df_false_negative.write.mode("overwrite").parquet(false_negative_result_path)
    join_and_write(df_false_negative, false_negative_result_path_csv)
    
    logger.warn("八个文件写入完成")
    
    # 4. 错误原因输出
    df_false_negative.show(1)
    print(df_false_negative.count())
    df_false_negative.printSchema()
    
    # 4.1 生产厂商
    eff_mnf_needed = 0.836
    df_mnf_check = df_false_negative.where(df_false_negative.EFFTIVENESS_MANUFACTURER <= eff_mnf_needed)
    df_mnf_check = df_mnf_check.select("id", "EFFTIVENESS_MANUFACTURER", "COSINE_SIMILARITY", "EFFTIVENESS_MANUFACTURER_FIRST", "MANUFACTURER_NAME_ORIGINAL", "MANUFACTURER_NAME", \
                                        "MANUFACTURER_NAME_CLEANNING_WORDS_SEG", "CORP_NAME_STANDARD", "MANUFACTURER_NAME_STANDARD", "MANUFACTURER_NAME_STANDARD_WORDS_SEG")
    df_mnf_check = df_mnf_check.withColumn("EFFTIVENESS_MANUFACTURER_NEEDED", lit(eff_mnf_needed))
    df_mnf_check.show()
    df_mnf_check.write.mode("overwrite").parquet(mnf_check_path)
    
    # 4.2 spec
    eff_spec_needed = 0.985
    df_spec_check = df_false_negative.where(df_false_negative.EFFTIVENESS_SPEC <= eff_spec_needed)
    df_spec_check = df_spec_check.select("id", "EFFTIVENESS_SPEC", "EFFTIVENESS_SPEC_SPLIT", "EFFTIVENESS_SPEC_FIRST", "SPEC_ORIGINAL", "SPEC", \
                                        "SPEC_STANDARD", "SPEC_STANDARD_ORIGINAL", "SPEC_valid_digit_STANDARD", "SPEC_valid_unit_STANDARD", "SPEC_gross_digit_STANDARD", \
                                        "SPEC_gross_unit_STANDARD")
    df_spec_check = df_spec_check.withColumn("EFFTIVENESS_MANUFACTURER_NEEDED", lit(eff_spec_needed))
    df_spec_check.show()
    df_spec_check.write.mode("overwrite").parquet(spec_check_path)

    # 4.2 dosage
    # eff_dosage_needed = 0.5375
    # df_dosage_check = df_false_negative.where(df_false_negative.EFFTIVENESS_DOSAGE <= eff_dosage_needed)
    # df_dosage_check = df_dosage_check.select("id", "EFFTIVENESS_SPEC", "EFFTIVENESS_SPEC_SPLIT", "EFFTIVENESS_SPEC_FIRST", "SPEC_ORIGINAL", "SPEC", \
    #                                     "SPEC_STANDARD", "SPEC_STANDARD_ORIGINAL", "SPEC_valid_digit_STANDARD", "SPEC_valid_unit_STANDARD", "SPEC_gross_digit_STANDARD", \
    #                                     "SPEC_gross_unit_STANDARD")
    # df_dosage_check = df_dosage_check.withColumn("EFFTIVENESS_MANUFACTURER_NEEDED", lit(eff_dosage_needed))
    # df_dosage_check.show()
    # df_dosage_check.write.mode("overwrite").parquet(dosage_check_path)
    
    # 5. 结果统计
    all_count = df_all.count()  # 数据总数
    positive_count = df_positive.count()  # 机器判断正确条目
    true_positive_count = df_true_positive.count()  # 其中正确条目
    matching_ratio = positive_count / all_count  # 匹配率
    if positive_count != 0:
        precision = true_positive_count / positive_count  # 正确率
    else:
        precision = 0
    
    # 6. 最终结果报告以csv形式写入s3
    report = [("data_matching_report", ),]
    report_schema = StructType([StructField('title',StringType(),True),])
    df_report = spark.createDataFrame(report, schema=report_schema).na.fill("")
    df_report.show()
    df_report = df_report.withColumn("数据总数", lit(str(all_count)))
    df_report = df_report.withColumn("进入匹配流程条目", lit(str(ph_total)))
    df_report = df_report.withColumn("丢失条目", lit(str(df_lost.count())))
    df_report = df_report.withColumn("机器判断正确条目", lit(str(positive_count)))
    df_report = df_report.withColumn("其中正确条目", lit(str(true_positive_count)))
    df_report = df_report.withColumn("匹配率", lit(str(matching_ratio)))
    df_report = df_report.withColumn("正确率", lit(str(precision)))
    df_report.show()
    df_report.repartition(1).write.mode("overwrite").option("header", "true").csv(final_report_path)
    logger.warn("final report csv文件写入完成")

    return {}

def similarity(df):
	df = df.withColumn("SIMILARITY", \
					(df.EFFTIVENESS_MOLE_NAME + df.EFFTIVENESS_PRODUCT_NAME + df.EFFTIVENESS_DOSAGE \
						+ df.EFFTIVENESS_SPEC + df.EFFTIVENESS_PACK_QTY + df.EFFTIVENESS_MANUFACTURER))
	windowSpec = Window.partitionBy("id").orderBy(desc("SIMILARITY"), desc("EFFTIVENESS_MOLE_NAME"), desc("EFFTIVENESS_DOSAGE"), desc("PACK_ID_STANDARD"))
	df = df.withColumn("RANK", row_number().over(windowSpec))
	df = df.where((df.RANK <= 5) | (df.label == 1.0))
	return df


def join_and_write(df, csv_path):
    join_udf = udf(lambda x: ",".join(map(str,x)))
    df = df.withColumn("MASTER_DOSAGE", join_udf(col("MASTER_DOSAGE"))) \
                            .withColumn("MANUFACTURER_NAME_STANDARD_WORDS", join_udf(col("MANUFACTURER_NAME_STANDARD_WORDS"))) \
                            .withColumn("MANUFACTURER_NAME_CLEANNING_WORDS", join_udf(col("MANUFACTURER_NAME_CLEANNING_WORDS"))) \
                            .withColumn("MANUFACTURER_NAME_STANDARD_WORDS_SEG", join_udf(col("MANUFACTURER_NAME_STANDARD_WORDS_SEG"))) \
                            .withColumn("MANUFACTURER_NAME_CLEANNING_WORDS_SEG", join_udf(col("MANUFACTURER_NAME_CLEANNING_WORDS_SEG"))) \
                            .withColumn("features", join_udf(col("features")))
    df.repartition(1).write.mode("overwrite").option("header", "true").csv(csv_path)