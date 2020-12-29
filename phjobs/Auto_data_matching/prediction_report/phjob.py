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
    training_data_path = kwargs["training_data_path"]
    predictions_path = kwargs["predictions_path"]
    split_data_path = kwargs["split_data_path"]

    # output
    job_id = kwargs["job_id"]
    if not job_id:
        job_id = str(int(time.time()))
    positive_result_path = kwargs["positive_result_path"] + "/" + job_id
    negative_result_path = kwargs["negative_result_path"] + "/" + job_id
    lost_data_path = kwargs["lost_data_path"] + "/" + job_id
    positive_result_path_csv = kwargs["positive_result_path_csv"] + "/" + job_id
    negative_result_path_csv = kwargs["negative_result_path_csv"] + "/" + job_id
    lost_data_path_csv = kwargs["lost_data_path_csv"] + "/" + job_id
    
    # 1. load the data
    df_result = spark.read.parquet(training_data_path)  # 进入清洗流程的所有数据
    df_validate = df_result
    df_all = spark.read.parquet(split_data_path)  # 带id的所有数据
    resultid = df_result.select("id").distinct()
    resultid_lst = resultid.toPandas()["id"].tolist()
    df_lost = df_all.where(~df_all.id.isin(resultid_lst))  # 第一步就丢失了的数据

    # 2. load predictions
    predictions = spark.read.parquet(predictions_path)

    # 3. compute accuracy on the test set
    evaluator = MulticlassClassificationEvaluator(labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    logger.warn("Test Error = %g " % (1.0 - accuracy))
    logger.warn("Test set accuracy = " + str(accuracy))

    # 4. Test with Pharbers defined methods
    result = predictions
    result_similarity = similarity(result)
    # result_similarity.write.mode("overwrite").parquet("s3a://ph-max-auto/2020-08-11/BPBatchDAG/refactor/zyyin/eia/0.0.2/for_analysis")
    # print("用于分析的的条目写入完成")
    result = result.withColumn("JACCARD_DISTANCE_MOLE_NAME", result.JACCARD_DISTANCE[0]) \
                .withColumn("JACCARD_DISTANCE_DOSAGE", result.JACCARD_DISTANCE[1]) \
                .drop("JACCARD_DISTANCE", "indexedFeatures").drop("rawPrediction", "probability")
    ph_total = result.groupBy("id").agg({"prediction": "first", "label": "first"}).count()
    # logger.warn("丢失条目： " + str(df_lost.count()))
    
    # df_report.show()

    # 5. 生成文件
    join_udf = udf(lambda x: ",".join(map(str,x)))
    result = similarity(result)
    df_positive = result.where((result.prediction == 1.0) & (result.RANK == 1))
    df_positive.write.mode("overwrite").parquet(positive_result_path)   # TODO 记得打开
    df_positive = df_positive.withColumn("MASTER_DOSAGE", join_udf(col("MASTER_DOSAGE"))) \
                            .withColumn("MANUFACTURER_NAME_STANDARD_WORDS", join_udf(col("MANUFACTURER_NAME_STANDARD_WORDS"))) \
                            .withColumn("MANUFACTURER_NAME_CLEANNING_WORDS", join_udf(col("MANUFACTURER_NAME_CLEANNING_WORDS"))) \
                            .withColumn("MANUFACTURER_NAME_STANDARD_WORDS_SEG", join_udf(col("MANUFACTURER_NAME_STANDARD_WORDS_SEG"))) \
                            .withColumn("MANUFACTURER_NAME_CLEANNING_WORDS_SEG", join_udf(col("MANUFACTURER_NAME_CLEANNING_WORDS_SEG"))) \
                            .withColumn("features", join_udf(col("features")))
    df_positive.repartition(1).write.mode("overwrite").option("header", "true").csv(positive_result_path_csv)
    logger.warn("机器判断positive的条目写入完成")
    
    df_negative = result.where((result.prediction == 0.0) | ((result.prediction == 1.0) & (result.RANK != 1)))
    df_negative.write.mode("overwrite").parquet(negative_result_path)
    df_negative = df_negative.withColumn("MASTER_DOSAGE", join_udf(col("MASTER_DOSAGE"))) \
                            .withColumn("MANUFACTURER_NAME_STANDARD_WORDS", join_udf(col("MANUFACTURER_NAME_STANDARD_WORDS"))) \
                            .withColumn("MANUFACTURER_NAME_CLEANNING_WORDS", join_udf(col("MANUFACTURER_NAME_CLEANNING_WORDS"))) \
                            .withColumn("MANUFACTURER_NAME_STANDARD_WORDS_SEG", join_udf(col("MANUFACTURER_NAME_STANDARD_WORDS_SEG"))) \
                            .withColumn("MANUFACTURER_NAME_CLEANNING_WORDS_SEG", join_udf(col("MANUFACTURER_NAME_CLEANNING_WORDS_SEG"))) \
                            .withColumn("features", join_udf(col("features")))
    df_negative.repartition(1).write.mode("overwrite").option("header", "true").csv(negative_result_path_csv)
    logger.warn("机器判断negative的条目写入完成")
    
    df_lost.write.mode("overwrite").parquet(lost_data_path)
    df_lost.repartition(1).write.mode("overwrite").option("header", "true").csv(lost_data_path_csv)
    logger.warn("匹配第一步丢失条目写入完成")
    
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
	# df.repartition(1).write.format("parquet").mode("overwrite").save("s3a://ph-max-auto/2020-08-11/BPBatchDAG/refactor/zyyin/qilu/0.0.3/result_analyse/all_similarity_rank5")
	# print("写入完成")
	return df
