# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""
from phcli.ph_logs.ph_logs import phs3logger
import uuid
import time
import pandas as pd
import numpy as np
from pyspark.sql.types import *
from pyspark.ml import PipelineModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import udf, pandas_udf, PandasUDFType
from pyspark.sql.functions import desc, col 
from pyspark.sql.functions import rank, lit, when, row_number
from pyspark.sql import Window


def execute(**kwargs):
    """
    please input your code below
    get spark session: spark = kwargs["spark"]()
    """
#############--------configure---------------###############
    logger = phs3logger(kwargs["job_id"])
    spark = kwargs["spark"]()
    logger.info(kwargs)
#############--------configure---------------###############

###########------------input---------######################
    depends = get_depends_path(kwargs)
    df_of_features_path = depends['input_data_of_features']
    model_path = depends['model']
    origin_data_path = depends['origin_data']
    shareholds = [kwargs["g_sharehold_mole_name"],\
                  kwargs["g_sharehold_product_name"],\
                  kwargs["g_sharehold_dosage"],\
                  kwargs["g_sharehold_spec"],\
                  kwargs["g_sharehold_pack_qty"],\
                  kwargs["g_sharehold_manufacturer_name"]]
###########------------input---------######################
    
###############-------------output---------------##################
    job_id = get_job_id(kwargs)
    run_id = get_run_id(kwargs)
    result_path_prefix = get_result_path(kwargs, run_id, job_id)
    prediction_path = result_path_prefix + kwargs["prediction_result"]
    positive_result_path = result_path_prefix + kwargs["positive_result"]
    negative_result_path = result_path_prefix + kwargs["negative_result"]
    ## rusult 目录文件
    tm = time.strftime("%Y-%m-%d_%H-%M-%S", time.localtime())
    final_predictions = get_final_result_path(kwargs, run_id, kwargs["final_predictions"], tm)
    final_positive_path = get_final_result_path(kwargs, run_id, kwargs["final_positive"], tm)
    final_negative_path = get_final_result_path(kwargs, run_id, kwargs["final_negative"], tm)
    final_report_path = get_final_result_path(kwargs, run_id, kwargs["final_report"], tm)
###################------------output------------#######################

#################------------loading files--------------#################
    df_test = loading_files(spark,input_path=df_of_features_path)
    df_origin_data = loading_files(spark,input_path=origin_data_path)
    model = loading_model(input_model_path=model_path)
################-----------loading files---------------##################
    

##################-----------main functions---------####################
    
    df_predictions = let_model_to_classification(input_data=df_test,\
                                                 input_model=model)
    model_score = model_performance_evaluation(input_dataframe=df_predictions)
    
    
    
##################-----------main functions---------####################
	# df_predictions.write.mode("overwrite").parquet(prediction_path)
    '''
    # 5. 生成文件
    df_predictions = df_predictions.drop("indexedFeatures", "rawPrediction", "probability")
    df_predictions = similarity(df_predictions, shareholds)
    df_predictions = df_predictions.na.fill("").drop("features")
    df_predictions.persist()
#     print(df_predictions.count())
#     print(df_predictions.select("ID").distinct().count())
#     print(df_predictions.where((df_predictions.label == 1)).count())
#     print(df_predictions.where((df_predictions.prediction == 1) & (df_predictions.RANK == 1)).count())
#     print(df_predictions.where((df_predictions.prediction == 1) & (df_predictions.label == 1) & (df_predictions.RANK == 1)).count())
    df_predictions.write.mode("overwrite").parquet(prediction_path)
    df_predictions.repartition(1).write.mode("overwrite").option("header", "true").csv(final_predictions)

    df_positive = df_predictions.where((df_predictions.prediction == 1.0) & (df_predictions.RANK == 1))
    df_positive.write.mode("overwrite").parquet(positive_result_path)   # TODO 记得打开

	# df_positive = df_positive.withColumn("MASTER_DOSAGE", join_pandas_udf(col("MASTER_DOSAGE"))) \
	# 						.withColumn("MANUFACTURER_NAME_STANDARD_WORDS", join_pandas_udf(col("MANUFACTURER_NAME_STANDARD_WORDS"))) \
	# 						.withColumn("MANUFACTURER_NAME_CLEANNING_WORDS", join_pandas_udf(col("MANUFACTURER_NAME_CLEANNING_WORDS"))) \
	# 						.withColumn("MANUFACTURER_NAME_STANDARD_WORDS_SEG", join_pandas_udf(col("MANUFACTURER_NAME_STANDARD_WORDS_SEG"))) \
	# 						.withColumn("MANUFACTURER_NAME_CLEANNING_WORDS_SEG", join_pandas_udf(col("MANUFACTURER_NAME_CLEANNING_WORDS_SEG"))) \
	# 						.withColumn("features", join_udf(col("features")))
    df_positive.repartition(1).write.mode("overwrite").option("header", "true").csv(final_positive_path)
    logger.warn("机器判断positive的条目写入完成")

    df_negative = df_predictions.where((df_predictions.prediction == 0.0) | ((df_predictions.prediction == 1.0) & (df_predictions.RANK != 1)))
    df_negative.write.mode("overwrite").parquet(negative_result_path)
	# df_negative = df_negative.withColumn("MASTER_DOSAGE", join_pandas_udf(col("MASTER_DOSAGE"))) \
	# 						.withColumn("MANUFACTURER_NAME_STANDARD_WORDS", join_pandas_udf(col("MANUFACTURER_NAME_STANDARD_WORDS"))) \
	# 						.withColumn("MANUFACTURER_NAME_CLEANNING_WORDS", join_pandas_udf(col("MANUFACTURER_NAME_CLEANNING_WORDS"))) \
	# 						.withColumn("MANUFACTURER_NAME_STANDARD_WORDS_SEG", join_pandas_udf(col("MANUFACTURER_NAME_STANDARD_WORDS_SEG"))) \
	# 						.withColumn("MANUFACTURER_NAME_CLEANNING_WORDS_SEG", join_pandas_udf(col("MANUFACTURER_NAME_CLEANNING_WORDS_SEG"))) \
	# 						.withColumn("features", join_udf(col("features")))
    df_negative.repartition(1).write.mode("overwrite").option("header", "true").csv(final_negative_path)
    logger.warn("机器判断negative的条目写入完成")

#	df_lost = df_lost.select("ID").distinct()
	# df_lost.write.mode("overwrite").parquet(final_lost_path)
	# df_lost.drop("JACCARD_DISTANCE").repartition(1).write.mode("overwrite").option("header", "true").csv(final_lost_path)
	# logger.warn("匹配第一步丢失条目写入完成")

    # 6. 结果统计
    #all_count = df_predictions.count()# + df_lost.count() # 数据总数
    all_count = df_origin_data.count()
    ph_total = df_result.groupBy("ID").agg({"label": "first"}).count()
    positive_count = df_positive.count()  # 机器判断pre=1条目
    negative_count = ph_total - positive_count # - df_lost.count()  # 机器判断pre=0条目
    matching_ratio = str(round((float(positive_count / ph_total) * 100),2)) + '%'       # 匹配率

    # 7. 最终结果报告以csv形式写入s3
    report = [("data_matching_report", ),]
    report_schema = StructType([StructField('title',StringType(),True),])
    df_report = spark.createDataFrame(report, schema=report_schema).na.fill("")
    df_report = df_report.withColumn("数据总数", lit(str(all_count)))
    df_report = df_report.withColumn("进入匹配流程条目", lit(str(ph_total)))
    # 	df_report = df_report.withColumn("丢失条目", lit(str(df_lost.count())))
    df_report = df_report.withColumn("机器匹配条目", lit(str(positive_count)))
    df_report = df_report.withColumn("机器无法匹配条目", lit(str(negative_count)))
    df_report = df_report.withColumn("匹配率", lit(str(matching_ratio)))
    df_report = df_report.withColumn("准确率", lit(str(Accuracy)))
    df_report.show()
    df_report.repartition(1).write.mode("overwrite").option("header", "true").csv(final_report_path)
    logger.warn("final report csv文件写入完成")
    ''' 
    
    return {}

    

################--------------------- functions ---------------------################
"""
中间文件与结果文件路径
"""
def get_run_id(kwargs):
    run_id = kwargs["run_id"]
    if not run_id:
        run_id = "runid_" + "alfred_runner_test"
    return run_id

def get_job_id(kwargs):
    job_name = kwargs["job_name"]
    job_id = kwargs["job_id"]
    if not job_id:
        job_id = "jobid_" + uuid.uuid4().hex
    return job_name # + "_" + job_id 

def get_result_path(kwargs, run_id, job_id):
    path_prefix = kwargs["path_prefix"]
    return path_prefix + "/" + run_id + "/" + job_id + "/"

def get_depends_file_path(kwargs, job_name, job_key):
    run_id = get_run_id(kwargs)
    return get_result_path(kwargs, run_id, job_name) + job_key

def get_final_result_path(kwargs, run_id, final_key, tm):
    path_prefix = kwargs["final_prefix"]
    return path_prefix + "/" + tm + "/" + final_key

def get_depends_path(kwargs):
    depends_lst = eval(kwargs["depend_job_names_keys"])
    result = {}
    for item in depends_lst:
        tmp_lst = item.split("#")
        depends_job = tmp_lst[0]
        depends_key = tmp_lst[1]
        depends_name = tmp_lst[2]
        result[depends_name] = get_depends_file_path(kwargs, depends_job, depends_key)
    return result
####### == 中间文件路径 == ######## 

##### == 下载数据
def loading_files(spark,input_path):
    try:
        dataframe = spark.read.parquet(input_path)
        print(fr"{input_path} loading success!")
    except:
        print(fr"{input_path} loading fail")
        dataframe = None
    return dataframe


#### == 加载模型
def loading_model(input_model_path):
    try:
        mode = PipelineModel.load(input_model_path)
        print("mode 路径存在")
    except:
        print("mode 路径不存在")
        mode = None
    return mode

### ==进行分类预测 
def let_model_to_classification(input_data,input_model):
    
    df_predictions = input_model.transform(input_data)
    print(df_predictions.count())
    
    
    print("模型预测完毕！")
    return df_predictions

### 转换百分数
def let_decimal_to_be_percentage(input_decimal):
    
    data = '%.2f%%' % (input_decimal * 100)
    
    return data
    
### == 模型性能评估
def model_performance_evaluation(input_dataframe):
    evaluator_acc =  MulticlassClassificationEvaluator(labelCol="indexedLabel",\
                                     predictionCol="prediction",\
                                     metricName="accuracy").evaluate(input_dataframe)
    
    precision = MulticlassClassificationEvaluator(labelCol="indexedLabel",\
                                                  predictionCol="prediction",\
                                                  metricName="weightedPrecision").evaluate(input_dataframe)
    
    evaluator_recall = MulticlassClassificationEvaluator(labelCol="indexedLabel",\
                                               predictionCol="prediction",\
                                               metricName="weightedRecall").evaluate(input_dataframe)
    
    model_socre = (evaluator_acc,precision,evaluator_recall)
    model_socre = tuple(map(lambda x: let_decimal_to_be_percentage(x),model_socre))
    model_score_name = ('Accuracy','Precision','Recall')
    score = dict(zip(model_score_name,model_socre))
    
    print(score)
    
    return score 


def similarity(df, shareholds):
    df = df.withColumn("SIMILARITY", \
                       (df.EFFTIVENESS_MOLE_NAME * shareholds[0] + df.EFFTIVENESS_PRODUCT_NAME * shareholds[1] + df.EFFTIVENESS_DOSAGE * shareholds[2] \
                        + df.EFFTIVENESS_SPEC * shareholds[3] + df.EFFTIVENESS_PACK_QTY * shareholds[4] + df.EFFTIVENESS_MANUFACTURER * shareholds[5]))
    windowSpec = Window.partitionBy("ID").orderBy(desc("SIMILARITY"), desc("EFFTIVENESS_MOLE_NAME"), desc("EFFTIVENESS_DOSAGE"), desc("PACK_ID_STANDARD"))
    df = df.withColumn("RANK", row_number().over(windowSpec))
    df = df.where((df.RANK <= 5) | (df.label == 1.0))
    return df


join_udf = udf(lambda x: ",".join(map(str,x)))

@pandas_udf(StringType(), PandasUDFType.SCALAR)
def join_pandas_udf(array_col):
    frame = { "array_col": array_col }

    df = pd.DataFrame(frame)

    def array_to_str(arr):
        if type(arr) != np.ndarray:
            s = ""
        else:
            s = ",".join(map(str,arr))
        return s

    df["RESULT"] = df["array_col"].apply(array_to_str)
    return df["RESULT"]
################-----------------------------------------------------################