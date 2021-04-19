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
    path_of_no_exist_pack_check_id = depends["input_df_of_no_pack_check_id"]
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
    final_predictions = get_final_result_path(kwargs, tm, kwargs["final_predictions"])
    final_positive_path = get_final_result_path(kwargs, tm, kwargs["final_positive"])
    final_negative_path = get_final_result_path(kwargs, tm, kwargs["final_negative"])
    final_report_path = get_final_result_path(kwargs, tm, kwargs["final_report"])
###################------------output------------#######################

#################------------loading files--------------#################
    df_test = loading_files(spark,input_path=df_of_features_path)
    df_origin_data = loading_files(spark,input_path=origin_data_path)
    df_of_no_pack_check_id = loading_csv_files(spark,input_path=path_of_no_exist_pack_check_id)
    model = loading_model(input_model_path=model_path)
################-----------loading files---------------##################
    

##################-----------main functions---------####################
    
    df_predictions = let_model_to_classification(input_data=df_test,\
                                                 input_model=model)
   
    model_score = model_performance_evaluation(input_dataframe=df_predictions)
    
    
    df_predictions,df_positive,df_negative = generate_output_dataframe(input_dataframe_of_predictions=df_predictions,\
                                                input_shareholds=shareholds)
    
    report_data = get_data_of_report(input_dataframe_of_original=df_origin_data,\
                                     input_dataframe_of_no_check_id=df_of_no_pack_check_id,\
                                     input_dataframe_of_test=df_test,\
                                     input_dataframe_of_predictions=df_predictions,\
                                     input_dataframe_of_positive=df_positive,\
                                     input_dataframe_of_negative=df_negative,\
                                     input_socre_of_model=model_score)
    
    df_report = generate_output_report(spark,input_report_data=report_data)
    
##################-----------main functions---------####################

###### == RESULT == ####
    write_file(input_dataframe=df_predictions,\
               write_path=prediction_path,\
               write_file_type="parquet")
    write_file(input_dataframe=df_predictions,\
               write_path=final_predictions ,\
               write_file_type="csv")
    write_file(input_dataframe=df_positive,\
               write_path=positive_result_path,\
               write_file_type="parquet")
    write_file(input_dataframe=df_positive,\
               write_path=final_positive_path,\
               write_file_type="csv")
    write_file(input_dataframe=df_negative,\
               write_path=negative_result_path,\
               write_file_type="parquet")
    write_file(input_dataframe=df_negative,\
               write_path=final_negative_path,\
               write_file_type="csv")
    write_file(input_dataframe=df_report,\
               write_path=final_report_path,\
               write_file_type="csv")
   
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

def get_final_result_path(kwargs, tm, final_key):
    path_prefix = kwargs["final_prefix"]
    if kwargs["run_id"] == None:
        tm = "test"
    else:
        tm = str(tm)
    final_result_path = path_prefix + "/" + tm +"/" + final_key 
    print(final_result_path)
        
    return final_result_path

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

def loading_csv_files(spark,input_path):

    try:
        dataframe = spark.read.csv(input_path,header=True)
        print(fr"{input_path}  csv_file loading success!")
    except:
        print(fr"{input_path} csv_file loading fail")
        dataframe = None
    return dataframe

#### == 写入数据
def write_file(input_dataframe,write_path,write_file_type):
    
    try:
        if write_file_type.lower() == 'parquet':
            input_dataframe.repartition(16).write.mode("overwrite").parquet(write_path)
        else:
            input_dataframe.repartition(1).write.mode("overwrite").csv(write_path,header=True)
        message = f"{write_path} {write_file_type}  write success !"
    except:
        message = f"{write_path} {write_file_type}  write fail!"
    print(message)
    
    return message

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
                                     metricName="accuracy")\
                                    .evaluate(input_dataframe)
    
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
   
    return score 

### == 生成输出dataframe
def generate_output_dataframe(input_dataframe_of_predictions,input_shareholds):
    
    # 5. 生成文件
    df_predictions = input_dataframe_of_predictions.drop("indexedFeatures", "rawPrediction", "probability")
    df_predictions = similarity(df_predictions, input_shareholds)
    df_predictions = df_predictions.na.fill("").drop("features")
    df_predictions.persist()
    
    
    df_positive = df_predictions.where((df_predictions.prediction == 1.0) & (df_predictions.RANK == 1))

    df_negative = df_predictions.where((df_predictions.prediction == 0.0) | ((df_predictions.prediction == 1.0) & (df_predictions.RANK != 1)))
    
    return df_predictions,df_positive,df_negative 


### == 获取报告数据
def get_data_of_report(input_dataframe_of_original,input_dataframe_of_no_check_id,\
                      input_dataframe_of_test,input_dataframe_of_predictions,\
                      input_dataframe_of_positive,input_dataframe_of_negative,\
                      input_socre_of_model):
    
    total_number = input_dataframe_of_original.count()
    number_of_lose_pack_check_id = input_dataframe_of_no_check_id.count()
    number_of_available = total_number - number_of_lose_pack_check_id
    number_of_enter_process = input_dataframe_of_test.groupBy("ID").agg({"label":"first"}).count()
    number_of_positive = input_dataframe_of_positive.count()
    number_of_negative = number_of_enter_process - number_of_positive
    matching_rate = str(round((float(number_of_positive / number_of_enter_process) * 100),2)) + '%'
    
    input_socre_of_model["total_number"]= str(total_number)
    input_socre_of_model["lose_pack_check_id"]= str(number_of_lose_pack_check_id)
    input_socre_of_model["available"]= str(number_of_available)
    input_socre_of_model["enter_process"]= str(number_of_enter_process)
    input_socre_of_model["positive"]= str(number_of_positive)
    input_socre_of_model["negative"]= str(number_of_negative)
    input_socre_of_model["matching_rate"]= str(matching_rate)
    print(input_socre_of_model)
    
    return input_socre_of_model


### == 生成报告
def generate_output_report(spark,input_report_data):
    
    # 7. 最终结果报告以csv形式写入s3
    report = [("Data_matching_report", ),]
    report_schema = StructType([StructField('Title',StringType(),True),])
    df_report = spark.createDataFrame(report, schema=report_schema).na.fill("")
    df_report = df_report.withColumn("total_number", lit(str(input_report_data["total_number"])))
    df_report = df_report.withColumn("lose_pack_check_id",\
                                     lit(str(input_report_data["lose_pack_check_id"])))
    df_report = df_report.withColumn("available",\
                                     lit(str(input_report_data["available"])))
    df_report = df_report.withColumn("enter_process",\
                                     lit(str(input_report_data["enter_process"])))
    df_report = df_report.withColumn("positive",\
                                     lit(str(input_report_data["positive"])))
    df_report = df_report.withColumn("negative",\
                                     lit(str(input_report_data["negative"])))
    df_report = df_report.withColumn("matching_rate",\
                                     lit(str(input_report_data["matching_rate"])))
    df_report = df_report.withColumn("Precision",\
                                     lit(str(input_report_data["Precision"])))
    df_report = df_report.withColumn("Recall",\
                                     lit(str(input_report_data["Recall"])))
    
    
    df_report.show()
    
    return df_report 


def similarity(df, shareholds):
    df = df.withColumn("SIMILARITY", \
                       (df.EFFTIVENESS_MOLE_NAME * shareholds[0] + df.EFFTIVENESS_PRODUCT_NAME * shareholds[1] + df.EFFTIVENESS_DOSAGE * shareholds[2] \
                        + df.EFFTIVENESS_SPEC * shareholds[3] + df.EFFTIVENESS_PACK_QTY * shareholds[4] + df.EFFTIVENESS_MANUFACTURER * shareholds[5]))
    windowSpec = Window.partitionBy("ID").orderBy(desc("SIMILARITY"), desc("EFFTIVENESS_MOLE_NAME"), desc("EFFTIVENESS_DOSAGE"), desc("PACK_ID_STANDARD"))
    df = df.withColumn("RANK", row_number().over(windowSpec))
    df = df.where((df.RANK <= 5) | (df.label == 1.0))
    return df
################-----------------------------------------------------################