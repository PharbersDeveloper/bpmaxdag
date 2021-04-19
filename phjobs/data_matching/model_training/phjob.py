# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger
import uuid
import time
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import desc,col,rank,when
from pyspark.sql import Window
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler


def execute(**kwargs):
    """
        please input your code below
        get spark session: spark = kwargs["spark"]()
    """
###########==========configure============############
    logger = phs3logger(kwargs["job_id"])
    spark = kwargs["spark"]()
    logger.info(kwargs)
###########==========configure============############

#############--------input-----------#################
    depends = get_depends_path(kwargs)
    path_label_result = depends["input"]
    input_model_path = kwargs["input_model_path"]
#############--------input-----------#################

#############--------output-----------#################
    job_id = get_job_id(kwargs)
    run_id = get_run_id(kwargs)
    result_path_prefix = get_result_path(kwargs, run_id, job_id)
    output_model_path = result_path_prefix + kwargs["model_result"]
    validate_path = result_path_prefix + kwargs["model_validate"]
    data_of_features_path = result_path_prefix + kwargs["data_of_features"]
    tm = time.strftime("%Y-%m-%d_%H-%M-%S", time.localtime())
    final_path = get_final_result_path(kwargs, tm, kwargs["final_model"])
#############--------output-----------#################

###########-------loading files-----------#################
    label_data = loading_files(spark,input_path=path_label_result)
    model_state = judge_state_of_model(input_model_path=input_model_path)
###########-------loading files-----------#################

#####################-------main function-------------#####################

    ## == 生成features
    data_of_features = generate_features(input_data_frame=label_data)
    
    ## 生成模型
    model = get_model(input_model_state=model_state,\
                      input_data_frame=data_of_features)
    
#####################-------main function-------------#####################
    treeModel = model.stages[2]
    # summary only
    print(treeModel.toDebugString)
########## == RESULT == ###########
    #写入run路径
    write_model(input_model=model,output_path=output_model_path)
    #写入报告路径
    write_model(input_model=model,output_path=final_path)
    # 写入features数据
    write_file(input_data=data_of_features,\
               output_path=data_of_features_path) 

########## == RESULT == ###########
    return {}


################--------------------- functions ---------------------################

###### == 中间文件与结果文件路径

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

    
####### == 逻辑部分 == ########
##### == 下载数据
def loading_files(spark,input_path):
    try:
        dataframe = spark.read.parquet(input_path)
    except:
        print("数据下载失败！")
        dataframe = None
    return dataframe

##### == 写入路径 
def write_model(input_model,output_path):
    
    try:
        input_model.write().overwrite().save(output_path)
        status_info = fr"{output_path} Write Success"
    except:
        status_info = fr"{output_path} Write Failed"
    print(status_info)
    
    return status_info

def write_file(input_data,output_path):
    
    try:
        input_data.repartition(16).write.mode("overwrite").parquet(output_path)
        message = fr"{output_path} Write Success"
    except:
        message = fr"{output_path} Write Failed"
    print(message)
    return message

def judge_state_of_model(input_model_path):
    try:
        mode = PipelineModel.load(input_model_path)
        print("mode 路径存在")
    except:
        print("mode 路径不存在")
        mode = None
    return mode

def generate_features(input_data_frame):
    
    assembler = VectorAssembler( \
                                inputCols=["EFFTIVENESS_MOLE_NAME", "EFFTIVENESS_PRODUCT_NAME", "EFFTIVENESS_DOSAGE", "EFFTIVENESS_SPEC", \
                                           "EFFTIVENESS_PACK_QTY", "EFFTIVENESS_MANUFACTURER"], \
                                outputCol="features")
    data_frame = assembler.transform(input_data_frame)

    return data_frame


######## 统一数据类型
def define_data_types_as_double(input_dataframe,input_col):
    
    df = input_dataframe.withColumn(input_col,col(input_col).cast(StringType()))
    
    return df

#######  左连接
def merge_table(left_table,right_table,left_key,right_key):
    
    left_table = define_data_types_as_double(input_dataframe=left_table,\
                                             input_col=left_key)
    right_table = define_data_types_as_double(input_dataframe=right_table,\
                                             input_col=right_key)
    
    left_table = left_table.withColumnRenamed(left_key,"left_col")
    right_table = right_table.withColumnRenamed(right_key,"right_col")
    df = left_table.join(right_table,left_table.left_col==right_table.right_col,"left")
    df = df.withColumnRenamed("left_col",left_key)\
            .withColumnRenamed("right_col",right_key)
    
    return df


def get_training_and_test_data(input_data):
    
    input_data = input_data.select("ID", "label", "features")   
   
    distinct_id = input_data.select("ID").distinct()
    (training_id, test_id) = distinct_id.randomSplit([0.7, 0.3])
    
    df_training = merge_table(left_table=training_id,\
                              right_table=input_data,\
                              left_key="ID",\
                              right_key="ID")
    df_test = merge_table(left_table=test_id,\
                          right_table=input_data,\
                          left_key="ID",\
                          right_key="ID")
    
    return df_training,df_test

def get_element_for_pipeline(input_data_frame):
    
    labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(input_data_frame)
    featureIndexer = VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=6).fit(input_data_frame)

    # Train a DecisionTree model.
    dt = DecisionTreeClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures")
    
    return labelIndexer,featureIndexer,dt


def training_model(input_training_dataframe):
    
    stages = get_element_for_pipeline(input_training_dataframe)
    # Chain indexers and tree in a Pipeline
    pipeline = Pipeline(stages=[stages[0], stages[1], stages[2]])

    # Train model.  This also runs the indexers.
    model = pipeline.fit(input_training_dataframe)
    
    return model
    
def get_model(input_model_state,input_data_frame):
    
    if input_model_state == None:
        data_frame = get_training_and_test_data(input_data=input_data_frame)[0]   
        model = training_model(input_training_dataframe=data_frame)
        print("生成的模型")
    else:
        model = input_model_state
        print("调用的模型")
    
    return model

################-----------------------------------------------------################
    
'''
    if input_model_path == "unknown":
        # 0. load the cleanning data
        # features
        # training_data = training_data.withColumn("EFFTIVENESS_DOSAGE", when(training_data.EFFTIVENESS_DOSAGE > 0.995, 0.995).otherwise(training_data.EFFTIVENESS_DOSAGE))
        print(training_data.count())
        assembler = VectorAssembler( \
                                    inputCols=["EFFTIVENESS_MOLE_NAME", "EFFTIVENESS_PRODUCT_NAME", "EFFTIVENESS_DOSAGE", "EFFTIVENESS_SPEC", \
                                               "EFFTIVENESS_PACK_QTY", "EFFTIVENESS_MANUFACTURER"], \
                                    outputCol="features")
        training_data = assembler.transform(training_data)


        df_cleanning = training_data.select("ID").distinct()
        # Split the data into training and test sets (30% held out for testing)
        (df_training, df_test) = df_cleanning.randomSplit([0.7, 0.3])
        # (df_training, df_test) = raw_data.randomSplit([0.7, 0.3])
        print(df_training.count())
        df_training.show(100, truncate=False)

        # 1. load the training data
        # 准备训练集合
        df_result = training_data
        df_result = df_result.select("ID", "label", "features")
#         print(df_result.where(df_result.label > 0).count())
#         df_result.where(df_result.label > 0).show(100, truncate=False)
        labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(df_result)
        featureIndexer = VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=6).fit(df_result)

        # 1.1 构建训练集合
        df_training = df_training.join(df_result, how="left", on="ID")
        # df_training.show()

        # 1.2 构建测试集合
        df_test = df_test.join(df_result, how="left", on="ID")
        df_test.write.mode("overwrite").parquet(validate_path)
        # df_test.show()

        # Train a DecisionTree model.
        dt = DecisionTreeClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures")

        # Chain indexers and tree in a Pipeline
        pipeline = Pipeline(stages=[labelIndexer, featureIndexer, dt])
    
        # Train model.  This also runs the indexers.
        model = pipeline.fit(df_training)
        model.write().overwrite().save(model_path)
        model.write().overwrite().save(final_path)

        # validata the model
        # Make predictions.
        df_predictions = model.transform(df_test)

        # save predictions
        #df_predictions.write.mode("overwrite").parquet(model_validata)

        # Select (prediction, true label) and compute test error
        #evaluator = MulticlassClassificationEvaluator(
        #labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
        #accuracy = evaluator.evaluate(df_predictions)
        #logger.warn("Test Error = %g " % (1.0 - accuracy))

        # Create pandas data frame and convert it to a spark data frame 
        #pandas_df = pd.DataFrame({"MODEL":["Decision Tree"], "ACCURACY": [accuracy]})
        #spark_df = spark.createDataFrame(pandas_df)
        #spark_df.repartition(1).write.mode("overwrite").parquet(validate_path)
    else:
        # load 
        model = PipelineModel.load(input_model_path)
        model.write().overwrite().save(model_path)
        model.write().overwrite().save(final_path)

    treeModel = model.stages[2]
    # summary only
    print(treeModel.toDebugString)
    
'''
 