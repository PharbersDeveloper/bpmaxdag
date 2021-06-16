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
###########-------loading files-----------#################
    print(label_data.printSchema())
    label_data = label_data.limit(5000)

#####################-------main function-------------#####################
    signal_of_files = Judge_TrainingData_OrNot(input_dataframe=label_data,\
                                               inputCheckCol="pack_id_check")
    signal_of_trainingModel =  singal_of_whether_training_model_ByOutSide(kwargs)

    signal_of_model = Judge_Training_Model_OrNot(signal_of_dataframe=signal_of_files,\
                                                 signal_of_whether_training_model=signal_of_trainingModel,\
                                                input_model_path=input_model_path)
    
    
    ## == 生成features
    data_of_features = generate_features(input_data_frame=label_data)
    
    ## 生成模型
    model = get_model(the_state_of_model=signal_of_model,\
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
        print(fr"{input_path} 数据下载成功！")
    except:
        print(fr"{input_path} 数据下载失败！")
        dataframe = None
    print(dataframe.printSchema())
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

#### == 判断数据类型
def Judge_TrainingData_OrNot(input_dataframe,inputCheckCol):
    
    Cols_of_data = list(map(lambda x: x.upper(),input_dataframe.columns))
    
    Check_col = inputCheckCol.upper()
    
    if Check_col in Cols_of_data:
        signal = True
    else:
        signal = False
    
    return signal

def singal_of_whether_training_model_ByOutSide(kwargs):
    
    try:
        if kwargs["Training_Model"] == True:
            signal = True
        else:
            signal = False 
    except:
        signal = False
    
    return signal

### == 判断是否训练模型
def Judge_Training_Model_OrNot(signal_of_dataframe,\
                               signal_of_whether_training_model,\
                              input_model_path):
    
    if signal_of_dataframe == True:
        
        if signal_of_whether_training_model == True:
            
            signal_of_model = True
        else:
            signal_of_model = False
    else:
        signal_of_model = False
        
    if signal_of_model == False:
        
        signal_of_model = judge_state_of_model(input_model_path)
    
    return signal_of_model

def generate_features(input_data_frame):
    
    assembler = VectorAssembler( \
                                inputCols=["EFFECTIVENESS_MOLE", "EFFTIVENESS_PRODUCT", "EFFECTIVENESS_DOSAGE", "EFFECTIVENESS_SPEC", \
                                           "EFFECTIVENESS_PACK_QTY", "EFFECTIVENESS_MANUFACTURER"], \
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
    featureIndexer = VectorIndexer(inputCol="features",\
                                   outputCol="indexedFeatures",\
                                   maxCategories=6,\
                                   handleInvalid="skip").fit(input_data_frame)

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

def get_model(the_state_of_model,input_data_frame):
    
    if the_state_of_model == True:
        print("生成的模型")
        data_frame = get_training_and_test_data(input_data=input_data_frame)[0]   
        model = training_model(input_training_dataframe=data_frame)
    else:
        model = the_state_of_model
        print("调用的模型")
    
    return model

################-----------------------------------------------------################
    
