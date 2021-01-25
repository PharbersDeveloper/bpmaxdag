# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

import uuid
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL


def execute(**kwargs):
    """
        please input your code below
        get spark session: spark = kwargs["spark"]()
    """
    logger = phs3logger(kwargs["job_id"], LOG_DEBUG_LEVEL)
    logger.info("当前 owner 为 " + str(kwargs["owner"]))
    logger.info("当前 run_id 为 " + str(kwargs["run_id"]))
    logger.info("当前 job_id 为 " + str(kwargs["job_id"]))
    spark = kwargs["spark"]()
    
    
    #input
    depends = get_depends_path(kwargs) 
    origin_path = depends["origin"]
    prediction_result_path = depends["prediction"]
    
    #output
    job_id = get_job_id(kwargs)
    run_id = get_run_id(kwargs)
    result_path_prefix = get_result_path(kwargs, run_id, job_id)
    path_prediction_origin_result = result_path_prefix + kwargs["prediction_origin_result"]
    path_lost_data = result_path_prefix + kwargs["lost_data_result"]
     
    #load data
    df_origin = spark.read.parquet(origin_path)
    df_prediction = spark.read.parquet(prediction_result_path)
     
    #处理prediction表,获取原始表名
    df_result = get_prediction_origin(df_origin,df_prediction)
    
    df_result.repartition(1).write.csv(path_prediction_origin_result)
    
    #get lost_data
    lost_data = get_lost_data(df_origin,df_prediction)
    
    lost_data.repartition(1).write.csv(path_lost_data)
    
    
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


#获取原始表信息
def get_prediction_origin(df_origin,df_prediction):
    df_origin = df_origin.withColumnRenamed('MOLE_NAME','MOLE_NAME_origin')\
                .withColumnRenamed('PRODUCT_NAME','PRODUCT_NAME_origin')\
                .withColumnRenamed('DOSAGE','DOSAGE_origin')\
                .withColumnRenamed('SPEC','SPEC_origin')\
                .withColumnRenamed('MANUFACTURER_NAME','MANUFACTURER_NAME_origin')\
                .withColumnRenamed('PACK_QTY','PACK_QTY_origin')\
                .withColumnRenamed('PACK_ID_CHECK','PACK_ID_CHECK_origin')\
                .withColumnRenamed('code','code_origin')\
                .withColumnRenamed('id','id_origin')

    df_result = df_prediction.join(df_origin,df_prediction.id == df_origin.id_origin, 'left')
    return  df_result 


#获取丢失数据
def get_lost_data(df_origin,df_prediction):
    df_origin_id = df_origin.select("id")
    df_prediction_id = df_prediction.select("id").distinct()
    df_lost_id = df_origin_id.subtract(df_prediction_id)  
    df_origin = df_origin.withColumnRenamed('id','newid')
    df_lost = df_lost_id.join(df_origin,df_lost_id.id == df_origin.newid ,'left').drop('newid')
    
    return df_lost


################--------------------- functions ---------------------################


