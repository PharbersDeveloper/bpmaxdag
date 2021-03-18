# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
from phcli.ph_logs.ph_logs import phs3logger
import uuid
import pandas as pd 
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import DoubleType
from nltk.metrics.distance import jaro_winkler_similarity


def execute(**kwargs):
    """
        please input your code below
        get spark session: spark = kwargs["spark"]()
    """
##############========configure============##############
    logger = phs3logger(kwargs["job_id"])
    spark = kwargs["spark"]()
    logger.info(kwargs)
############==========configure============###############

#######################---------------input-------------#######################	
    depends = get_depends_path(kwargs)
    path_jws_result = depends["input"]
    g_repartition_shared = int(kwargs["g_repartition_shared"])
#######################---------------input-------------#######################	

#######################--------------output-------------######################## 
    job_id = get_job_id(kwargs)
    run_id = get_run_id(kwargs)
    result_path_prefix = get_result_path(kwargs, run_id, job_id)
    result_path = result_path_prefix + kwargs["prod_adjust_result"]
#######################--------------output--------------########################


###################--------loading files--------------########################
    
    
##################--------loading files----------------########################

    df_jws = load_jws_file(spark, path_jws_result)
    


########################--------------main function--------------------#################
    
    df_cleanning = make_prod_col(df_jws)
    
#     df_cleanning.repartition(g_repartition_shared).write.mode("overwrite").parquet(result_path)
######################--------------main function--------------------#################   
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

def load_jws_file(spark, path_jws_result):
    df_jws = spark.read.parquet(path_jws_result)
    return df_jws

def make_prod_col(df_jws):
    
    df_cleanning = df_jws.withColumn("EFFTIVENESS_PRODUCT_NAME",get_prod_eff(df_jws.EFFTIVENESS_MOLE_NAME,\
                                                              df_jws.EFFTIVENESS_DOSAGE,\
                                                              df_jws.EFFTIVENESS_PRODUCT_NAME))
    df_cleanning.select("EFFTIVENESS_PRODUCT_NAME","PRODUCT_NAME","PRODUCT_NAME_STANDARD").distinct().show(300)
    print(df_cleanning.printSchema())
    print(df_cleanning.count())
    print(df_cleanning.select("ID").distinct().count())
    
    return df_cleanning

@pandas_udf(DoubleType(),PandasUDFType.SCALAR)
def get_prod_eff(eff_mole, eff_dosage, eff_product):
    frame = {"eff_mole":eff_mole,
            "eff_dosage":eff_dosage,
            "eff_product":eff_product}
    df = pd.DataFrame(frame)
    df['eff_prod'] = df.apply(lambda x: max(x['eff_mole'], x['eff_dosage'], x['eff_product']), axis=1)
    return df['eff_prod']

# def select_specified_cols(df_cleanning):

#     cols = ["SID", "ID","PACK_ID_CHECK",  "PACK_ID_STANDARD","DOSAGE","MOLE_NAME","PRODUCT_NAME","SPEC","PACK_QTY","MANUFACTURER_NAME","SPEC_ORIGINAL",
#                 "MOLE_NAME_STANDARD","PRODUCT_NAME_STANDARD","CORP_NAME_STANDARD","MANUFACTURER_NAME_STANDARD","MANUFACTURER_NAME_EN_STANDARD","DOSAGE_STANDARD","SPEC_STANDARD","PACK_QTY_STANDARD",
#                 "SPEC_STANDARD_GROSS","SPEC_STANDARD_VALID","SPEC_GROSS","SPEC_VALID",
#                 "EFFTIVENESS_MOLE_NAME","EFFTIVENESS_PRODUCT_NAME","EFFTIVENESS_DOSAGE","EFFTIVENESS_PACK_QTY","EFFTIVENESS_MANUFACTURER","EFFTIVENESS_SPEC"]
#     df_cleanning = df_cleanning.select(cols)
    
#     return df_cleanning
################---------------functions--------------------################
