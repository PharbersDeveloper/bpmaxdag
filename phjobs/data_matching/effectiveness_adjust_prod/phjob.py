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
    path_mnf_adjust = depends["mnf_adjust"]
    g_repartition_shared = int(kwargs["g_repartition_shared"])
#######################---------------input-------------#######################	

#######################--------------output-------------######################## 
    job_id = get_job_id(kwargs)
    run_id = get_run_id(kwargs)
    result_path_prefix = get_result_path(kwargs, run_id, job_id)
    result_path = result_path_prefix + kwargs["prod_adjust_result"]
#######################--------------output--------------########################


###################--------loading files--------------########################
    #mole 分词job数据
    
    
    #mnf 分词job数据
    df_cleanning = load_mnf_files(spark,path_mnf_adjust)
##################--------loading files----------------########################



########################--------------main function--------------------#################
    df_cleanning = second_round_with_col_recalculate(df_cleanning)
    
    df_cleanning = select_specified_cols(df_cleanning)
    
    df_cleanning.repartition(g_repartition_shared).write.mode("overwrite").parquet(result_path)
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

def load_mnf_files(spark,path_mnf_adjust):
    
    df_cleanning = spark.read.parquet(path_mnf_adjust)
    
    return df_cleanning

def second_round_with_col_recalculate(df_cleanning):
   
    df_cleanning = df_cleanning.withColumn("EFFTIVENESS_PRODUCT_NAME_SE", \
                                    prod_name_replace(df_cleanning.EFFTIVENESS_MOLE_NAME, df_cleanning.EFFTIVENESS_MANUFACTURER_SE, \
                                    df_cleanning.EFFTIVENESS_PRODUCT_NAME, df_cleanning.MOLE_NAME, \
                                    df_cleanning.PRODUCT_NAME_STANDARD))
        
    df_cleanning = df_cleanning.withColumnRenamed("EFFTIVENESS_PRODUCT_NAME", "EFFTIVENESS_PRODUCT_NAME_FIRST") \
                    .withColumnRenamed("EFFTIVENESS_MANUFACTURER", "EFFTIVENESS_MANUFACTURER_FIRST") \
                    .withColumnRenamed("EFFTIVENESS_DOSAGE_SE", "EFFTIVENESS_DOSAGE") \
                    .withColumnRenamed("EFFTIVENESS_MANUFACTURER_SE", "EFFTIVENESS_MANUFACTURER") \
                    .withColumnRenamed("EFFTIVENESS_PRODUCT_NAME_SE", "EFFTIVENESS_PRODUCT_NAME")
								# .withColumnRenamed("EFFTIVENESS_DOSAGE", "EFFTIVENESS_DOSAGE_FIRST") \
    df_cleanning.persist()
    return df_cleanning



#prod 处理逻辑
@pandas_udf(DoubleType(), PandasUDFType.SCALAR)
def prod_name_replace(eff_mole_name, eff_mnf_name, eff_product_name, mole_name, prod_name_standard):
    frame = { "EFFTIVENESS_MOLE_NAME": eff_mole_name, "EFFTIVENESS_MANUFACTURER_SE": eff_mnf_name, "EFFTIVENESS_PRODUCT_NAME": eff_product_name,
            "MOLE_NAME": mole_name, "PRODUCT_NAME_STANDARD": prod_name_standard, }
    df = pd.DataFrame(frame)

    df["EFFTIVENESS_PROD"] = df.apply(lambda x: max((0.5* x["EFFTIVENESS_MOLE_NAME"] + 0.5* x["EFFTIVENESS_MANUFACTURER_SE"]), \
                                (x["EFFTIVENESS_PRODUCT_NAME"]), \
                                (jaro_winkler_similarity(x["MOLE_NAME"], x["PRODUCT_NAME_STANDARD"]))), axis=1)

    return df["EFFTIVENESS_PROD"]

def select_specified_cols(df_cleanning):

    cols = ["SID", "ID","PACK_ID_CHECK",  "PACK_ID_STANDARD","DOSAGE","MOLE_NAME","PRODUCT_NAME","SPEC","PACK_QTY","MANUFACTURER_NAME","SPEC_ORIGINAL",
                "MOLE_NAME_STANDARD","PRODUCT_NAME_STANDARD","CORP_NAME_STANDARD","MANUFACTURER_NAME_STANDARD","MANUFACTURER_NAME_EN_STANDARD","DOSAGE_STANDARD","SPEC_STANDARD","PACK_QTY_STANDARD",
                "SPEC_STANDARD_GROSS","SPEC_STANDARD_VALID","SPEC_GROSS","SPEC_VALID",
                "EFFTIVENESS_MOLE_NAME","EFFTIVENESS_PRODUCT_NAME","EFFTIVENESS_DOSAGE","EFFTIVENESS_PACK_QTY","EFFTIVENESS_MANUFACTURER","EFFTIVENESS_SPEC"]
    df_cleanning = df_cleanning.select(cols)
    
    return df_cleanning
################---------------functions--------------------################
