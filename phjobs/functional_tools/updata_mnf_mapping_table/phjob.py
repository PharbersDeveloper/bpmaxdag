# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from pyspark.sql.functions import array, array_join, col,\
                                split, array_union,array_remove,\
                                collect_set, explode, array_distinct


def execute(**kwargs):
    
    ########### == configure == ###########
    """
        please input your code below
        get spark session: spark = kwargs["spark"]()
    """
    logger = phs3logger(kwargs["job_id"], LOG_DEBUG_LEVEL)
    logger.info("当前 owner 为 " + str(kwargs["owner"]))
    logger.info("当前 run_id 为 " + str(kwargs["run_id"]))
    logger.info("当前 job_id 为 " + str(kwargs["job_id"]))
    spark = kwargs["spark"]()
    ########### == configure == ###########
    
    
    ########### == input == ###########
    path_mnf_mapping_original = kwargs['path_mnf_mapping_original']
    path_mnf_mapping_prediction = kwargs['path_mnf_mapping_predictions']
    ########### == input == ###########
    
    
    ########### == output == ###########
    path_output = kwargs['path_write_file']
    ########### == output == ###########
    
    ########## == loading files == #############
    
    df_mnf_mapping_original = loading_parquet_files(spark, path_mnf_mapping_original)
    
    df_mnf_mapping_prediction = loading_csv_files(spark, path_mnf_mapping_prediction)
    
    ######### == loading files == #############
    
    
    ########### == main function == #########
     
    df_mnf_mapping_original = get_mnf_mapping_elements(input_dataframe=df_mnf_mapping_original,\
                                                             input_mnf="MANUFACTURER_NAME_STANDARD",\
                                                             input_master="MASTER_MANUFACTURER")
    
    df_mnf_mapping_prediction =  get_prediction_mnf_mapping_elements(input_dataframe=df_mnf_mapping_prediction\
                                     ,input_mnf="MANUFACTURER_NAME"\
                                     ,input_standard_mnf="MANUFACTURER_NAME_STANDARD"\
                                     ,input_master="MASTER_MANUFACTURER",\
                                     similarity=0.9)


    df = get_available_mnf_mapping_elements(input_dataframe_oringal=df_mnf_mapping_original,\
                                               input_dataframe_prediction=df_mnf_mapping_prediction)

    
#     wirte_files(df,path_output)
   
    
    ########### == main function == #########
    
    
    
    return {}




#####  == 下载文件 == ########
def loading_csv_files(spark, input_path):

    df = spark.read.csv(input_path, header=True) 
    
    return df

def loading_parquet_files(spark, input_path):
    try:
        df = spark.read.parquet(input_path)
        return df
    except:
        print("parquet文件不存在，请检查文件格式及路径！")
        return None 

##### == 写入路径 == #########
def wirte_files(input_df, path_output):
    
    try:
        input_df.write.mode("overwrite").parquet(path_output)
        status_info = r"Write Success"
        
    except:
        status_info = r"Write Failed"
    print(status_info)
    
    return status_info

def get_mnf_mapping_elements(input_dataframe,input_mnf,input_master):
    
    if input_dataframe == None:
        return None
    else:

        input_dataframe = input_dataframe.na.fill('',subset=[input_mnf])

        input_dataframe = input_dataframe.withColumn(input_master,explode(col(input_master)))

        data_frame = input_dataframe.groupBy(col(input_mnf)).agg(collect_set(col(input_master)).alias(input_master))

        return data_frame


def get_prediction_mnf_mapping_elements(input_dataframe,input_mnf,input_standard_mnf,input_master,similarity):
    
        
    input_dataframe = input_dataframe.filter((col("EFFTIVENESS_MANUFACTURER") < float(similarity)) &\
                                             (col("PACK_ID_CHECK")==col("PACK_ID_STANDARD")))

#     input_dataframe = input_dataframe.filter(col("PACK_ID_CHECK")==col("PACK_ID_STANDARD"))

    if input_master in  input_dataframe.columns:
     
        input_dataframe = input_dataframe.na.fill('',subset=[input_master])
    
        input_dataframe = input_dataframe.withColumn(input_master, split(col(input_master), pattern=' '))
        
        input_dataframe= input_dataframe.withColumn(input_mnf,array(col(input_mnf),col(input_standard_mnf)))\
                    .withColumn(input_mnf,array_distinct(array_union(col(input_mnf),col(input_master))))\
                    .withColumn(input_mnf, array_remove(col(input_mnf),""))
    else:
        input_dataframe= input_dataframe.withColumn(input_mnf,array(col(input_mnf),col(input_standard_mnf)))\
                .withColumn(input_mnf,array_distinct(col(input_mnf)))\
                .withColumn(input_mnf, array_remove(col(input_mnf),""))
        
    input_dataframe = input_dataframe.withColumn(input_mnf,explode(col(input_mnf)))
    data_frame =  input_dataframe.groupBy(col(input_standard_mnf)).agg(collect_set(col(input_mnf)).alias(input_master))
    data_frame.show(500)
    
    return data_frame


def get_available_mnf_mapping_elements(input_dataframe_oringal, input_dataframe_prediction):
    
    if input_dataframe_oringal == None:
        data_frame = input_dataframe_prediction
    else:
        data_frame = input_dataframe_oringal.union(input_dataframe_prediction)
        data_frame = get_mnf_mapping_elements(input_dataframe=data_frame\
                                                 ,input_mnf="MANUFACTURER_NAME_STANDARD",\
                                                 input_master="MASTER_MANUFACTURER")
    return data_frame