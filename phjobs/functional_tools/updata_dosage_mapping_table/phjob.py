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
    path_dosage_mapping_original = kwargs['path_dosage_mapping_original']
    path_dosage_mapping_negative = kwargs['path_dosage_mapping_negative']
    ########### == input == ###########
    
    
    ########### == output == ###########
    path_output = kwargs['path_write_file']
    ########### == output == ###########
    
    ########## == loading files == #############
    df_dosage_mapping_original = loading_parquet_files(spark, path_dosage_mapping_original)
    df_dosage_mapping_negative = loading_csv_files(spark, path_dosage_mapping_negative)
    
    ######### == loading files == #############
    
    
    ########### == main function == #########
    
    df_dosage_mapping_original = get_dosage_mapping_elements(input_dataframe=df_dosage_mapping_original,\
                                                             input_dosage="DOSAGE",\
                                                             input_master="MASTER_DOSAGE")
    
    df_dosage_mapping_negative =  get_negative_dosage_mapping_elements(input_dataframe=df_dosage_mapping_negative\
                                     ,input_dosage="DOSAGE"\
                                     ,input_standard_dosage="DOSAGE_STANDARD"\
                                     ,input_master="MASTER_DOSAGE",\
                                     similarity=0.5)
    df = get_available_dosage_mapping_elements(input_dataframe_oringal=df_dosage_mapping_original,\
                                               input_dataframe_negative=df_dosage_mapping_negative)

    
#     wirte_files(df,path_output)
    
    ########### == main function == #########
    
    
    
    return {}




#####  == 下载文件 == ########
def loading_csv_files(spark, input_path):

    df = spark.read.csv(input_path, header=True) 
    
    return df

def loading_parquet_files(spark, input_path):
    
    df = spark.read.parquet(input_path)
    
    return df

##### == 写入路径 == #########
def wirte_files(input_df, path_output):
    
    try:
        input_df.write.mode("overwrite").parquet(path_output)
        status_info = r"Write Success"
        
    except:
        status_info = r"Write Failed"
    print(status_info)
    
    return status_info





def get_dosage_mapping_elements(input_dataframe,input_dosage,input_master):
    
    input_dataframe = input_dataframe.na.fill('',subset=[input_dosage])
    
    input_dataframe = input_dataframe.withColumn(input_master,explode(col(input_master)))
    
    data_frame = input_dataframe.groupBy(col(input_dosage)).agg(collect_set(col(input_master)).alias(input_master))
    
    
    return data_frame


def get_negative_dosage_mapping_elements(input_dataframe,input_dosage,input_standard_dosage,input_master,similarity):
     
    input_dataframe = input_dataframe.na.fill('',subset=[input_master])
    
    input_dataframe = input_dataframe.withColumn(input_master, split(col(input_master), pattern=' '))\
                                    .filter((col("EFFTIVENESS_DOSAGE") < float(similarity)) & (col("PACK_ID_CHECK")==col("PACK_ID_STANDARD")))
    
    input_dataframe=  input_dataframe.withColumn(input_standard_dosage,array(col(input_dosage),col(input_standard_dosage)))\
                .withColumn(input_standard_dosage,array_distinct(array_union(col(input_standard_dosage),col(input_master))))\
                .withColumn(input_standard_dosage, array_remove(col(input_standard_dosage),""))
    
    input_dataframe = input_dataframe.withColumn(input_standard_dosage,explode(col(input_standard_dosage)))
    data_frame =  input_dataframe.groupBy(col(input_dosage)).agg(collect_set(col(input_standard_dosage)).alias(input_master))
    data_frame.show(300)
    
    return data_frame


def get_available_dosage_mapping_elements(input_dataframe_oringal, input_dataframe_negative):
    
    data_frame = input_dataframe_oringal.union(input_dataframe_negative)
    
    data_frame = get_dosage_mapping_elements(input_dataframe=data_frame\
                                             ,input_dosage="DOSAGE",\
                                             input_master="MASTER_DOSAGE")
    
    return data_frame