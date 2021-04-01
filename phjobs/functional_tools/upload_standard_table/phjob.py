# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL


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
    path_standard_table = kwargs['path_standard_table']
    ########### == input == ###########
    
    
    ########### == output == ###########
    path_output = kwargs['path_write_file']
    ########### == output == ###########
    
    ########## == loading files == #############
    df_standard_table = loading_files(spark, path_standard_table)
    
    ######### == loading files == #############
    
    
    ########### == main function == #########
    
    wirte_files(df_standard_table,path_output)
    
    ########### == main function == #########
    
    
    
    return {}




#####  == 下载文件 == ########
def loading_files(spark, path_standard_table):
    
    df_standard_table = spark.read.csv(path_standard_table, header=True, encoding="gbk") 
    
    return df_standard_table


##### == 写入路径 == #########
def wirte_files(df_standard_table, path_output):
    
    try:
        df_standard_table.write.mode("overwrite").parquet(path_output)
        status_info = r"Write Success"
        
    except:
        status_info = r"Write Failed"
    print(status_info)
    
    return status_info