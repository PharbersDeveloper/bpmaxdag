# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
import time
import os 

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
    logger.info(kwargs["input"])
    logger.info(kwargs["output_path_job1"])

    logger.info("****************************************")
    
    output_path_job1 = kwargs["input"] +"\n" + "hello !"
    logger.info(output_path_job1)
    
    kwargs['output_path_job1'] = kwargs['output_path_job1'] + str(time.time()) + "哈哈！"
    
    logger.info('-------------------------------------')
    logger.info(kwargs['output_path_job1'])
    
    project_path = 's3a://ph-max-auto/2020-08-11/data_matching'
    
    path = os.path.join(project_path,'aiflow')
    print(path)
    
    
    return {}
