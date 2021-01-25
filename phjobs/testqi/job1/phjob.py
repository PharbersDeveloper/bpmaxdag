# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

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
#     spark = kwargs["spark"]()
    logger.info(kwargs["project_path"])
    logger.info(kwargs["parent_path"])
    logger.info(kwargs["run_id_path"])
    logger.info(kwargs["split_data_path"])
    logger.info(kwargs["job_id_path"])
    
    
    logger.info(("read: " + kwargs["input"])
    logger.info(("write: " + kwargs["project_path"]+kwargs["parent_path"]+kwargs["run_id_path"]+kwargs["split_data_path"]+kwargs["job_id_path"])
    return {}
