# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import cast, rand
from pyspark.sql.functions import udf, pandas_udf, PandasUDFType


def execute(**kwargs):
    """
        please input your code below
        get spark session: spark = kwargs["spark"]()
    """
#####################============configure================#################
	logger = phs3logger(kwargs["job_id"])
	spark = kwargs["spark"]()
	logger.info(kwargs)
#####################=============configure===============#################


#################-----------input---------------################
	depends = get_depends_path(kwargs)
	cal_path = depends["cal_path"]
	competitor_path = depends["competitor_path"]
################------------input----------------################


###############----------------output-------------################
	job_id = get_job_id(kwargs)
	run_id = get_run_id(kwargs)
	result_path_prefix = get_result_path(kwargs, run_id, job_id)
	assessment_result = result_path_prefix + kwargs["assessment_result"]
# 	drop_path = result_path_prefix + kwargs["cross_drop"]
###############----------------output--------------##################


    

    return {}
