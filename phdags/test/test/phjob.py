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
    spark = kwargs['spark']()
    logger = phs3logger(kwargs["job_id"], LOG_DEBUG_LEVEL)

    spark.sql("SHOW tables").show()

    spark.read.parquet("s3a://ph-stream/common/public/universe/0.0.1").show()



