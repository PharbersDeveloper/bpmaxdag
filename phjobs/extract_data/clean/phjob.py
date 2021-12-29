# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
import boto3
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL


def execute(**kwargs):
    """
        please input your code below
        """
    logger = phs3logger(kwargs["job_id"], LOG_DEBUG_LEVEL)
    logger.info("当前 owner 为 " + str(kwargs["owner"]))
    logger.info("当前 run_id 为 " + str(kwargs["run_id"]))
    logger.info("当前 job_id 为 " + str(kwargs["job_id"]))

    awsConf = {
        "aws_access_key_id": "AKIAWPBDTVEAPUSJJMWN",
        "aws_secret_access_key": "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"
    }

    clean(awsConf, kwargs["path"], logger)
    return {}


def clean(awsConf, path, logger):
    prefix = "/".join(path.replace("//", "").split("/")[1:])
    s3 = boto3.resource('s3',
                        aws_access_key_id=awsConf["aws_access_key_id"],
                        aws_secret_access_key=awsConf["aws_secret_access_key"],
                        region_name="cn-northwest-1")
    bucket = s3.Bucket("ph-stream")
    for obj in bucket.objects.filter(Prefix=prefix):
        logger.info("Delete bucket = {}, path = {}".format(
            obj.bucket_name, obj.key))
        obj.delete()
