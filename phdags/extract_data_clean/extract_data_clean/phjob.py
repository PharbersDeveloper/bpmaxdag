# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
import boto3
import traceback
from ph_logs.ph_logs import phs3logger


def execute(**kwargs):
    """
        please input your code below
        get spark session: spark = kwargs["spark"]()
    """
    logger = phs3logger(kwargs["job_id"])
    logger.info("当前 owner 为 " + str(kwargs["owner"]))
    logger.info("当前 run_id 为 " + str(kwargs["run_id"]))
    logger.info("当前 job_id 为 " + str(kwargs["job_id"]))
    spark = kwargs["spark"]()
    awsConf = {
        "aws_access_key_id": "AKIAWPBDTVEAI6LUCLPX",
        "aws_secret_access_key": "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"
    }
    try:
        clean(awsConf, kwargs["path"], logger)
        return {"status": "success"}
    except Exception as e:
        logger.error(traceback.format_exc())
        raise


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
