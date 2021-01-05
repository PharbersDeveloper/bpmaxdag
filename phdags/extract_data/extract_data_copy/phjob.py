# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
import boto3
import uuid
import traceback
from ph_logs.ph_logs import phs3logger


def execute(**kwargs):
    """
        please input your code below
        get spark session: spark = kwargs["spark"]()
    """
    spark = kwargs["spark"]()
    logger = phs3logger(kwargs["job_id"])
    awsConf = {
        "aws_access_key_id": "AKIAWPBDTVEAI6LUCLPX",
        "aws_secret_access_key": "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"
    }

    try:
        copy(kwargs["from"], kwargs["to"], awsConf)
        return {"status": "success"}
    except Exception as e:
        logger.error(traceback.format_exc())
        raise e


def copy(target, to, awsConf):
    prefix = "/".join(target.replace("//", "").split("/")[1:])
    s3 = boto3.resource('s3',
                        aws_access_key_id=awsConf["aws_access_key_id"],
                        aws_secret_access_key=awsConf["aws_secret_access_key"],
                        region_name="cn-northwest-1")
    bucket = s3.Bucket("ph-stream")
    for obj in bucket.objects.filter(Prefix=prefix):
        target_key = obj.key[obj.key.index(
            "extract_data_out") + len("extract_data_out"):]
        copy_source = {
            'Bucket': 'ph-stream',
            'Key': obj.key
        }
        obj = bucket.Object(
            "/".join(to.replace("//", "").split("/")[1:]) + target_key)
        obj.copy(copy_source)
