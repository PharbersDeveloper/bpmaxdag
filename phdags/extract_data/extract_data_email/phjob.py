# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
import boto3
import uuid
from ph_logs.ph_logs import phs3logger


def execute(**kwargs):
    """
        please input your code below
        get spark session: spark = kwargs["spark"]()
    """
    spark = kwargs["spark"]()
    emailConf = {
        "email": kwargs["email"],
        "subject": kwargs["subject"],
        "type": kwargs["content_type"],
        "content": kwargs["content"]
    }
    awsConf = {
        "aws_access_key_id": "AKIAWPBDTVEAI6LUCLPX",
        "aws_secret_access_key": "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"
    }
    sqsConf = {
        "queue_name": "ph-notification.fifo"
    }
    result = send_sqs(emailConf, awsConf, sqsConf)
    if "ResponseMetadata" in result and result["ResponseMetadata"]["HTTPStatusCode"] == 200:
        return {"status": "success"}
    else:
        return {"status": "fail"}


def send_sqs(emailConf, awsConf, sqsConf):
    sqs = boto3.resource('sqs',
                         aws_access_key_id=awsConf["aws_access_key_id"],
                         aws_secret_access_key=awsConf["aws_secret_access_key"],
                         region_name="cn-northwest-1")
    queue = sqs.get_queue_by_name(QueueName=sqsConf["queue_name"])
    response = queue.send_message(
        MessageBody='SendEmailHandle',
        MessageGroupId='sqs-invoke',
        MessageDeduplicationId=str(uuid.uuid4()),
        MessageAttributes={
            "To": {
                "DataType": "String",
                "StringValue": emailConf["email"]
            },
            "Subject": {
                "DataType": "String",
                "StringValue": emailConf["subject"]
            },
            "ContentType": {
                "DataType": "String",
                "StringValue": emailConf["type"]
            },
            "Content": {
                "DataType": "String",
                "StringValue": emailConf["content"]
            }
        }
    )
    return response
