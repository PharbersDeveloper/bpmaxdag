# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
import boto3
import uuid
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL


def execute(**kwargs):
    """
        please input your code below
        """
    logger = phs3logger(kwargs["job_id"], LOG_DEBUG_LEVEL)
    logger.info("当前 owner 为 " + str(kwargs["owner"]))
    logger.info("当前 run_id 为 " + str(kwargs["run_id"]))
    logger.info("当前 job_id 为 " + str(kwargs["job_id"]))

    emailConf = {
        "email": kwargs["email"],
        "subject": kwargs["subject"],
        "type": kwargs["content_type"],
        "content": kwargs["content"]
    }
    awsConf = {
        "aws_access_key_id": "AKIAWPBDTVEAPUSJJMWN",
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
