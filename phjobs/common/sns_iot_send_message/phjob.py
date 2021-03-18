# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
import boto3
import base64
import json

from phcli.ph_aws.ph_sts import PhSts
from phcli import define_value as dv


def execute(kwargs):
    defalut_sns_arn_prefix = 'arn:aws-cn:sns:cn-northwest-1:444603803904:'
    phsts = PhSts().assume_role(
        base64.b64decode(dv.ASSUME_ROLE_ARN).decode(),
        dv.ASSUME_ROLE_EXTERNAL_ID
    )        
    
    dict_message = eval(kwargs['message'])
    json_message = json.dumps(dict_message)
    sns_client = boto3.client('sns', **phsts.get_cred())
    sns_client.publish(
        TopicArn= defalut_sns_arn_prefix + kwargs['sns_topic_name'],
        Message=json_message
    )


if __name__ == '__main__':
    kwargs = {
        'message' : {
            "topic": "test/1",
            "message": "{\"key11\": \"value11\"}"
        },
        'sns_topic_name' : 'PH_NOTICE_IOT' 
    }
    execute(kwargs)
