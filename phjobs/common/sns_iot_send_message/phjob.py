# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
import boto3
import base64
import json

from phcli.ph_aws.ph_sts import PhSts
from phcli import define_value as dv


def execute(**kwargs):
    defalut_sns_arn_prefix = 'arn:aws-cn:sns:cn-northwest-1:444603803904:'
    defalut_sns_topic_name = 'PH_NOTICE_IOT'
    phsts = PhSts().assume_role(
        base64.b64decode(dv.ASSUME_ROLE_ARN).decode(),
        dv.ASSUME_ROLE_EXTERNAL_ID
    )
    sns_message = {}
    sns_message['topic'] = kwargs['topic']
    message_str = base64.b64decode(kwargs['message']).decode()
    message_str = message_str.replace("$(status)", kwargs['pres_job_status']) \
                            .replace("$(errorMsg)", kwargs['job_error_msg'])

    message_dict = json.loads(message_str)
    if kwargs['pres_job_status'] == "success":
        if "errorMsg" in message_dict:
            del message_dict['errorMsg']
    else:
        if "message" in message_dict:
            del message_dict["message"]
            
    sns_message["message"] = json.dumps(message_dict)
    json_message = json.dumps(sns_message)

    sns_client = boto3.client('sns', **phsts.get_cred()) if phsts.get_cred() else boto3.client('sns')
    sns_client.publish(
        TopicArn=defalut_sns_arn_prefix + defalut_sns_topic_name,
        Message=json_message
    )


if __name__ == '__main__':
    
    # create test data
    # status == "success" or  error", if status == "error", errorMsg not null
    message = {
        "uuid": "5SCsFDZn09FT4QpaSP6y",
        "accountId": "5SCsFDZn09FT4QpaSP6y",
        "status": "$(status)",
        "message": "计算完成",
        "errorMsg": "计算错误，错误原因为: $(errorMsg)",
    }
    message_str = json.dumps(message)
    print(message_str)
    
    message_base64_str = base64.b64encode(message_str.encode()).decode()
    print(message_base64_str)

    # {
    #   "topic": "test/1"
    #   "message": message_base64_str
    # }
    
    
    kwargs = {
        "topic": "test/1",
        "message": message_base64_str,
        "pres_job_status": 'success',
        "job_error_msg": 'job计算失败'
    }
    execute(**kwargs)