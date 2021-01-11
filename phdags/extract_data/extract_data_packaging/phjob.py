# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
import boto3
import os
import zipfile
import time
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
        "aws_access_key_id": "AKIAWPBDTVEAI6LUCLPX",
        "aws_secret_access_key": "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"
    }
    packaging(kwargs["from"], kwargs["out_suffix"], awsConf)
    return {}


def get_bucket_name(path):
    return path.replace("//", "/").split("/")[1]


def packaging(target, out_suffix, awsConf):
    prefix = "/".join(target.replace("//", "").split("/")[1:])
    s3 = boto3.resource('s3',
                        aws_access_key_id=awsConf["aws_access_key_id"],
                        aws_secret_access_key=awsConf["aws_secret_access_key"],
                        region_name="cn-northwest-1")
    bucket = s3.Bucket(get_bucket_name(target))
    # test
    local_path = "/root/{}"
    for obj in bucket.objects.filter(Prefix=prefix):
        download_path = local_path.format(
            obj.key[obj.key.find(out_suffix):])
        createFile(download_path)
        s3.meta.client.download_file(
            get_bucket_name(target), obj.key, download_path)

    createZip(local_path.format(out_suffix),
              local_path.format(out_suffix + ".zip"))

    s3.Object(
        get_bucket_name(target), prefix + "/" + out_suffix + ".zip").upload_file(local_path.format(out_suffix + ".zip"))


def createFile(path):
    fd = '/'.join(path.split("/")[:-1])
    folder = os.path.exists(fd)
    if not folder:
        os.makedirs(fd)
    file = open(path, 'w')
    file.close()


def createZip(filePath, savePath):
    fileList = []
    newZip = zipfile.ZipFile(savePath, 'w')
    for dirpath, dirnames, filenames in os.walk(filePath):
        for filename in filenames:
            fileList.append(os.path.join(dirpath, filename))
    for tar in fileList:
        newZip.write(tar, tar[len(filePath):])
    newZip.close()
