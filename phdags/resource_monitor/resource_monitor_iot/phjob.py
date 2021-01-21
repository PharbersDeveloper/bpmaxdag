# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

import abc
import boto3
from awscrt import io
from awscrt import mqtt
from awsiot import mqtt_connection_builder
import uuid
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL



class IOT(metaclass=abc.ABCMeta):
    client_id = ""

    @abc.abstractmethod
    def build(self):
        """Builder"""
        pass

    @abc.abstractmethod
    def open(self):
        """Open client"""
        pass

    @abc.abstractmethod
    def close(self):
        """Close client"""
        pass

    @abc.abstractmethod
    def publish(self, topic, message):
        """Iot Publish Message"""
        pass


class MQTT(IOT):
    def __init__(self):
        self.__connection = None
        self.__endpoint = ""
        self.__cert = ""
        self.__key = ""
        self.__ca = ""
        self.__clean_session = False
        self.__keep_alive_secs = 6
        self.__client_bootstrap = None

    def set_end_point(self, endpoint):
        self.__endpoint = endpoint

    def set_cert(self, cert):
        self.__cert = cert

    def set_key(self, key):
        self.__key = key

    def set_ca(self, ca):
        self.__ca = ca

    def set_clean_session(self, flag):
        self.__clean_session = flag

    def set_keep_alive(self, secs):
        self.__keep_alive_secs = secs

    def set_client_bootstrap(self, bootstrap):
        self.__client_bootstrap = bootstrap

    def build(self):
        self.__connection = mqtt_connection_builder.mtls_from_bytes(
            cert_bytes=self.__cert.encode("utf-8"),
            pri_key_bytes=self.__key.encode("utf-8"),
            ca_bytes=self.__ca.encode("utf-8"),
            endpoint=self.__endpoint,
            client_id=self.client_id,
            client_session=self.__clean_session,
            keep_alive_secs=self.__keep_alive_secs,
            client_bootstrap=self.__client_bootstrap
        )

    def open(self):
        connect_future = self.__connection.connect()
        connect_future.result()

    def close(self):
        disconnect_future = self.__connection.disconnect()
        disconnect_future.result()

    def publish(self, topic, message):
        self.__connection.publish(topic=topic, payload=message, qos=mqtt.QoS.AT_LEAST_ONCE)

def getObject(bk_name, key):
    s3 = boto3.client("s3",
                      aws_access_key_id="AKIAWPBDTVEAPUSJJMWN",
                      aws_secret_access_key="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",
                      region_name="cn-northwest-1")
    return s3.get_object(Bucket=bk_name, Key=key)["Body"].read().decode("utf-8")


def execute(**kwargs):
    """
        please input your code below
    """
    logger = phs3logger(kwargs["job_id"], LOG_DEBUG_LEVEL)
    logger.info("当前 owner 为 " + str(kwargs["owner"]))
    logger.info("当前 run_id 为 " + str(kwargs["run_id"]))
    logger.info("当前 job_id 为 " + str(kwargs["job_id"]))

    logger.info(kwargs["topic"])
    logger.info(kwargs["message"])

    event_loop_group = io.EventLoopGroup(1)
    host_resolver = io.DefaultHostResolver(event_loop_group)
    client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)

    mqtt = MQTT()
    mqtt.client_id = str(uuid.uuid4())
    mqtt.set_ca(getObject("ph-platform", "2020-11-11/certificaties/IoT/all/root-CA.crt"))
    mqtt.set_key(getObject("ph-platform", "2020-11-11/certificaties/IoT/all/iot-private.pem.key"))
    mqtt.set_cert(getObject("ph-platform", "2020-11-11/certificaties/IoT/all/iot-certificate.pem.crt"))
    mqtt.set_end_point("a23ve0kwl75dll-ats.iot.cn-northwest-1.amazonaws.com.cn")
    mqtt.set_client_bootstrap(client_bootstrap)
    mqtt.build()
    mqtt.open()
    mqtt.publish(kwargs["topic"], kwargs["message"])
    mqtt.close()

    return {}
