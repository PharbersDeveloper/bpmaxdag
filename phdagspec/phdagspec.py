# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This module document the YAML Job Args for Pharbers jobs
"""
from phdagjobs.phdagjobs import PhYAMLDAGJobs


class PhYAMLDAGSpec(object):
    def __init__(self, dt):
        self.description = ""
        self.start_date = 1
        self.schedule_interval = ""
        self.dag_timeout = 60
        self.email = []
        self.email_on_failure = ""
        self.email_on_retry = ""
        self.retries = 1
        self.retry_delay = "minutes=5",
        self.owner = ""
        self.linkage = ""
        self.dag_id = ""
        self.dag_tag = ""
        self.jobs = ""
        self.dict2obj(dt)

        targs = []
        for i in range(len(self.jobs)):
            targs.append(PhYAMLDAGJobs(self.jobs[i]))
        self.jobs = targs

    def dict2obj(self, dt):
        self.__dict__.update(dt)
