# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This module document the YAML Job Args for Pharbers jobs
"""


class PhYAMLJobOutputs(object):
    def __init__(self, dt):
        self.key = ""
        self.value = ""
        self.dict2obj(dt)

    def dict2obj(self, dt):
        self.__dict__.update(dt)
