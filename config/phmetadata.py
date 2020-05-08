# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This module document the YAML Matadata for Pharbers jobs
"""


class PhYAMLMetadata(object):
    def __init__(self, dt):
        self.labels = ""
        self.name = ""
        self.description = ""
        self.dict2obj(dt)

    def dict2obj(self, dt):
        self.__dict__.update(dt)
