# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This module document the YAML Config container for Pharbers jobs
"""
from phcontainer.phcontainer import PhYAMLContainer


class PhYAMLSpec(object):
    def __init__(self, dt):
        self.containers = ""
        self.dict2obj(dt)
        self.containers = PhYAMLContainer(self.containers)

    def dict2obj(self, dt):
        self.__dict__.update(dt)
