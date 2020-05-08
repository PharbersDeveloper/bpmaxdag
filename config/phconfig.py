# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This module document the YAML Config for Pharbers jobs
"""
import yaml
from config.phmetadata import PhYAMLMetadata
from config.phspec import PhYAMLSpec
from phcontext.phexceptions import exception_function_not_implement


class PhYAMLConfig(object):
    def __init__(self, path, name=""):
        self.path = path
        self.name = name
        self.apiVersion = ""
        self.kind = ""
        self.metadata = ""
        self.spec = ""

    def dict2obj(self, dt):
        self.__dict__.update(dt)

    def load_yaml(self):
        f = open(self.path + "/phconf.yaml")
        y = yaml.safe_load(f)
        self.dict2obj(y)
        if self.kind == "PhJob":
            self.metadata = PhYAMLMetadata(self.metadata)
            self.spec = PhYAMLSpec(self.spec)
        else:
            raise exception_function_not_implement
