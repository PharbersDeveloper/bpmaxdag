# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This module document the YAML Config for Pharbers jobs
"""
import yaml

from phdagspec.phdagspec import PhYAMLDAGSpec
from phspec.phspec import PhYAMLSpec
from phmetadata.phmetadata import PhYAMLMetadata
from phexceptions.phexceptions import exception_function_not_implement


class PhYAMLConfig(object):
    def __init__(self, path, name="/phconf.yaml"):
        self.path = path
        self.name = name
        self.apiVersion = ""
        self.kind = ""
        self.metadata = ""
        self.spec = ""

    def dict2obj(self, dt):
        self.__dict__.update(dt)

    def load_yaml(self):
        f = open(self.path + self.name)
        y = yaml.safe_load(f)
        self.dict2obj(y)
        if self.kind == "PhJob":
            self.metadata = PhYAMLMetadata(self.metadata)
            self.spec = PhYAMLSpec(self.spec)
        elif self.kind == "PhDag":
            self.metadata = PhYAMLMetadata(self.metadata)
            self.spec = PhYAMLDAGSpec(self.spec)
        else:
            raise exception_function_not_implement
