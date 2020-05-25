# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This module document the YAML Config container for Pharbers jobs
"""
from phconfig.phjobargs import PhYAMLJobArgs
from phconfig.pyoutputs import PhYAMLJobOutputs


class PhYAMLContainer(object):
    def __init__(self, dt):
        self.repository = ""
        self.code = ""
        self.config = ""
        self.args = []
        self.outputs = []
        self.dict2obj(dt)

        targs = []
        for i in range(len(self.args)):
            targs.append(PhYAMLJobArgs(self.args[i]))
        self.args = targs

        toutput = []
        for i in range(len(self.outputs)):
            toutput.append(PhYAMLJobOutputs(self.outputs[i]))
        self.outputs = toutput

    def dict2obj(self, dt):
        self.__dict__.update(dt)
