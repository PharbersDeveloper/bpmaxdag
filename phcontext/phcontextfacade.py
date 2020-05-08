# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This module document the usage of class pharbers command context,
"""
import os
from phcontext.phexceptions import exception_file_already_exist, PhException, exception_file_not_exist, \
    exception_function_not_implement
from config import PhYAMLConfig
import subprocess


class PhContextFacade(object):
    """The Pharbers Max Job Command Line Interface (CLI) Command Context Entry

        Args:
            cmd: the command that you want to process
            path: the directory that you want to process
    """

    def __init__(self, cmd, path):
        self.cmd = cmd
        self.path = path
        self.name = ""
        self.prefix = "phjobs"

    def execute(self):
        self.check_dir()
        if self.cmd == "create":
            self.command_create_exec()
        elif self.cmd == "combine":
            self.command_combine_exec()
        elif self.cmd == "run":
            self.command_run_exec()
        else:
            self.command_publish_exec()

    def check_dir(self):
        if "/" not in self.path:
            self.name = self.path
            self.path = os.getcwd() + "/" + self.prefix + "/" + self.name
        try:
            if self.cmd == "create":
                if os.path.exists(self.path):
                    raise exception_file_already_exist
            else:
                if not os.path.exists(self.path):
                    raise exception_file_not_exist
        except PhException as e:
            print(e.msg)
            raise e

    def command_create_exec(self):
        print("command create")
        config = PhYAMLConfig(self.path)
        template_path = os.getcwd() + "/phcontext/template/"
        subprocess.call(["mkdir", "-p", self.path])
        # subprocess.call(["cp", "-rf", template_path + "session", self.path + "/session"])
        subprocess.call(["cp", template_path + "__init__.py", self.path + "/__init__.py"])
        subprocess.call(["cp", template_path + "phjob.py", self.path + "/phjob.py"])
        subprocess.call(["cp", template_path + "phconf.yaml", self.path + "/phconf.yaml"])

        config.load_yaml()
        w = open(self.path + "/phjob.py", "a")
        w.write("@click.command()\n")
        for arg in config.spec.containers.args:
            w.write("@click.option('--" + arg.key + "')\n")
        w.write("def execute(")
        for arg_index in range(len(config.spec.containers.args)):
            arg = config.spec.containers.args[arg_index]
            if arg_index == len(config.spec.containers.args) - 1:
                w.write(arg.key)
            else:
                w.write(arg.key + ", ")
        w.write("):\n")
        w.write('\t"""\n')
        w.write('\t\tplease input your code below\n')
        w.write('\t"""\n')
        w.close()

        e = open(self.path + "/phmain.py", "w")
        f = open(template_path + "phmain.tmp")
        s = []
        for arg in config.spec.containers.args:
            s.append(arg.key)
        for line in f:
            e.write(line)
        f.close()
        e.close()

    def command_combine_exec(self):
        print("combine")
        config = PhYAMLConfig(self.path)

    def command_publish_exec(self):
        print("publish")
        config = PhYAMLConfig(self.path)

    def command_run_exec(self):
        print("run")
        config = PhYAMLConfig(self.path, self.name)
        config.load_yaml()
        if config.spec.containers.repository == "local":
            entry_point = config.spec.containers.code
            if "/" not in entry_point:
                entry_point = self.path + "/" + entry_point
                cb = ["python", entry_point]
                for arg in config.spec.containers.args:
                    cb.append("--" + arg.key + "=" + str(arg.value))
                subprocess.call(cb)
        else:
            raise exception_function_not_implement
