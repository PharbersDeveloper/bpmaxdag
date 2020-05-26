# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This module document the usage of class pharbers command context,
"""
import os
from phexceptions.phexceptions import exception_file_already_exist, PhException, exception_file_not_exist, \
    exception_function_not_implement
from phconfig.phconfig import PhYAMLConfig
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
        self.job_path = path
        self.combine_path = path
        self.dag_path = path
        self.name = ""
        self.job_prefix = "phjobs"
        self.combine_prefix = "phcombines"
        self.dag_prefix = "phdags"

    def execute(self):
        self.check_dir()
        if self.cmd == "create":
            self.command_create_exec()
        elif self.cmd == "combine":
            self.command_combine_exec()
        elif self.cmd == "run":
            self.command_run_exec()
        elif self.cmd == "dag":
            self.command_dag_exec()
        else:
            self.command_publish_exec()

    def get_destination_path(self):
        self.job_path = os.getcwd() + "/" + self.job_prefix + "/" + self.name
        self.combine_path = os.getcwd() + "/" + self.combine_prefix + "/" + self.name
        self.dag_path = os.getcwd() + "/" + self.dag_prefix + "/"
        if self.cmd == "create":
            return os.getcwd() + "/" + self.job_prefix + "/" + self.name
        elif self.cmd == "combine":
            return os.getcwd() + "/" + self.combine_prefix + "/" + self.name
        elif self.cmd == "dag":
            return os.getcwd() + "/" + self.combine_prefix + "/" + self.name
        elif self.cmd == "run":
            return os.getcwd() + "/" + self.job_prefix + "/" + self.name
        else:
            raise Exception("Something goes wrong!!!")

    def check_dag_dir(self, dag_id):
        if os.path.exists(self.dag_path + "/" + dag_id):
            raise exception_file_already_exist

    def check_dir(self):
        if "/" not in self.path:
            self.name = self.path
            self.path = self.get_destination_path()
        try:
            if (self.cmd == "create") | (self.cmd == "combine"):
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
        # w.write("@click.command()\n")
        # for arg in config.spec.containers.args:
        #     w.write("@click.option('--" + arg.key + "')\n")
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
            if line == "$alfred_debug_execute\n":
                e.write("@click.command()\n")
                for arg in config.spec.containers.args:
                    e.write("@click.option('--" + arg.key + "')\n")
                # e.write("def debug_execute():\n")
                e.write("def debug_execute(")
                for arg_index in range(len(config.spec.containers.args)):
                    arg = config.spec.containers.args[arg_index]
                    if arg_index == len(config.spec.containers.args) - 1:
                        e.write(arg.key)
                    else:
                        e.write(arg.key + ", ")
                e.write("):\n")
                e.write("\texecute(")
                for arg_index in range(len(config.spec.containers.args)):
                    arg = config.spec.containers.args[arg_index]
                    if arg_index == len(config.spec.containers.args) - 1:
                        e.write(arg.key)
                    else:
                        e.write(arg.key + ", ")
                e.write(")\n")
            else:
                e.write(line)

        f.close()
        e.close()

    def command_combine_exec(self):
        print("command combine")
        config = PhYAMLConfig(self.path)
        template_path = os.getcwd() + "/phcontext/template/"
        subprocess.call(["mkdir", "-p", self.path])
        # subprocess.call(["cp", "-rf", template_path + "session", self.path + "/session"])
        subprocess.call(["cp", template_path + "phdag.yaml", self.path + "/phdag.yaml"])

    def command_publish_exec(self):
        print("publish")
        config = PhYAMLConfig(self.path)

    def command_run_exec(self):
        print("run")
        config = PhYAMLConfig(self.job_path)
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

    def command_dag_exec(self):
        print("command dag")
        config = PhYAMLConfig(self.combine_path, "/phdag.yaml")
        config.load_yaml()
        self.check_dag_dir(config.spec.dag_id)
        # subprocess.call(["mkdir", "-p", self.path])

        template_path = os.getcwd() + "/phcontext/template/"
        w = open(self.dag_path + "/ph_dag_" + config.spec.dag_id + ".py", "a")
        f = open(template_path + "/phgraphtemp.tmp", "r")
        for line in f:
            if line == "$alfred_import_jobs\n":
                for j in config.spec.jobs:
                    w.write("from phjobs." + j.name + ".phjob import execute as " + j.name + "\n")
            else:
                w.write(
                    line.replace("$alfred_dag_owner", str(config.spec.owner)) \
                        .replace("$alfred_email_on_failure", str(config.spec.email_on_failure)) \
                        .replace("$alfred_email_on_retry", str(config.spec.email_on_retry)) \
                        .replace("$alfred_email", str(config.spec.email)) \
                        .replace("$alfred_retries", str(config.spec.retries)) \
                        .replace("$alfred_retry_delay", str(config.spec.retry_delay)) \
                        .replace("$alfred_dag_id", str(config.spec.dag_id)) \
                        .replace("$alfred_schedule_interval", str(config.spec.schedule_interval)) \
                        .replace("$alfred_description", str(config.spec.description)) \
                        .replace("$alfred_dag_timeout", str(config.spec.dag_timeout)) \
                        .replace("$alfred_start_date", str(config.spec.start_date))
                )
        f.close()
        jf = open(template_path + "/phDagJob.tmp", "r")
        for jt in config.spec.jobs:
            jf.seek(0)
            for line in jf:
                w.write(
                    line.replace("$alfred_command", str(jt.command)) \
                        .replace("$alfred_job_path", str(self.job_path[0:self.job_path.rindex("/") + 1]))
                        .replace("$alfred_name", str(jt.name))
                )
            subprocess.call(["cp",
                             self.job_path[0:self.job_path.rindex("/") + 1] + jt.name + "/phconf.yaml",
                             self.dag_path + jt.name + ".yaml"])
        jf.close()

        w.write(config.spec.linkage)
        w.write("\n")
        w.close()
