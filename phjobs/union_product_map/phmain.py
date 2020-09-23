# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
from phjob import execute
import click


@click.command()
@click.option('--max_path')
@click.option('--project_list')
def debug_execute(max_path, project_list):
	execute(max_path, project_list)


if __name__ == '__main__':
    debug_execute()

