# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""
from phjob import execute
import click
@click.command()
@click.option('--max_path')
@click.option('--out_path')
@click.option('--project_name')
@click.option('--max_path_list')
def debug_execute(max_path, out_path, project_name, max_path_list):
	execute(max_path, out_path, project_name, max_path_list)
if __name__ == '__main__':
    debug_execute()
