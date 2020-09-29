# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""
from phjob import execute
import click
@click.command()
@click.option('--max_path')
@click.option('--extract_path')
@click.option('--project_name')
@click.option('--max_path_list')
@click.option('--out_dir')
def debug_execute(max_path, extract_path, project_name, max_path_list, out_dir):
	execute(max_path, extract_path, project_name, max_path_list, out_dir)
if __name__ == '__main__':
    debug_execute()
