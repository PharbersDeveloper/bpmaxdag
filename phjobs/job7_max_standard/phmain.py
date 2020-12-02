# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""
from phjob import execute
import click
@click.command()
@click.option('--owner')
@click.option('--run_id')
@click.option('--job_id')
@click.option('--max_path')
@click.option('--extract_path')
@click.option('--project_name')
@click.option('--max_path_list')
@click.option('--out_dir')
@click.option('--a')
@click.option('--b')
def debug_execute(owner, run_id, job_id, a, b, max_path, extract_path, project_name, max_path_list, out_dir):
	execute(max_path, extract_path, project_name, max_path_list, out_dir)
if __name__ == '__main__':
    debug_execute()
