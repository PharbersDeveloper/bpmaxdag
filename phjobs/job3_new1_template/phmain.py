# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""
from phjob import execute
import click
@click.command()
@click.option('--max_path')
@click.option('--project_name')
@click.option('--out_path')
@click.option('--out_dir')
@click.option('--current_year')
@click.option('--current_month')
def debug_execute(max_path, project_name, out_path, out_dir, current_year, current_month):
	execute(max_path, project_name, out_path, out_dir, current_year, current_month)
if __name__ == '__main__':
    debug_execute()
