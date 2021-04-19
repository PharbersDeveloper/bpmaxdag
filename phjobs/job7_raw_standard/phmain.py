# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""
from phjob import execute
import click
@click.command()
@click.option('--owner')
@click.option('--dag_name')
@click.option('--job_full_name')
@click.option('--run_id')
@click.option('--job_id')
@click.option('--max_path')
@click.option('--extract_path')
@click.option('--project_name')
@click.option('--if_two_source')
@click.option('--out_dir')
@click.option('--minimum_product_sep')
@click.option('--minimum_product_columns')
@click.option('--a')
@click.option('--b')
def debug_execute(owner, dag_name, job_full_name, run_id, job_id, a, b, max_path, extract_path, project_name, if_two_source, out_dir, minimum_product_sep, minimum_product_columns):
	execute(max_path, extract_path, project_name, if_two_source, out_dir, minimum_product_sep, minimum_product_columns)
if __name__ == '__main__':
    debug_execute()
