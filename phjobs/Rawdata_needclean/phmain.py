# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""
from phjob import execute
import click
@click.command()
@click.option('--job_id')
@click.option('--max_path')
@click.option('--project_name')
@click.option('--outdir')
@click.option('--minimum_product_sep')
@click.option('--minimum_product_columns')
@click.option('--test')
@click.option('--a')
@click.option('--b')
def debug_execute(job_id, a, b, max_path, project_name, outdir, minimum_product_sep, minimum_product_columns, test):
	execute(max_path, project_name, outdir, minimum_product_sep, minimum_product_columns, test)
if __name__ == '__main__':
    debug_execute()
