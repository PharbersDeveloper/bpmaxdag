# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""
from phjob import execute
import click
@click.command()
@click.option('--max_path')
@click.option('--project_name')
@click.option('--outdir')
@click.option('--minimum_product_sep')
@click.option('--minimum_product_columns')
def debug_execute(max_path, project_name, outdir, minimum_product_sep, minimum_product_columns):
	execute(max_path, project_name, outdir, minimum_product_sep, minimum_product_columns)
if __name__ == '__main__':
    debug_execute()
