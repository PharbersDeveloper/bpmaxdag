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
@click.option('--year')
@click.option('--month')
@click.option('--three')
@click.option('--twelve')
def debug_execute(max_path, project_name, outdir, minimum_product_sep, minimum_product_columns, year, month, three, twelve):
	execute(max_path, project_name, outdir, minimum_product_sep, minimum_product_columns, year, month, three, twelve)
if __name__ == '__main__':
    debug_execute()
