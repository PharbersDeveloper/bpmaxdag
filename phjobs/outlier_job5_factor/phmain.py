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
@click.option('--doi')
@click.option('--product_input')
@click.option('--fst_prd')
@click.option('--bias')
def debug_execute(max_path, project_name, out_path, out_dir, doi, product_input, fst_prd, bias):
	execute(max_path, project_name, out_path, out_dir, doi, product_input, fst_prd, bias)


if __name__ == '__main__':
    debug_execute()

