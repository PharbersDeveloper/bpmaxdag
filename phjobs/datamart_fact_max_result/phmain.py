# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
from phjob import execute
import click


@click.command()
@click.option('--input_path')
@click.option('--max_result_prod_path')
@click.option('--max_result_hosp_path')
@click.option('--max_result_etc_path')
@click.option('--output_path')
def debug_execute(**kwargs):
	execute(**kwargs)


if __name__ == '__main__':
	debug_execute()

