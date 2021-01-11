# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
from phjob import execute
import click


@click.command()
@click.option('--input_path')
@click.option('--cpa_prod_path')
@click.option('--cpa_hosp_path')
@click.option('--cpa_date_path')
@click.option('--cpa_etc_path')
@click.option('--output_path')
def debug_execute(**kwargs):
	execute(**kwargs)


if __name__ == '__main__':
    debug_execute()

