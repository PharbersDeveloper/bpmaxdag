# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
from phjob import execute
import click


@click.command()
@click.option('--in_prod_path')
@click.option('--in_hr_path')
@click.option('--out_path')
# @click.option('--prod_min_key_lst')

def debug_execute(in_prod_path, in_hr_path, out_path):
	execute(in_prod_path, in_hr_path, out_path)


if __name__ == '__main__':
    debug_execute()

