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
@click.option('--pk')
@click.option('--mnf_name_mapping')

def debug_execute(in_prod_path, in_hr_path, out_path, pk, mnf_name_mapping):
	execute(in_prod_path, in_hr_path, out_path, pk, mnf_name_mapping)


if __name__ == '__main__':
    debug_execute()

