# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
from phjob import execute
import click


@click.command()
@click.option('--in_cpa_path')
@click.option('--in_prod_path')
@click.option('--in_hr_path')
@click.option('--in_mhr_path')
@click.option('--out_path')
@click.option('--min_keys_lst')
def debug_execute(in_cpa_path, in_prod_path, in_hr_path, in_mhr_path, out_path, min_keys_lst):
	 execute(in_cpa_path, in_prod_path, in_hr_path, in_mhr_path, out_path, min_keys_lst)


if __name__ == '__main__':
	debug_execute()

