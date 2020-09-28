# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
from phjob import execute
import click


@click.command()
@click.option('--out_path')
@click.option('--cpa_raw_data_path')
def debug_execute(out_path, cpa_raw_data_path):
	execute(out_path, cpa_raw_data_path)


if __name__ == '__main__':
    debug_execute()

