# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
from phjob import execute
import click


@click.command()
@click.option('--input_path')
@click.option('--output_path')
@click.option('--table_name')
def debug_execute(input_path, output_path, table_name):
	execute(input_path, output_path, table_name)

if __name__ == '__main__':
    debug_execute()

