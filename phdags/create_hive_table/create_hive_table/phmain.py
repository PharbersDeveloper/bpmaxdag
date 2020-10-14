# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
from phjob import execute
import click


@click.command()
@click.option('--input_file_format')
@click.option('--input_path')
@click.option('--output_file_format')
@click.option('--output_path')
@click.option('--save_mode')
@click.option('--table_name')
def debug_execute(input_file_format, input_path, output_file_format, output_path, save_mode, table_name):
	execute(input_file_format, input_path, output_file_format, output_path, save_mode, table_name)

if __name__ == '__main__':
    debug_execute()

