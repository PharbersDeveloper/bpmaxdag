# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
from phjob import execute
import click


@click.command()
@click.option('--extract_path')
@click.option('--extract_file')
@click.option('--update_max')
def debug_execute(extract_path, extract_file, update_max):
	execute(extract_path, extract_file, update_max)


if __name__ == '__main__':
    debug_execute()

