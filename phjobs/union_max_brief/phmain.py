# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
from phjob import execute
import click


@click.command()
@click.option('--max_path')
@click.option('--path_for_extract_path')
def debug_execute(max_path, path_for_extract_path):
	execute(max_path, path_for_extract_path)


if __name__ == '__main__':
    debug_execute()

