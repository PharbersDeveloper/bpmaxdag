# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
from phjob import execute
import click


@click.command()
@click.option('--job_id')
@click.option('--extract_path')
@click.option('--extract_file')
@click.option('--data_type')
@click.option('--a')
@click.option('--b')
def debug_execute(job_id, a, b, extract_path, extract_file, data_type):
	execute(extract_path, extract_file, data_type)


if __name__ == '__main__':
    debug_execute()

