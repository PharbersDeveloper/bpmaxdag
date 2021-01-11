# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""
from phjob import execute
import click
@click.command()
@click.option('--version')
@click.option('--run_id')
@click.option('--job_id')
@click.option('--dimensions')
@click.option('--cuboids_path')
@click.option('--lattices_path')
@click.option('--dimensions_path')
def debug_execute(**args):
	execute(**args)
if __name__ == '__main__':
    debug_execute()
