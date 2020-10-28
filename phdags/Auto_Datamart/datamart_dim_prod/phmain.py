# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
from phjob import execute
import click


@click.command()
@click.option('--prod_input_path')
@click.option('--mnfs_input_path')
@click.option('--prod_output_path')
def debug_execute(**kwargs):
	execute(**kwargs)


if __name__ == '__main__':
    debug_execute()

