# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""
from phjob import execute
import click
@click.command()
@click.option('--dimensions')
@click.option('--id')
def debug_execute(**args):
	execute(**args)
if __name__ == '__main__':
    debug_execute()
