# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""
from phjob import execute
import click


@click.command()
@click.option('--a')
@click.option('--b')
def debug_execute(a, b):
	execute(a, b)


if __name__ == '__main__':
    debug_execute()
