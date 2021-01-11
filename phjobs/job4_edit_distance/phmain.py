# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
from phjob import execute
import click


@click.command()
@click.option('--out_path')
@click.option('--pk')
def debug_execute(out_path, pk):
	execute(out_path, pk)


if __name__ == '__main__':
    debug_execute()

