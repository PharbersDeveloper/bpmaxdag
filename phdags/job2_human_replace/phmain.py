# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
from phjob import execute
import click


@click.command()
@click.option('--out_path')
@click.option('--in_hrpackid_path')
def debug_execute(out_path, in_hrpackid_path):
	execute(out_path, in_hrpackid_path)


if __name__ == '__main__':
    debug_execute()

