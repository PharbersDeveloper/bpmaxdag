# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
from phjob import execute
import click


@click.command()
@click.option('--correct_data_path')
@click.option('--test_data_path')
def debug_execute(correct_data_path, test_data_path):
    execute(correct_data_path, test_data_path)


if __name__ == '__main__':
    debug_execute()

