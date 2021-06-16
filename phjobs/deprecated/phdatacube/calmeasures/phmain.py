# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""
from phjob import execute
import click
@click.command()
@click.option('--run_id')
@click.option('--job_id')
@click.option('--version')
@click.option('--final_result_path')
@click.option('--ver_measures_content_path')
def debug_execute(**kwargs):
    execute(**kwargs)

if __name__ == '__main__':
    debug_execute()
