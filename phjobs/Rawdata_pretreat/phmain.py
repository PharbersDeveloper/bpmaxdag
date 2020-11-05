# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""
from phjob import execute
import click
@click.command()
@click.option('--max_path')
@click.option('--project_name')
@click.option('--outdir')
@click.option('--history_outdir')
@click.option('--if_two_source')
@click.option('--time_left')
@click.option('--time_right')
def debug_execute(max_path, project_name, outdir, history_outdir, if_two_source, time_left, time_right):
	execute(max_path, project_name, outdir, history_outdir, if_two_source, time_left, time_right)
if __name__ == '__main__':
    debug_execute()
