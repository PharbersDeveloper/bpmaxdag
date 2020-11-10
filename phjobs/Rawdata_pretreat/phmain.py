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
@click.option('--history_raw_data_path')
@click.option('--if_two_source')
@click.option('--cut_time_left')
@click.option('--cut_time_right')
@click.option('--raw_data_path')
@click.option('--test')
def debug_execute(max_path, project_name, outdir, history_raw_data_path, if_two_source, cut_time_left, cut_time_right, raw_data_path, test):
	execute(max_path, project_name, outdir, history_raw_data_path, if_two_source, cut_time_left, cut_time_right, raw_data_path, test)
if __name__ == '__main__':
    debug_execute()
