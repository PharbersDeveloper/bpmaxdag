# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
from phjob import execute
import click


@click.command()
@click.option('--max_path')
@click.option('--max_path_local')
@click.option('--project_name')
@click.option('--if_base')
@click.option('--time_left')
@click.option('--time_right')
@click.option('--left_models')
@click.option('--time_left_models')
@click.option('--rest_models')
@click.option('--time_rest_models')
@click.option('--all_models')
@click.option('--other_models')
@click.option('--test_out_path')
def debug_execute(max_path, max_path_local, project_name, if_base, time_left, time_right, left_models, time_left_models, rest_models, time_rest_models, all_models, other_models, test_out_path):
	execute(max_path, max_path_local, project_name, if_base, time_left, time_right, left_models, time_left_models, rest_models, time_rest_models, all_models, other_models, test_out_path)


if __name__ == '__main__':
    debug_execute()

