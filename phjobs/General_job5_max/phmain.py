# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
from phjob import execute
import click


@click.command()
@click.option('--max_path')
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
@click.option('--universe_choice')
@click.option('--out_path')
@click.option('--need_test')
def debug_execute(max_path, project_name, if_base, time_left, time_right, left_models, time_left_models, rest_models, time_rest_models, all_models, other_models, universe_choice, out_path, need_test):
	execute(max_path, project_name, if_base, time_left, time_right, left_models, time_left_models, rest_models, time_rest_models, all_models, other_models, universe_choice, out_path, need_test)


if __name__ == '__main__':
    debug_execute()
