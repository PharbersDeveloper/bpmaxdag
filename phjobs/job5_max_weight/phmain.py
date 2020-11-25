# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
from phjob import execute
import click


@click.command()
@click.option('--job_id')
@click.option('--max_path')
@click.option('--project_name')
@click.option('--if_base')
@click.option('--time_left')
@click.option('--time_right')
@click.option('--left_models')
@click.option('--left_models_time_left')
@click.option('--right_models')
@click.option('--right_models_time_right')
@click.option('--all_models')
@click.option('--universe_choice')
@click.option('--if_others')
@click.option('--out_path')
@click.option('--out_dir')
@click.option('--need_test')
@click.option('--a')
@click.option('--b')
def debug_execute(job_id, a, b, max_path, project_name, if_base, time_left, time_right, left_models, left_models_time_left, right_models, right_models_time_right, all_models, universe_choice, if_others, out_path, out_dir, need_test):
	execute(max_path, project_name, if_base, time_left, time_right, left_models, left_models_time_left, right_models, right_models_time_right, all_models, universe_choice, if_others, out_path, out_dir, need_test)


if __name__ == '__main__':
    debug_execute()
