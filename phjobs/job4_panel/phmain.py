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
@click.option('--model_month_left')
@click.option('--model_month_right')
@click.option('--paths_foradding')
@click.option('--test_out_path')

def debug_execute(max_path, max_path_local, project_name, model_month_left, model_month_right, paths_foradding, test_out_path):
	execute(max_path, max_path_local, project_name, model_month_left, model_month_right, paths_foradding, test_out_path)


if __name__ == '__main__':
    debug_execute()

