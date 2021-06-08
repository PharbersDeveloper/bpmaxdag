# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
from phjob import execute
import click


@click.command()
@click.option('--owner')
@click.option('--run_id')
@click.option('--job_id')
@click.option('--max_path')
@click.option('--project_name')
@click.option('--out_path')
@click.option('--out_dir')
@click.option('--panel_path')
@click.option('--universe_path')
@click.option('--doi')
@click.option('--product_input')
@click.option('--model_month_left')
@click.option('--model_month_right')
@click.option('--arg_year')
@click.option('--a')
@click.option('--b')

def debug_execute(owner, run_id, job_id, a, b, max_path, project_name, out_path, out_dir, panel_path, universe_path, doi, product_input, model_month_left, model_month_right, arg_year):
	execute(max_path, project_name, out_path, out_dir, panel_path, universe_path, doi, product_input, model_month_left, model_month_right, arg_year)


if __name__ == '__main__':
    debug_execute()

