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
@click.option('--model_month_right')
@click.option('--model_month_left')
@click.option('--current_year')
@click.option('--current_month')
@click.option('--a')
@click.option('--b')
def debug_execute(owner, run_id, job_id, a, b, max_path, project_name, out_path, out_dir, model_month_right, model_month_left, current_year, current_month):
    execute(max_path, project_name, out_path, out_dir, model_month_right, model_month_left, current_year, current_month)
if __name__ == '__main__':
    debug_execute()
