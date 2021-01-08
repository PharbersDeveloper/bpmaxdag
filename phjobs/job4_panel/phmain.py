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
@click.option('--model_month_left')
@click.option('--model_month_right')
@click.option('--if_others')
@click.option('--current_year')
@click.option('--current_month')
@click.option('--paths_foradding')
@click.option('--not_arrived_path')
@click.option('--published_path')
@click.option('--monthly_update')
@click.option('--panel_for_union')
@click.option('--out_path')
@click.option('--out_dir')
@click.option('--need_test')
@click.option('--add_47')
@click.option('--a')
@click.option('--b')
def debug_execute(owner, run_id, job_id, a, b, max_path, project_name, model_month_left, model_month_right, 
if_others, current_year, current_month, paths_foradding, not_arrived_path, published_path, monthly_update, 
panel_for_union, out_path, out_dir, need_test, add_47):
	execute(max_path, project_name, model_month_left, model_month_right, 
	if_others, current_year, current_month, paths_foradding, not_arrived_path, published_path, monthly_update, 
	panel_for_union, out_path, out_dir, need_test, add_47)


if __name__ == '__main__':
	debug_execute()

