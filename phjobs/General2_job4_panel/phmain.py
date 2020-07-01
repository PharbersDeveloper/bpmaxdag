# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
from phjob import execute
import click


@click.command()
@click.option('--max_path')
@click.option('--project_name')
@click.option('--model_month_left')
@click.option('--model_month_right')
@click.option('--current_year')
@click.option('--current_month')
@click.option('--paths_foradding')
@click.option('--not_arrived_path')
@click.option('--unpublished_path')
@click.option('--monthly_update')
@click.option('--panel_for_union_dir')
@click.option('--out_path')
@click.option('--out_dir')
@click.option('--need_test')
def debug_execute(max_path, project_name, model_month_left, model_month_right, current_year, current_month, paths_foradding, not_arrived_path, unpublished_path, monthly_update, panel_for_union_dir, out_path, out_dir, need_test):
	execute(max_path, project_name, model_month_left, model_month_right, current_year, current_month, paths_foradding, not_arrived_path, unpublished_path, monthly_update, panel_for_union_dir, out_path, out_dir, need_test)


if __name__ == '__main__':
	debug_execute()

