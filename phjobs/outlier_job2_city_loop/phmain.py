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
@click.option('--out_path')
@click.option('--out_dir')
@click.option('--doi')
@click.option('--product_input')
@click.option('--cities')
@click.option('--num_ot_max')
@click.option('--sample_max')
@click.option('--a')
@click.option('--b')
def debug_execute(job_id, a, b, max_path, project_name, out_path, out_dir, doi, product_input, cities, num_ot_max, sample_max):
	execute(max_path, project_name, out_path, out_dir, doi, product_input, cities, num_ot_max, sample_max)


if __name__ == '__main__':
    debug_execute()

