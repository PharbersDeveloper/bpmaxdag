# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
from phjob import execute
import click


@click.command()
@click.option('--job_id')
@click.option('--max_path')
@click.option('--extract_path')
@click.option('--out_path')
@click.option('--out_suffix')
@click.option('--extract_file')
@click.option('--time_left')
@click.option('--time_right')
@click.option('--molecule')
@click.option('--atc')
@click.option('--project')
@click.option('--doi')
@click.option('--molecule_sep')
@click.option('--data_type')
@click.option('--a')
@click.option('--b')
def debug_execute(job_id, a, b, max_path, extract_path, out_path, out_suffix, extract_file, time_left, time_right, molecule, atc, 
project, doi, molecule_sep, data_type):
	execute(max_path, extract_path, out_path, out_suffix, extract_file, time_left, time_right, molecule, atc, 
	project, doi, molecule_sep, data_type)


if __name__ == '__main__':
    debug_execute()

