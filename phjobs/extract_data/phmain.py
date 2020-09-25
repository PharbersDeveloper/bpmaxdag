# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
from phjob import execute
import click


@click.command()
@click.option('--max_path')
@click.option('--out_path')
@click.option('--out_suffix')
@click.option('--extract_file')
@click.option('--time_left')
@click.option('--time_right')
@click.option('--molecule')
@click.option('--atc')
@click.option('--project')
@click.option('--doi')
def debug_execute(max_path, out_path, out_suffix, extract_file, time_left, time_right, molecule, atc, project, doi):
	execute(max_path, out_path, out_suffix, extract_file, time_left, time_right, molecule, atc, project, doi)


if __name__ == '__main__':
    debug_execute()

