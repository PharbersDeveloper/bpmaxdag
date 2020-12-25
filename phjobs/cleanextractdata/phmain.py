# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""

from phjob import execute
import click


@click.command()
@click.option('--start')
@click.option('--end')
@click.option('--run_id')
@click.option('--job_id')
@click.option('--version')
@click.option('--max_result_path')
@click.option('--cleancube_result_path')
def debug_execute(**kwargs):
	execute(**kwargs)


if __name__ == '__main__':
    debug_execute()
