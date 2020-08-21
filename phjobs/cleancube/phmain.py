# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""

from phjob import execute
import click


@click.command()
@click.option('--start')
@click.option('--end')
@click.option('--replace')
@click.option('--job_id')
@click.option('--start_date')
@click.option('--source')
def debug_execute(start, end, replace, **kwargs):
	execute(start, end, replace, **kwargs)


if __name__ == '__main__':
    debug_execute()
