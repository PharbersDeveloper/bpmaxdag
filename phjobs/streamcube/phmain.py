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
def debug_execute(start, end, replace):
	execute(start, end, replace, process_row)


if __name__ == '__main__':
    debug_execute()
