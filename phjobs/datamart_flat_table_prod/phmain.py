# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""
from phjob import execute
import click
@click.command()
@click.option('--origin_prod_path')
@click.option('--national_drug_code_domestically_path')
@click.option('--national_drug_code_imported_path')
@click.option('--flat_table_prod_path')
def debug_execute(**kwargs):
	execute(**kwargs)
if __name__ == '__main__':
    debug_execute()
