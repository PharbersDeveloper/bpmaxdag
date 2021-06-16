# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
import click
import traceback
from phjob import execute
from phcli.ph_logs.ph_logs import phs3logger
from phcli.ph_max_auto.ph_hook.ph_hook import exec_before, exec_after


@click.command()
@click.option('--owner')
@click.option('--run_id')
@click.option('--job_id')
@click.option('--version')
@click.option('--mole_spec_dim_version')
@click.option('--mnf_dim_version')
@click.option('--product_category_version')
@click.option('--product_rel_version')
@click.option('--input_path')
@click.option('--out_put')
@click.option('--mole_spec_dim_path')
@click.option('--mnf_dim_path')
@click.option('--product_category_path')
@click.option('--product_rel_path')
@click.option('--table_type')
@click.option('--table_name')
@click.option('--c')
@click.option('--d')
def debug_execute(**kwargs):
    try:
        args = {"name": "product_dimension"}
        outputs = ["c", "d"]

        args.update(kwargs)
        result = exec_before(**args)

        args.update(result if isinstance(result, dict) else {})
        result = execute(**args)

        args.update(result if isinstance(result, dict) else {})
        result = exec_after(outputs=outputs, **args)

        return result
    except Exception as e:
        logger = phs3logger(kwargs["job_id"])
        logger.error(traceback.format_exc())
        raise e


if __name__ == '__main__':
    debug_execute()


