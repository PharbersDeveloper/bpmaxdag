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
@click.option('--c')
@click.option('--d')
def debug_execute(**kwargs):
    try:
        args = {"name": "extract"}
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
