# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
import click
import traceback
from phjob import execute
from ph_logs.ph_logs import phs3logger
from ph_max_auto.ph_hook.ph_hook import exec_before, exec_after


@click.command()
@click.option('--owner')
@click.option('--run_id')
@click.option('--job_id')
@click.option('--input_report')
@click.option('--input_csv')
@click.option('--input_alfred')
@click.option('--output_flag')
@click.option('--output_actcode')
def debug_execute(**kwargs):
    try:
        args = {'name': 'job_atc_code_match'}

        args.update(kwargs)
        result = exec_before(**args)

        args.update(result)
        result = execute(**args)

        args.update(result)
        result = exec_after(outputs=[],**args)

        return result
    except Exception as e:
        logger = phs3logger(kwargs["job_id"])
        logger.error(traceback.format_exc())
        raise e


if __name__ == '__main__':
    debug_execute()


