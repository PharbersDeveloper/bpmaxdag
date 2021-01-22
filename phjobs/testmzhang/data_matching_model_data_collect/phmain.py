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
@click.option('--job_name')
@click.option('--path_prefix')
@click.option('--depend_job_names_keys')
@click.option('--cleaning_origin')
@click.option('--prediction_result')
@click.option('--prediction_origin_result')
@click.option('--lost_data_result')
def debug_execute(**kwargs):
    try:
        args = {"name": "data_matching_model_result"}
        outputs = ["prediction_origin_result","lost_data_result"]

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


