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
@click.option('--dag_name')
@click.option('--run_id')
@click.option('--job_full_name')
@click.option('--job_id')
@click.option('--max_path')
@click.option('--project_name')
@click.option('--market_city_brand')
@click.option('--lmda')
@click.option('--learning_rate')
@click.option('--max_iteration')
@click.option('--gradient_type')
@click.option('--test')
@click.option('--year_list')
@click.option('--c')
@click.option('--d')
def debug_execute(**kwargs):
    try:
        args = {"name": "gradient_descent"}
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
        print(traceback.format_exc())
        raise e


if __name__ == '__main__':
    debug_execute()


