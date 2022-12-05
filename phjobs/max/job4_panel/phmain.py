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
@click.option('--model_month_left')
@click.option('--model_month_right')
@click.option('--if_others')
@click.option('--current_year')
@click.option('--current_month')
@click.option('--paths_foradding')
@click.option('--not_arrived_path')
@click.option('--published_path')
@click.option('--panel_for_union')
@click.option('--monthly_update')
@click.option('--out_path')
@click.option('--out_dir')
@click.option('--need_test')
@click.option('--add_47')
@click.option('--a')
@click.option('--b')
@click.option('--time_left')
@click.option('--time_right')
def debug_execute(**kwargs):
    try:
        args = {"name": "job4_panel"}
        outputs = ["a", "b"]

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


@click.command()
@click.option('--owner')
@click.option('--dag_name')
@click.option('--run_id')
@click.option('--job_full_name')
@click.option('--job_id')
@click.option('--job_args_name')
def online_debug_execute(**kwargs):
    try:
        args = {"name": "job4_panel"}
        outputs = ["a", "b"]

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
    online_debug_execute()

