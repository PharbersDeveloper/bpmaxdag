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
@click.option('--project_name')
@click.option('--time_left')
@click.option('--time_right')
@click.option('--all_models')
@click.option('--if_others')
@click.option('--minimum_product_columns')
@click.option('--minimum_product_sep')
@click.option('--minimum_product_newname')
@click.option('--if_two_source')
@click.option('--hospital_level')
@click.option('--bedsize')
@click.option('--id_bedsize_path')
@click.option('--out_path')
@click.option('--run_id')
@click.option('--owner')
@click.option('--g_database_temp')
@click.option('--g_database_input')
@click.option('--g_out_max_backfill')
def debug_execute(**kwargs):
    try:
        args = {"name": "job6_max_city"}
        outputs = ["g_out_max_backfill"]

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
        args = {"name": "job6_max_city"}
        outputs = ["g_out_max_backfill"]
        
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


