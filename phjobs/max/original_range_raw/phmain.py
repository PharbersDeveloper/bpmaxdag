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
@click.option('--model_month_right')
@click.option('--max_month')
@click.option('--year_missing')
@click.option('--current_year')
@click.option('--first_month')
@click.option('--current_month')
@click.option('--if_others')
@click.option('--monthly_update')
@click.option('--if_add_data')
@click.option('--out_path')
@click.option('--run_id')
@click.option('--owner')
@click.option('--g_database_temp')
@click.option('--g_database_input')
@click.option('--g_out_adding_data')
@click.option('--g_out_new_hospital')
@click.option('--g_out_raw_data_adding_final')
def debug_execute(**kwargs):
    try:
        args = {"name": "job3_2_original_range_raw"}
        outputs = ["g_out_adding_data", "g_out_new_hospital", "g_out_raw_data_adding_final"]

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
        args = {"name": "job3_2_original_range_raw"}
        outputs = ["g_out_adding_data", "g_out_new_hospital", "g_out_raw_data_adding_final"]
        
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


