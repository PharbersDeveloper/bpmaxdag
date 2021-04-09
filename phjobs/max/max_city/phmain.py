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
@click.option('--g_project_name')
@click.option('--g_minimum_product_columns')
@click.option('--g_minimum_product_sep')
@click.option('--g_minimum_product_newname')
@click.option('--depend_job_names_keys')
@click.option('--max_path')
@click.option('--project_name')
@click.option('--time_left')
@click.option('--time_right')
@click.option('--left_models')
@click.option('--left_models_time_left')
@click.option('--right_models')
@click.option('--right_models_time_right')
@click.option('--all_models')
@click.option('--if_others')
@click.option('--out_path')
@click.option('--out_dir')
@click.option('--if_two_source')
@click.option('--hospital_level')
@click.option('--bedsize')
@click.option('--id_bedsize_path')
@click.option('--dag_name')
@click.option('--run_id')
@click.option('--g_out_dir')
@click.option('--c')
@click.option('--d')
def debug_execute(**kwargs):
    try:
        args = {"name": "max_city"}
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


