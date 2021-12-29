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
@click.option('--path_cleaning_data')
@click.option('--path_human_interfere')
@click.option('--path_second_human_interfere')
@click.option('--source_data_type')
@click.option('--cleaning_result')
@click.option('--cleaning_origin')
@click.option('--raw_table_of_no_exist_pack_check_id')
def debug_execute(**kwargs):
    try:
        args = {'name': 'cleaning_data_normalization'}

        args.update(kwargs)
        result = exec_before(**args)

        args.update(result)
        result = execute(**args)

        args.update(result)
        result = exec_after(outputs=[], **args)

        return result
    except Exception as e:
        logger = phs3logger(kwargs["job_id"])
        logger.error(traceback.format_exc())
        raise e


if __name__ == '__main__':
    debug_execute()


