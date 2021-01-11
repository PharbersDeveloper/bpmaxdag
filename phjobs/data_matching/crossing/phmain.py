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
@click.option('--job_id')
@click.option('--run_id')
@click.option('--raw_data_path')
@click.option('--standard_prod_path')
@click.option('--human_replace_packid_path')
@click.option('--cpa_dosage_lst_path')
@click.option('--word_dict_encode_path')
@click.option('--split_data_path')
@click.option('--interim_result_path')
@click.option('--result_path')
def debug_execute(**kwargs):
    try:
        args = {'name': 'crossing'}

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
