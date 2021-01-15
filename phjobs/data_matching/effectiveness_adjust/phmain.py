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
@click.option('--job_name')
@click.option('--path_prefix')
@click.option('--dosage_mapping_path')
@click.option('--word_dict_encode_path')
@click.option('--lexicon_path')
@click.option('--depend_job_names_keys')
@click.option('--effective_result')
def debug_execute(**kwargs):
    try:
        args = {'name': 'effectiveness_adjust'}

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


