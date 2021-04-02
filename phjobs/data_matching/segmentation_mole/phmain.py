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
@click.option('--job_name')
@click.option('--job_id')
@click.option('--run_id')
@click.option('--path_prefix')
@click.option('--mole_stopwords_path')
@click.option('--depend_job_names_keys')
@click.option('--g_repartition_shared')
@click.option('--segmentation_mole_result')
def debug_execute(**kwargs):
    try:
        args = {"name": "segmentation_mole"}
        outputs = ["segmentation_mole_result"]

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


