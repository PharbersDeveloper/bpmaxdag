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
@click.option('--final_prefix')
@click.option('--g_is_labeled')
@click.option('--depend_job_names_keys')
@click.option('--prediction_result')
@click.option('--positive_result')
@click.option('--negative_result')
@click.option('--g_sharehold_mole_name')
@click.option('--g_sharehold_product_name')
@click.option('--g_sharehold_dosage')
@click.option('--g_sharehold_spec')
@click.option('--g_sharehold_manufacturer_name')
@click.option('--g_sharehold_pack_qty')
@click.option('--final_predictions')
@click.option('--final_positive')
@click.option('--final_negative')
@click.option('--final_lost')
@click.option('--final_report')
def debug_execute(**kwargs):
    try:
        args = {'name': 'model_preditions'}

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


