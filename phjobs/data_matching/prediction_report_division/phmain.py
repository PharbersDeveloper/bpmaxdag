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
@click.option('--true_positive_result_path')
@click.option('--true_negative_result_path')
@click.option('--false_positive_result_path')
@click.option('--false_negative_result_path')
@click.option('--true_positive_result_path_csv')
@click.option('--true_negative_result_path_csv')
@click.option('--false_positive_result_path_csv')
@click.option('--false_negative_result_path_csv')
@click.option('--training_data_path')
@click.option('--split_data_path')
@click.option('--positive_result_path')
@click.option('--negative_result_path')
@click.option('--lost_data_path')
@click.option('--final_report_path')
@click.option('--mnf_check_path')
@click.option('--spec_check_path')
@click.option('--dosage_check_path')

def debug_execute(**kwargs):
    try:
        args = {'name': 'prediction_report_division'}

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
