from phjob import execute
import click


@click.command()
@click.option('--job_id')
@click.option('--max_path')
@click.option('--project_name')
@click.option('--minimum_product_columns')
@click.option('--minimum_product_sep')
@click.option('--minimum_product_newname')
@click.option('--need_cleaning_cols')
@click.option('--if_others')
@click.option('--out_path')
@click.option('--out_dir')
@click.option('--need_test')
@click.option('--a')
@click.option('--b')
def debug_execute(job_id, a, b, max_path, project_name, minimum_product_columns, minimum_product_sep, minimum_product_newname, need_cleaning_cols, if_others, out_path, out_dir, need_test):
    execute(max_path, project_name, minimum_product_columns, minimum_product_sep, minimum_product_newname, need_cleaning_cols, if_others, out_path, out_dir, need_test)


if __name__ == '__main__':
    debug_execute()
