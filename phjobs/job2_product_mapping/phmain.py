from phjob import execute
import click


@click.command()
@click.option('--max_path')
@click.option('--project_name')
@click.option('--minimum_product_columns')
@click.option('--minimum_product_sep')
@click.option('--minimum_product_newname')
@click.option('--need_cleaning_cols')
@click.option('--out_path')
@click.option('--need_test')
def debug_execute(max_path, project_name, minimum_product_columns, minimum_product_sep, minimum_product_newname, need_cleaning_cols, out_path, need_test):
    execute(max_path, project_name, minimum_product_columns, minimum_product_sep, minimum_product_newname, need_cleaning_cols, out_path, need_test)


if __name__ == '__main__':
    debug_execute()
