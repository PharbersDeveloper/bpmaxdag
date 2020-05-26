from phjob import execute
import click


@click.command()
@click.option('--max_path')
@click.option('--max_path_local')
@click.option('--project_name')
@click.option('--minimum_product_columns')
@click.option('--minimum_product_sep')
@click.option('--minimum_product_newname')
@click.option('--need_cleaning_cols')
@click.option('--test_out_path')
def debug_execute(max_path, max_path_local, project_name, minimum_product_columns, minimum_product_sep, minimum_product_newname, need_cleaning_cols, test_out_path):
    execute(max_path, max_path_local, project_name, minimum_product_columns, minimum_product_sep, minimum_product_newname, need_cleaning_cols, test_out_path)


if __name__ == '__main__':
    debug_execute()
