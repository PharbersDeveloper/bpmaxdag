from phjob import execute
import click


@click.command()
@click.option('--max_path')
@click.option('--project_name')
@click.option('--model_month_right')
@click.option('--max_month')
@click.option('--year_missing')
@click.option('--out_path')
@click.option('--need_test')

def debug_execute(max_path, project_name, model_month_right, max_month, year_missing, out_path, need_test):
    execute(max_path, project_name, model_month_right, max_month, year_missing, out_path, need_test)


if __name__ == '__main__':
    debug_execute()
