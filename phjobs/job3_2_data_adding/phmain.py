from phjob import execute
import click


@click.command()
@click.option('--max_path')
@click.option('--project_name')
@click.option('--model_month_right')
@click.option('--max_month')
@click.option('--year_missing')
@click.option('--current_year')
@click.option('--first_month')
@click.option('--current_month')
@click.option('--if_others')
@click.option('--monthly_update')
@click.option('--not_arrived_path')
@click.option('--published_path')
@click.option('--out_path')
@click.option('--out_dir')
@click.option('--need_test')

def debug_execute(max_path, project_name, model_month_right, max_month, year_missing, current_year, first_month, current_month, if_others, 
monthly_update, not_arrived_path, published_path, out_path, out_dir, need_test):
    execute(max_path, project_name, model_month_right, max_month, year_missing, current_year, first_month, current_month, if_others, 
    monthly_update, not_arrived_path, published_path, out_path, out_dir, need_test)


if __name__ == '__main__':
    debug_execute()
