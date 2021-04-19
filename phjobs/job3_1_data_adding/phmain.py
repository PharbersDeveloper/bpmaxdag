from phjob import execute
import click


@click.command()
@click.option('--owner')
@click.option('--dag_name')
@click.option('--job_full_name')
@click.option('--run_id')
@click.option('--job_id')
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
@click.option('--if_add_data')
@click.option('--a')
@click.option('--b')
def debug_execute(owner, dag_name, job_full_name, run_id, job_id, a, b, max_path, project_name, model_month_right, max_month, year_missing, current_year, first_month, current_month, if_others, 
monthly_update, not_arrived_path, published_path, out_path, out_dir, need_test, if_add_data):
    execute(max_path, project_name, model_month_right, max_month, year_missing, current_year, first_month, current_month, if_others, 
    monthly_update, not_arrived_path, published_path, out_path, out_dir, need_test, if_add_data)


if __name__ == '__main__':
    debug_execute()
