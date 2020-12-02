from phjob import execute
import click


@click.command()
@click.option('--owner')
@click.option('--run_id')
@click.option('--job_id')
@click.option('--max_path')
@click.option('--project_name')
@click.option('--cpa_gyc')
@click.option('--if_others')
@click.option('--out_path')
@click.option('--out_dir')
@click.option('--auto_max')
@click.option('--need_test')
@click.option('--a')
@click.option('--b')
def debug_execute(owner, run_id, job_id, a, b, max_path, project_name, cpa_gyc, if_others, out_path, out_dir, auto_max, need_test):
    execute(max_path, project_name, cpa_gyc, if_others, out_path, out_dir, auto_max, need_test)


if __name__ == '__main__':
    debug_execute()
