from phjob import execute
import click


@click.command()
@click.option('--max_path')
@click.option('--project_name')
@click.option('--cpa_gyc')
@click.option('--test_out_path')
@click.option('--need_test')
def debug_execute(max_path, project_name, cpa_gyc, test_out_path, need_test):
    execute(max_path, project_name, cpa_gyc, test_out_path, need_test)


if __name__ == '__main__':
    debug_execute()
