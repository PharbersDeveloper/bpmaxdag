from phjob import execute
import click


@click.command()
@click.option('--max_path')
@click.option('--project_name')
@click.option('--cpa_gyc')
@click.option('--test_out_path')
def debug_execute(max_path, project_name, cpa_gyc, test_out_path):
    execute(max_path, project_name, cpa_gyc, test_out_path)


if __name__ == '__main__':
    debug_execute()
