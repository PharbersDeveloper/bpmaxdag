from phjob import execute
import click


@click.command()
@click.option('--max_path')
@click.option('--project_name')
@click.option('--cpa_gyc')
@click.option('--if_others')
@click.option('--out_path')
@click.option('--out_dir')
@click.option('--need_test')
def debug_execute(max_path, project_name, cpa_gyc, if_others, out_path, out_dir, need_test):
    execute(max_path, project_name, cpa_gyc, if_others, out_path, out_dir, need_test)


if __name__ == '__main__':
    debug_execute()
