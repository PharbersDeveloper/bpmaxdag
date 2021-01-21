import os
import uuid
import string
import subprocess
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.models import DAG, Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule


default_args = {
    "owner": "ywyuan",
    "start_date": days_ago(1),
    "email": ['airflow@example.com'],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="Auto_raw_data",
    tags=["Max"],
    default_args=default_args,
    schedule_interval=None,
    description="A Max Auto Job Example",
    dagrun_timeout=timedelta(minutes=2160.0)
)

var_key_lst = Variable.get("%s__SPARK_CONF" % (dag.dag_id), deserialize_json=True, default_var={})

# subprocess Ponen CMD
def process_cmd(cmd):
    print("process: " + cmd)

    p = subprocess.Popen(cmd, shell=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    last_line = ''
    while p.poll() is None:
        line = p.stdout.read().strip("\n")
        if line:
            last_line = line
            print(last_line)
    if p.returncode == 0:
        print('Subprogram success')
    else:
        raise Exception(last_line)

############## == Rawdata_pretreat == ###################
def Rawdata_pretreat_cmd(**context):
    ti = context['task_instance']
    owner = default_args['owner']
    run_id = context["dag_run"].run_id
    job_id = ti.hostname
    args = context["dag_run"].conf

    params = var_key_lst.get("common", {})
    params.update(var_key_lst.get("Rawdata_pretreat", {}))

    install_phcli = 'pip3 install phcli==2.0.12'
    process_cmd(install_phcli)

    exec_phcli_submit = 'phcli maxauto online_run --group Auto_raw_data --name Rawdata_pretreat ' \
                        '--owner "{}" --run_id "{}" --job_id "{}" --context "{}" "{}"'.format(str(owner), str(run_id), str(job_id), str(params), str(args))
    process_cmd(exec_phcli_submit)

    # key = ti.xcom_pull(task_ids='test', key='key').decode("UTF-8")
    # ti.xcom_push(key="key", value=key)

Rawdata_pretreat = PythonOperator(
    task_id='Rawdata_pretreat',
    provide_context=True,
    python_callable=Rawdata_pretreat_cmd,
    dag=dag
)
############## == Rawdata_pretreat == ###################


############## == Rawdata_needclean == ###################
def Rawdata_needclean_cmd(**context):
    ti = context['task_instance']
    owner = default_args['owner']
    run_id = context["dag_run"].run_id
    job_id = ti.hostname
    args = context["dag_run"].conf

    params = var_key_lst.get("common", {})
    params.update(var_key_lst.get("Rawdata_needclean", {}))

    install_phcli = 'pip3 install phcli==2.0.12'
    process_cmd(install_phcli)

    exec_phcli_submit = 'phcli maxauto online_run --group Auto_raw_data --name Rawdata_needclean ' \
                        '--owner "{}" --run_id "{}" --job_id "{}" --context "{}" "{}"'.format(str(owner), str(run_id), str(job_id), str(params), str(args))
    process_cmd(exec_phcli_submit)

    # key = ti.xcom_pull(task_ids='test', key='key').decode("UTF-8")
    # ti.xcom_push(key="key", value=key)

Rawdata_needclean = PythonOperator(
    task_id='Rawdata_needclean',
    provide_context=True,
    python_callable=Rawdata_needclean_cmd,
    dag=dag
)
############## == Rawdata_needclean == ###################


############## == Rawdata_check == ###################
def Rawdata_check_cmd(**context):
    ti = context['task_instance']
    owner = default_args['owner']
    run_id = context["dag_run"].run_id
    job_id = ti.hostname
    args = context["dag_run"].conf

    params = var_key_lst.get("common", {})
    params.update(var_key_lst.get("Rawdata_check", {}))

    install_phcli = 'pip3 install phcli==2.0.12'
    process_cmd(install_phcli)

    exec_phcli_submit = 'phcli maxauto online_run --group Auto_raw_data --name Rawdata_check ' \
                        '--owner "{}" --run_id "{}" --job_id "{}" --context "{}" "{}"'.format(str(owner), str(run_id), str(job_id), str(params), str(args))
    process_cmd(exec_phcli_submit)

    # key = ti.xcom_pull(task_ids='test', key='key').decode("UTF-8")
    # ti.xcom_push(key="key", value=key)

Rawdata_check = PythonOperator(
    task_id='Rawdata_check',
    provide_context=True,
    python_callable=Rawdata_check_cmd,
    dag=dag
)
############## == Rawdata_check == ###################


Rawdata_pretreat >> Rawdata_needclean >> Rawdata_check
