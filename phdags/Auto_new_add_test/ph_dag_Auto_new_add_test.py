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
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="Auto_new_add_test",
    tags=["Max"],
    default_args=default_args,
    schedule_interval=None,
    description="A Max Auto Job Example",
    dagrun_timeout=timedelta(minutes=2160.0)
)

var_key_lst = Variable.get("%s__SPARK_CONF" % (dag.dag_id), deserialize_json=True, default_var={})

# subprocess Ponen CMD
def process_cmd(cmd):
    print("process:" + cmd)

    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    last_line = ''
    while p.poll() is None:
        line = p.stdout.readline().decode('utf-8').strip("\n")
        if line:
            last_line = line
            print(last_line)
    if p.returncode == 0:
        print('Subprogram success')
    else:
        raise Exception(last_line)

############## == job3_new1_template == ###################
def job3_new1_template_cmd(**context):
    ti = context['task_instance']
    owner = default_args['owner']
    run_id = context["dag_run"].run_id
    job_id = ti.hostname
    args = context["dag_run"].conf

    params = var_key_lst.get("common", {})
    params.update(var_key_lst.get("job3_new1_template", {}))

    install_phcli = 'pip3 install phcli==2.0.6'
    process_cmd(install_phcli)

    exec_phcli_submit = 'phcli maxauto online_run --group Auto_new_add_test --name job3_new1_template ' \
                        '--owner "{}" --run_id "{}" --job_id "{}" --context "{}" "{}"'.format(str(owner), str(run_id), str(job_id), str(params), str(args))
    process_cmd(exec_phcli_submit)

    # key = ti.xcom_pull(task_ids='test', key='key').decode("UTF-8")
    # ti.xcom_push(key="key", value=key)

job3_new1_template = PythonOperator(
    task_id='job3_new1_template',
    provide_context=True,
    python_callable=job3_new1_template_cmd,
    dag=dag
)
############## == job3_new1_template == ###################


############## == job3_new2_dict == ###################
def job3_new2_dict_cmd(**context):
    ti = context['task_instance']
    owner = default_args['owner']
    run_id = context["dag_run"].run_id
    job_id = ti.hostname
    args = context["dag_run"].conf

    params = var_key_lst.get("common", {})
    params.update(var_key_lst.get("job3_new2_dict", {}))

    install_phcli = 'pip3 install phcli==2.0.6'
    process_cmd(install_phcli)

    exec_phcli_submit = 'phcli maxauto online_run --group Auto_new_add_test --name job3_new2_dict ' \
                        '--owner "{}" --run_id "{}" --job_id "{}" --context "{}" "{}"'.format(str(owner), str(run_id), str(job_id), str(params), str(args))
    process_cmd(exec_phcli_submit)

    # key = ti.xcom_pull(task_ids='test', key='key').decode("UTF-8")
    # ti.xcom_push(key="key", value=key)

job3_new2_dict = PythonOperator(
    task_id='job3_new2_dict',
    provide_context=True,
    python_callable=job3_new2_dict_cmd,
    dag=dag
)
############## == job3_new2_dict == ###################


############## == job3_new3_add == ###################
def job3_new3_add_cmd(**context):
    ti = context['task_instance']
    owner = default_args['owner']
    run_id = context["dag_run"].run_id
    job_id = ti.hostname
    args = context["dag_run"].conf

    params = var_key_lst.get("common", {})
    params.update(var_key_lst.get("job3_new3_add", {}))

    install_phcli = 'pip3 install phcli==2.0.6'
    process_cmd(install_phcli)

    exec_phcli_submit = 'phcli maxauto online_run --group Auto_new_add_test --name job3_new3_add ' \
                        '--owner "{}" --run_id "{}" --job_id "{}" --context "{}" "{}"'.format(str(owner), str(run_id), str(job_id), str(params), str(args))
    process_cmd(exec_phcli_submit)

    # key = ti.xcom_pull(task_ids='test', key='key').decode("UTF-8")
    # ti.xcom_push(key="key", value=key)

job3_new3_add = PythonOperator(
    task_id='job3_new3_add',
    provide_context=True,
    python_callable=job3_new3_add_cmd,
    dag=dag
)
############## == job3_new3_add == ###################


job3_new1_template >> job3_new2_dict >> job3_new3_add
