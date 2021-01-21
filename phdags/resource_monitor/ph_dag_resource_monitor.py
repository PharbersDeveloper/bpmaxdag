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
    "owner": "pqian",
    "start_date": days_ago(1),
    "email": ['pqian@pharbers.com'],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="resource_monitor",
    tags=["监控", "MQTT"],
    default_args=default_args,
    schedule_interval=None,
    description="资源监控",
    dagrun_timeout=timedelta(minutes=60)
)

var_key_lst = Variable.get("%s__SPARK_CONF" % (dag.dag_id), deserialize_json=True, default_var={})

# trigger message
# {"email":"pqian@pharbers.com","subject":"监控","content_type":"text/plain","content":"测试","topic":"test/2","message":"eyJkYXRhIjp7ImlkIjoieWV2NG9oN254Q1p6VTVBOSIsInR5cGUiOiJtcXR0IiwiYXR0cmlidXRlcyI6eyJkYXRlIjoxNjExMjIyMTQzMTIwLCJ0eXBlIjoiZXh0cmFjdCBkYXRhIiwiZmxhZyI6ZmFsc2UsIm1lc3NhZ2UiOiLmj5DmlbDnu5PmnZ/vvIzor7fliLDmlbDmja7otYTkuqfkuK3mn6XnnIvlubbkuIvovb0ifX19"}

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

############## == resource_monitor_email == ###################
def resource_monitor_email_cmd(**context):
    ti = context['task_instance']
    owner = default_args['owner']
    run_id = context["dag_run"].run_id
    job_id = ti.hostname
    args = context["dag_run"].conf

    params = var_key_lst.get("common", {})
    params.update(var_key_lst.get("resource_monitor_email", {}))

    install_phcli = 'pip3 install phcli==2.0.12'
    process_cmd(install_phcli)

    exec_phcli_submit = 'phcli maxauto online_run --group resource_monitor --name resource_monitor_email ' \
                        '--owner "{}" --run_id "{}" --job_id "{}" --context "{}" "{}"'.format(str(owner), str(run_id), str(job_id), str(params), str(args))
    process_cmd(exec_phcli_submit)

    # key = ti.xcom_pull(task_ids='test', key='key').decode("UTF-8")
    # ti.xcom_push(key="key", value=key)

resource_monitor_email = PythonOperator(
    task_id='resource_monitor_email',
    provide_context=True,
    python_callable=resource_monitor_email_cmd,
    dag=dag
)
############## == resource_monitor_email == ###################


############## == resource_monitor_iot == ###################
def resource_monitor_iot_cmd(**context):
    ti = context['task_instance']
    owner = default_args['owner']
    run_id = context["dag_run"].run_id
    job_id = ti.hostname
    args = context["dag_run"].conf

    params = var_key_lst.get("common", {})
    params.update(var_key_lst.get("resource_monitor_iot", {}))

    install_phcli = 'pip3 install phcli==2.0.12'
    process_cmd(install_phcli)

    install_phcli = 'pip3 install awsiotsdk'
    process_cmd(install_phcli)


    exec_phcli_submit = 'phcli maxauto online_run --group resource_monitor --name resource_monitor_iot ' \
                        '--owner "{}" --run_id "{}" --job_id "{}" --context "{}" "{}"'.format(str(owner), str(run_id), str(job_id), str(params), str(args))
    process_cmd(exec_phcli_submit)

    # key = ti.xcom_pull(task_ids='test', key='key').decode("UTF-8")
    # ti.xcom_push(key="key", value=key)

resource_monitor_iot = PythonOperator(
    task_id='resource_monitor_iot',
    provide_context=True,
    python_callable=resource_monitor_iot_cmd,
    dag=dag
)
############## == resource_monitor_iot == ###################


resource_monitor_email >> resource_monitor_iot
