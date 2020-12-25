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
    "owner": "zm",
    "start_date": days_ago(1),
    "email": ['airflow@example.com'],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="job_atc_code_match",
    default_args=default_args,
    schedule_interval=None,
    description="A Max Auto Job Example",
    dagrun_timeout=timedelta(minutes=60)
)

var_key_lst = Variable.get("%s__SPARK_CONF" % (dag.dag_id), deserialize_json=True, default_var={})


############## == job_atc_code_match == ###################
def job_atc_code_match_cmd(**context):
    ti = context['task_instance']
    owner = default_args['owner']
    run_id = context["dag_run"].run_id
    job_id = ti.hostname.split("-")[-1]
    conf = context["dag_run"].conf

    params = var_key_lst.get("common", {})
    params.update(var_key_lst.get("job_atc_code_match", {}))

    write_hosts = 'echo "192.168.1.28    spark.master" >> /etc/hosts'
    print(write_hosts)
    print(subprocess.check_output(write_hosts, shell=True, stderr=subprocess.STDOUT))

    install_phcli = 'pip3 install phcli==1.2.3'
    print(install_phcli)
    print(subprocess.check_output(install_phcli, shell=True, stderr=subprocess.STDOUT))

    exec_phcli_submit = 'LANG=C.UTF-8 phcli maxauto --runtime python3 --group job_atc_code_match --path job_atc_code_match --cmd submit ' \
                        '--owner "{}" --run_id "{}" --job_id "{}" --context "{}" "{}"'.format(str(owner), str(run_id), str(job_id), str(params), str(conf))
    print(exec_phcli_submit)
    print(subprocess.check_output(exec_phcli_submit, shell=True, stderr=subprocess.STDOUT))

    # key = ti.xcom_pull(task_ids='test', key='key').decode("UTF-8")
    # ti.xcom_push(key="key", value=key)

job_atc_code_match = PythonOperator(
    task_id='job_atc_code_match',
    provide_context=True,
    python_callable=job_atc_code_match_cmd,
    dag=dag
)
############## == job_atc_code_match == ###################


job_atc_code_match
