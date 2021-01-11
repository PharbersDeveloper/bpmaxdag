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
    dag_id="extract_data_clean",
    tags=['Extract Data', 'Clean'],
    default_args=default_args,
    schedule_interval="@daily",
    description="每天凌晨清除提数的副本",
    dagrun_timeout=timedelta(minutes=60)
)

var_key_lst = Variable.get("%s__SPARK_CONF" % (
    dag.dag_id), deserialize_json=True, default_var={})


############## == extract_data_clean == ###################
def extract_data_clean_cmd(**context):
    ti = context['task_instance']
    owner = default_args['owner']
    run_id = context["dag_run"].run_id
    job_id = ti.hostname.split("-")[-1]
    conf = context["dag_run"].conf

    params = var_key_lst.get("common", {})
    params.update(var_key_lst.get("extract_data_clean", {}))

    write_hosts = 'echo "192.168.18.7    spark.master" >> /etc/hosts'
    print(write_hosts)
    print(subprocess.check_output(write_hosts, shell=True,
                                  stderr=subprocess.STDOUT).decode("utf-8"))

    install_phcli = 'pip3 install phcli==1.2.3'
    print(install_phcli)
    print(subprocess.check_output(install_phcli, shell=True,
                                  stderr=subprocess.STDOUT).decode("utf-8"))

    exec_phcli_submit = 'LANG=C.UTF-8 phcli maxauto --runtime python3 --group extract_data_clean --path extract_data_clean --cmd submit ' \
                        '--owner "{}" --run_id "{}" --job_id "{}" --context "{}" "{}"'.format(
                            str(owner), str(run_id), str(job_id), str(params), str(conf))
    print(exec_phcli_submit)
    print(subprocess.check_output(exec_phcli_submit,
                                  shell=True, stderr=subprocess.STDOUT).decode("utf-8"))

    # key = ti.xcom_pull(task_ids='test', key='key').decode("UTF-8")
    # ti.xcom_push(key="key", value=key)


extract_data_clean = PythonOperator(
    task_id='extract_data_clean',
    provide_context=True,
    python_callable=extract_data_clean_cmd,
    dag=dag
)
############## == extract_data_clean == ###################


extract_data_clean
