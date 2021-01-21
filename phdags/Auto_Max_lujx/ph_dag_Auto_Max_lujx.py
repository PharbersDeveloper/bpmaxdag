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
    "owner": "lujingxian",
    "start_date": days_ago(1),
    "email": ['airflow@example.com'],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="Auto_Max_lujx",
    tags=["Max"],
    default_args=default_args,
    schedule_interval=None,
    description="A Max Auto Job Example",
    dagrun_timeout=timedelta(minutes=7200.0)
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

############## == job1_hospital_mapping == ###################
def job1_hospital_mapping_cmd(**context):
    ti = context['task_instance']
    owner = default_args['owner']
    run_id = context["dag_run"].run_id.replace(':', '_')
    job_id = ti.hostname
    args = context["dag_run"].conf

    params = var_key_lst.get("common", {})
    params.update(var_key_lst.get("job1_hospital_mapping", {}))

    install_phcli = 'pip3 install phcli==2.0.12'
    process_cmd(install_phcli)

    exec_phcli_submit = 'phcli maxauto online_run --group Auto_Max_lujx --name job1_hospital_mapping ' \
                        '--owner "{}" --run_id "{}" --job_id "{}" --context "{}" "{}"'.format(str(owner), str(run_id), str(job_id), str(params), str(args))
    process_cmd(exec_phcli_submit)

    # key = ti.xcom_pull(task_ids='test', key='key').decode("UTF-8")
    # ti.xcom_push(key="key", value=key)

job1_hospital_mapping = PythonOperator(
    task_id='job1_hospital_mapping',
    provide_context=True,
    python_callable=job1_hospital_mapping_cmd,
    dag=dag
)
############## == job1_hospital_mapping == ###################


############## == job2_product_mapping == ###################
def job2_product_mapping_cmd(**context):
    ti = context['task_instance']
    owner = default_args['owner']
    run_id = context["dag_run"].run_id.replace(':', '_')
    job_id = ti.hostname
    args = context["dag_run"].conf

    params = var_key_lst.get("common", {})
    params.update(var_key_lst.get("job2_product_mapping", {}))

    install_phcli = 'pip3 install phcli==2.0.12'
    process_cmd(install_phcli)

    exec_phcli_submit = 'phcli maxauto online_run --group Auto_Max_lujx --name job2_product_mapping ' \
                        '--owner "{}" --run_id "{}" --job_id "{}" --context "{}" "{}"'.format(str(owner), str(run_id), str(job_id), str(params), str(args))
    process_cmd(exec_phcli_submit)

    # key = ti.xcom_pull(task_ids='test', key='key').decode("UTF-8")
    # ti.xcom_push(key="key", value=key)

job2_product_mapping = PythonOperator(
    task_id='job2_product_mapping',
    provide_context=True,
    python_callable=job2_product_mapping_cmd,
    dag=dag
)
############## == job2_product_mapping == ###################


############## == job3_1_data_adding == ###################
def job3_1_data_adding_cmd(**context):
    ti = context['task_instance']
    owner = default_args['owner']
    run_id = context["dag_run"].run_id.replace(':', '_')
    job_id = ti.hostname
    args = context["dag_run"].conf

    params = var_key_lst.get("common", {})
    params.update(var_key_lst.get("job3_1_data_adding", {}))

    install_phcli = 'pip3 install phcli==2.0.12'
    process_cmd(install_phcli)

    exec_phcli_submit = 'phcli maxauto online_run --group Auto_Max_lujx --name job3_1_data_adding ' \
                        '--owner "{}" --run_id "{}" --job_id "{}" --context "{}" "{}"'.format(str(owner), str(run_id), str(job_id), str(params), str(args))
    process_cmd(exec_phcli_submit)

    # key = ti.xcom_pull(task_ids='test', key='key').decode("UTF-8")
    # ti.xcom_push(key="key", value=key)

job3_1_data_adding = PythonOperator(
    task_id='job3_1_data_adding',
    provide_context=True,
    python_callable=job3_1_data_adding_cmd,
    dag=dag
)
############## == job3_1_data_adding == ###################


############## == job3_2_data_adding == ###################
def job3_2_data_adding_cmd(**context):
    ti = context['task_instance']
    owner = default_args['owner']
    run_id = context["dag_run"].run_id.replace(':', '_')
    job_id = ti.hostname
    args = context["dag_run"].conf

    params = var_key_lst.get("common", {})
    params.update(var_key_lst.get("job3_2_data_adding", {}))

    install_phcli = 'pip3 install phcli==2.0.12'
    process_cmd(install_phcli)

    exec_phcli_submit = 'phcli maxauto online_run --group Auto_Max_lujx --name job3_2_data_adding ' \
                        '--owner "{}" --run_id "{}" --job_id "{}" --context "{}" "{}"'.format(str(owner), str(run_id), str(job_id), str(params), str(args))
    process_cmd(exec_phcli_submit)

    # key = ti.xcom_pull(task_ids='test', key='key').decode("UTF-8")
    # ti.xcom_push(key="key", value=key)

job3_2_data_adding = PythonOperator(
    task_id='job3_2_data_adding',
    provide_context=True,
    python_callable=job3_2_data_adding_cmd,
    dag=dag
)
############## == job3_2_data_adding == ###################


############## == job4_panel == ###################
def job4_panel_cmd(**context):
    ti = context['task_instance']
    owner = default_args['owner']
    run_id = context["dag_run"].run_id.replace(':', '_')
    job_id = ti.hostname
    args = context["dag_run"].conf

    params = var_key_lst.get("common", {})
    params.update(var_key_lst.get("job4_panel", {}))

    install_phcli = 'pip3 install phcli==2.0.12'
    process_cmd(install_phcli)

    exec_phcli_submit = 'phcli maxauto online_run --group Auto_Max_lujx --name job4_panel ' \
                        '--owner "{}" --run_id "{}" --job_id "{}" --context "{}" "{}"'.format(str(owner), str(run_id), str(job_id), str(params), str(args))
    process_cmd(exec_phcli_submit)

    # key = ti.xcom_pull(task_ids='test', key='key').decode("UTF-8")
    # ti.xcom_push(key="key", value=key)

job4_panel = PythonOperator(
    task_id='job4_panel',
    provide_context=True,
    python_callable=job4_panel_cmd,
    dag=dag
)
############## == job4_panel == ###################


############## == job5_max_weight == ###################
def job5_max_weight_cmd(**context):
    ti = context['task_instance']
    owner = default_args['owner']
    run_id = context["dag_run"].run_id.replace(':', '_')
    job_id = ti.hostname
    args = context["dag_run"].conf

    params = var_key_lst.get("common", {})
    params.update(var_key_lst.get("job5_max_weight", {}))

    install_phcli = 'pip3 install phcli==2.0.12'
    process_cmd(install_phcli)

    exec_phcli_submit = 'phcli maxauto online_run --group Auto_Max_lujx --name job5_max_weight ' \
                        '--owner "{}" --run_id "{}" --job_id "{}" --context "{}" "{}"'.format(str(owner), str(run_id), str(job_id), str(params), str(args))
    process_cmd(exec_phcli_submit)

    # key = ti.xcom_pull(task_ids='test', key='key').decode("UTF-8")
    # ti.xcom_push(key="key", value=key)

job5_max_weight = PythonOperator(
    task_id='job5_max_weight',
    provide_context=True,
    python_callable=job5_max_weight_cmd,
    dag=dag
)
############## == job5_max_weight == ###################


############## == job6_max_city == ###################
def job6_max_city_cmd(**context):
    ti = context['task_instance']
    owner = default_args['owner']
    run_id = context["dag_run"].run_id.replace(':', '_')
    job_id = ti.hostname
    args = context["dag_run"].conf

    params = var_key_lst.get("common", {})
    params.update(var_key_lst.get("job6_max_city", {}))

    install_phcli = 'pip3 install phcli==2.0.12'
    process_cmd(install_phcli)

    exec_phcli_submit = 'phcli maxauto online_run --group Auto_Max_lujx --name job6_max_city ' \
                        '--owner "{}" --run_id "{}" --job_id "{}" --context "{}" "{}"'.format(str(owner), str(run_id), str(job_id), str(params), str(args))
    process_cmd(exec_phcli_submit)

    # key = ti.xcom_pull(task_ids='test', key='key').decode("UTF-8")
    # ti.xcom_push(key="key", value=key)

job6_max_city = PythonOperator(
    task_id='job6_max_city',
    provide_context=True,
    python_callable=job6_max_city_cmd,
    dag=dag
)
############## == job6_max_city == ###################


############## == job7_max_standard == ###################
def job7_max_standard_cmd(**context):
    ti = context['task_instance']
    owner = default_args['owner']
    run_id = context["dag_run"].run_id.replace(':', '_')
    job_id = ti.hostname
    args = context["dag_run"].conf

    params = var_key_lst.get("common", {})
    params.update(var_key_lst.get("job7_max_standard", {}))

    install_phcli = 'pip3 install phcli==2.0.12'
    process_cmd(install_phcli)

    exec_phcli_submit = 'phcli maxauto online_run --group Auto_Max_lujx --name job7_max_standard ' \
                        '--owner "{}" --run_id "{}" --job_id "{}" --context "{}" "{}"'.format(str(owner), str(run_id), str(job_id), str(params), str(args))
    process_cmd(exec_phcli_submit)

    # key = ti.xcom_pull(task_ids='test', key='key').decode("UTF-8")
    # ti.xcom_push(key="key", value=key)

job7_max_standard = PythonOperator(
    task_id='job7_max_standard',
    provide_context=True,
    python_callable=job7_max_standard_cmd,
    dag=dag
)
############## == job7_max_standard == ###################


############## == job7_raw_standard == ###################
def job7_raw_standard_cmd(**context):
    ti = context['task_instance']
    owner = default_args['owner']
    run_id = context["dag_run"].run_id.replace(':', '_')
    job_id = ti.hostname
    args = context["dag_run"].conf

    params = var_key_lst.get("common", {})
    params.update(var_key_lst.get("job7_raw_standard", {}))

    install_phcli = 'pip3 install phcli==2.0.12'
    process_cmd(install_phcli)

    exec_phcli_submit = 'phcli maxauto online_run --group Auto_Max_lujx --name job7_raw_standard ' \
                        '--owner "{}" --run_id "{}" --job_id "{}" --context "{}" "{}"'.format(str(owner), str(run_id), str(job_id), str(params), str(args))
    process_cmd(exec_phcli_submit)

    # key = ti.xcom_pull(task_ids='test', key='key').decode("UTF-8")
    # ti.xcom_push(key="key", value=key)

job7_raw_standard = PythonOperator(
    task_id='job7_raw_standard',
    provide_context=True,
    python_callable=job7_raw_standard_cmd,
    dag=dag
)
############## == job7_raw_standard == ###################


############## == Max_check == ###################
def Max_check_cmd(**context):
    ti = context['task_instance']
    owner = default_args['owner']
    run_id = context["dag_run"].run_id.replace(':', '_')
    job_id = ti.hostname
    args = context["dag_run"].conf

    params = var_key_lst.get("common", {})
    params.update(var_key_lst.get("Max_check", {}))

    install_phcli = 'pip3 install phcli==2.0.12'
    process_cmd(install_phcli)

    exec_phcli_submit = 'phcli maxauto online_run --group Auto_Max_lujx --name Max_check ' \
                        '--owner "{}" --run_id "{}" --job_id "{}" --context "{}" "{}"'.format(str(owner), str(run_id), str(job_id), str(params), str(args))
    process_cmd(exec_phcli_submit)

    # key = ti.xcom_pull(task_ids='test', key='key').decode("UTF-8")
    # ti.xcom_push(key="key", value=key)

Max_check = PythonOperator(
    task_id='Max_check',
    provide_context=True,
    python_callable=Max_check_cmd,
    dag=dag
)
############## == Max_check == ###################


job1_hospital_mapping >> job2_product_mapping >> job3_1_data_adding >> job3_2_data_adding >> job4_panel >> job5_max_weight >> job6_max_city >> [Max_check, job7_max_standard, job7_raw_standard]
