import os
import uuid
import string
import subprocess
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.models import DAG, Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule


default_args = {
    "owner": "jeorch",
    "start_date": days_ago(1),
    "email": ['czhang@pharbers-data.com'],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="Auto_data_matching",
    default_args=default_args,
    schedule_interval=None,
    description="A Max Auto Job Example",
    dagrun_timeout=timedelta(minutes=60)
)

var_key_lst = Variable.get("%s__SPARK_CONF" % (dag.dag_id), deserialize_json=True, default_var={})
project_path = 's3a://ph-max-auto/2020-08-11/data_matching'

############## == crossing == ###################
def crossing_cmd(**context):
    ti = context['task_instance']
    owner = default_args['owner']
    run_id = context["dag_run"].run_id
    run_id_path = getRunIdPath(run_id)
    job_id = ti.hostname.split("-")[-1]
    conf = context["dag_run"].conf

    # inputs

    # outputs
    split_data_path = ''
    interim_result_path = ''
    result_path = ''
    if 'split_data_path' not in conf:
        split_data_path = project_path + '/airflow_runs/' + run_id_path + '/crossing/split_data_path'
        conf['split_data_path'] = split_data_path
    if 'interim_result_path' not in conf:
        interim_result_path = project_path + '/airflow_runs/' + run_id_path + '/crossing/interim_result_path'
        conf['interim_result_path'] = interim_result_path
    if 'result_path' not in conf:
        result_path = project_path + '/airflow_runs/' + run_id_path + '/crossing/result_path'
        conf['result_path'] = result_path
    split_data_path = conf["split_data_path"] + "/" + str(job_id)
    interim_result_path = conf["interim_result_path"] + "/" + str(job_id)
    result_path = conf["result_path"] + "/" + str(job_id)
    ti.xcom_push(key="split_data_path", value=split_data_path)
    ti.xcom_push(key="interim_result_path", value=interim_result_path)
    ti.xcom_push(key="result_path", value=result_path)

    params = dict(var_key_lst.get("common", {}).items() + var_key_lst.get("crossing", {}).items())

    write_hosts = 'echo "192.168.1.28    spark.master" >> /etc/hosts'
    print(write_hosts)
    print(subprocess.check_output(write_hosts, shell=True, stderr=subprocess.STDOUT))

    install_phcli = 'apt-get update && apt install -f -y postgresql-server-dev-all && pip3 install phcli==1.1.0'
    print(install_phcli)
    print(subprocess.check_output(install_phcli, shell=True, stderr=subprocess.STDOUT))

    exec_phcli_submit = 'phcli maxauto --runtime python3 --group Auto_data_matching --path crossing --cmd submit ' \
                        '--owner "{}" --run_id "{}" --job_id "{}" --context "{}" "{}"'.format(str(owner), str(run_id), str(job_id), str(params), str(conf))
    print(exec_phcli_submit)
    subprocess.check_output(exec_phcli_submit, shell=True, stderr=subprocess.STDOUT)

    # key = ti.xcom_pull(task_ids='test', key='key').decode("UTF-8")
    # ti.xcom_push(key="key", value=key)

    # selector
    if 'model_path' not in conf:
        return 'training'
    else:
        print(conf['model_path'])
        return 'prediction'

crossing = BranchPythonOperator(
    task_id='crossing',
    provide_context=True,
    python_callable=crossing_cmd,
    dag=dag
)
############## == crossing == ###################


############## == training == ###################
def training_cmd(**context):
    ti = context['task_instance']
    owner = default_args['owner']
    run_id = context["dag_run"].run_id
    run_id_path = getRunIdPath(run_id)
    job_id = ti.hostname.split("-")[-1]
    conf = context["dag_run"].conf

    # inputs
    if 'training_data_path' not in conf:
        training_data_path = ti.xcom_pull(task_ids='crossing', key='result_path').decode("UTF-8")
        conf['training_data_path'] = training_data_path

    # outputs
    validate_path = ''
    model_path = ''
    if 'validate_path' not in conf:
        validate_path = project_path + '/airflow_runs/' + run_id_path + '/training/validate_path'
        conf['validate_path'] = validate_path
    if 'model_path' not in conf:
        model_path = project_path + '/airflow_runs/' + run_id_path + '/training/model_path'
        conf['model_path'] = model_path
    validate_path = conf["validate_path"] + "/" + str(job_id)
    model_path = conf["model_path"] + "/" + str(job_id)
    ti.xcom_push(key="validate_path", value=validate_path)
    ti.xcom_push(key="model_path", value=model_path)

    params = dict(var_key_lst.get("common", {}).items() + var_key_lst.get("training", {}).items())

    write_hosts = 'echo "192.168.1.28    spark.master" >> /etc/hosts'
    print(write_hosts)
    print(subprocess.check_output(write_hosts, shell=True, stderr=subprocess.STDOUT))

    install_phcli = 'apt-get update && apt install -f -y postgresql-server-dev-all && pip3 install phcli==1.1.0'
    print(install_phcli)
    print(subprocess.check_output(install_phcli, shell=True, stderr=subprocess.STDOUT))

    exec_phcli_submit = 'phcli maxauto --runtime python3 --group Auto_data_matching --path training --cmd submit ' \
                        '--owner "{}" --run_id "{}" --job_id "{}" --context "{}" "{}"'.format(str(owner), str(run_id), str(job_id), str(params), str(conf))
    print(exec_phcli_submit)
    subprocess.check_output(exec_phcli_submit, shell=True, stderr=subprocess.STDOUT)

    # key = ti.xcom_pull(task_ids='test', key='key').decode("UTF-8")
    # ti.xcom_push(key="key", value=key)

training = PythonOperator(
    task_id='training',
    provide_context=True,
    python_callable=training_cmd,
    dag=dag
)
############## == training == ###################


############## == training_validate == ###################
def training_validate_cmd(**context):
    ti = context['task_instance']
    owner = default_args['owner']
    run_id = context["dag_run"].run_id
    run_id_path = getRunIdPath(run_id)
    job_id = ti.hostname.split("-")[-1]
    conf = context["dag_run"].conf

    # inputs
    if "validate_path" not in conf:
        validate_path = ti.xcom_pull(task_ids='training', key='validate_path').decode("UTF-8")
        conf['validate_path'] = validate_path
    if "model_path" not in conf:
        model_path = ti.xcom_pull(task_ids='training', key='model_path').decode("UTF-8")
        conf['model_path'] = model_path

    # outputs
    validate_output_path = ''
    if 'validate_output_path' not in conf:
        validate_output_path = project_path + '/airflow_runs/' + run_id_path + '/training_validate/validate_output_path'
        conf['validate_output_path'] = validate_output_path
    validate_output_path = conf["validate_output_path"] + "/" + str(job_id)
    ti.xcom_push(key="validate_output_path", value=validate_output_path)

    params = dict(var_key_lst.get("common", {}).items() + var_key_lst.get("training_validate", {}).items())

    write_hosts = 'echo "192.168.1.28    spark.master" >> /etc/hosts'
    print(write_hosts)
    print(subprocess.check_output(write_hosts, shell=True, stderr=subprocess.STDOUT))

    install_phcli = 'apt-get update && apt install -f -y postgresql-server-dev-all && pip3 install phcli==1.1.0'
    print(install_phcli)
    print(subprocess.check_output(install_phcli, shell=True, stderr=subprocess.STDOUT))

    exec_phcli_submit = 'phcli maxauto --runtime python3 --group Auto_data_matching --path training_validate --cmd submit ' \
                        '--owner "{}" --run_id "{}" --job_id "{}" --context "{}" "{}"'.format(str(owner), str(run_id), str(job_id), str(params), str(conf))
    print(exec_phcli_submit)
    subprocess.check_output(exec_phcli_submit, shell=True, stderr=subprocess.STDOUT)

    # key = ti.xcom_pull(task_ids='test', key='key').decode("UTF-8")
    # ti.xcom_push(key="key", value=key)

training_validate = PythonOperator(
    task_id='training_validate',
    provide_context=True,
    python_callable=training_validate_cmd,
    dag=dag
)
############## == training_validate == ###################


############## == prediction == ###################
def prediction_cmd(**context):
    ti = context['task_instance']
    owner = default_args['owner']
    run_id = context["dag_run"].run_id
    run_id_path = getRunIdPath(run_id)
    job_id = ti.hostname.split("-")[-1]
    conf = context["dag_run"].conf

    # inputs
    if 'training_data_path' not in conf:
        training_data_path = ti.xcom_pull(task_ids='crossing', key='result_path').decode("UTF-8")
        conf['training_data_path'] = training_data_path

    # outputs
    result_path = ''
    if 'result_path' not in conf:
        result_path = project_path + '/airflow_runs/' + run_id_path + '/prediction/result_path'
        conf['result_path'] = result_path
    result_path = conf["result_path"] + "/" + str(job_id)
    ti.xcom_push(key="result_path", value=result_path)

    params = dict(var_key_lst.get("common", {}).items() + var_key_lst.get("prediction", {}).items())

    write_hosts = 'echo "192.168.1.28    spark.master" >> /etc/hosts'
    print(write_hosts)
    print(subprocess.check_output(write_hosts, shell=True, stderr=subprocess.STDOUT))

    install_phcli = 'apt-get update && apt install -f -y postgresql-server-dev-all && pip3 install phcli==1.1.0'
    print(install_phcli)
    print(subprocess.check_output(install_phcli, shell=True, stderr=subprocess.STDOUT))

    exec_phcli_submit = 'phcli maxauto --runtime python3 --group Auto_data_matching --path prediction --cmd submit ' \
                        '--owner "{}" --run_id "{}" --job_id "{}" --context "{}" "{}"'.format(str(owner), str(run_id), str(job_id), str(params), str(conf))
    print(exec_phcli_submit)
    subprocess.check_output(exec_phcli_submit, shell=True, stderr=subprocess.STDOUT)

    # key = ti.xcom_pull(task_ids='test', key='key').decode("UTF-8")
    # ti.xcom_push(key="key", value=key)

prediction = PythonOperator(
    task_id='prediction',
    provide_context=True,
    python_callable=prediction_cmd,
    dag=dag
)
############## == prediction == ###################


############## == prediction_report == ###################
def prediction_report_cmd(**context):
    ti = context['task_instance']
    owner = default_args['owner']
    run_id = context["dag_run"].run_id
    run_id_path = getRunIdPath(run_id)
    job_id = ti.hostname.split("-")[-1]
    conf = context["dag_run"].conf

    # inputs
    if 'training_data_path' not in conf:
        training_data_path = ti.xcom_pull(task_ids='crossing', key='result_path').decode("UTF-8")
        conf['training_data_path'] = training_data_path
    if 'predictions_path' not in conf:
        predictions_path = ti.xcom_pull(task_ids='prediction', key='result_path').decode("UTF-8")
        conf['predictions_path'] = predictions_path

    # outputs
    result_path = ''
    if 'result_path' not in conf:
        result_path = project_path + '/airflow_runs/' + run_id_path + '/prediction_report/result_path'
        conf['result_path'] = result_path
    result_path = conf["result_path"] + "/" + str(job_id)
    ti.xcom_push(key="result_path", value=result_path)

    params = dict(var_key_lst.get("common", {}).items() + var_key_lst.get("prediction_report", {}).items())

    write_hosts = 'echo "192.168.1.28    spark.master" >> /etc/hosts'
    print(write_hosts)
    print(subprocess.check_output(write_hosts, shell=True, stderr=subprocess.STDOUT))

    install_phcli = 'apt-get update && apt install -f -y postgresql-server-dev-all && pip3 install phcli==1.1.0'
    print(install_phcli)
    print(subprocess.check_output(install_phcli, shell=True, stderr=subprocess.STDOUT))

    exec_phcli_submit = 'phcli maxauto --runtime python3 --group Auto_data_matching --path prediction_report --cmd submit ' \
                        '--owner "{}" --run_id "{}" --job_id "{}" --context "{}" "{}"'.format(str(owner), str(run_id), str(job_id), str(params), str(conf))
    print(exec_phcli_submit)
    subprocess.check_output(exec_phcli_submit, shell=True, stderr=subprocess.STDOUT)

    # key = ti.xcom_pull(task_ids='test', key='key').decode("UTF-8")
    # ti.xcom_push(key="key", value=key)

prediction_report = PythonOperator(
    task_id='prediction_report',
    provide_context=True,
    python_callable=prediction_report_cmd,
    dag=dag
)
############## == prediction_report == ###################

def getRunIdPath(airflowRunID):
    run_id_dt_str = airflowRunID.split('__')[1].split('+')[0]
    dt = datetime.strptime(run_id_dt_str, '%Y-%m-%dT%H:%M:%S.%f')
    # Python2.7 no support for datetime.timestamp()
    ts = (dt - datetime(1970, 1, 1)).total_seconds()
    run_id_path = str(int(ts))
    return run_id_path

crossing >> training
crossing >> prediction
training >> training_validate
prediction >> prediction_report
