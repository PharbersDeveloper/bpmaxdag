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
    "owner": "zyyin",
    "start_date": days_ago(1),
    "email": ['airflow@example.com'],
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

############## == Auto_data_matching_crossing == ###################
def Auto_data_matching_crossing_cmd(**context):
    ti = context['task_instance']
    owner = default_args['owner']
    run_id = context["dag_run"].run_id
    run_id_path = getRunIdPath(run_id)
    job_id = ti.hostname.split("-")[-1]
    conf = context["dag_run"].conf

    # selector
    # if 'model_path' in conf:
    #     print(conf['model_path'])
    #     return 'Auto_data_matching_prediction'

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

    params = var_key_lst.get("common", {})
    params.update(var_key_lst.get("Auto_data_matching_crossing", {}))

    write_hosts = 'echo "192.168.1.28    spark.master" >> /etc/hosts'
    print(write_hosts)
    print(subprocess.check_output(write_hosts, shell=True, stderr=subprocess.STDOUT))

    install_phcli = 'pip3 install phcli==1.2.3'
    print(install_phcli)
    print(subprocess.check_output(install_phcli, shell=True, stderr=subprocess.STDOUT))

    exec_phcli_submit = 'phcli maxauto --runtime python3 --group Auto_data_matching --path Auto_data_matching_crossing --cmd submit ' \
                        '--owner "{}" --run_id "{}" --job_id "{}" --context "{}" "{}"'.format(str(owner), str(run_id), str(job_id), str(params), str(conf))
    print(exec_phcli_submit)
    print(subprocess.check_output(exec_phcli_submit, shell=True, stderr=subprocess.STDOUT))

    # key = ti.xcom_pull(task_ids='test', key='key').decode("UTF-8")
    # ti.xcom_push(key="key", value=key)

    # return 'Auto_data_matching_training'
    # selector
    if 'model_path' not in conf:
        return 'Auto_data_matching_training'
    else:
        print(conf['model_path'])
        # ti.xcom_push(key="result_path", value=result_path)
        return 'Auto_data_matching_prediction'

Auto_data_matching_crossing = BranchPythonOperator(
    task_id='Auto_data_matching_crossing',
    provide_context=True,
    python_callable=Auto_data_matching_crossing_cmd,
    dag=dag
)
############## == Auto_data_matching_crossing == ###################


############## == Auto_data_matching_prediction == ###################
def Auto_data_matching_prediction_cmd(**context):
    ti = context['task_instance']
    owner = default_args['owner']
    run_id = context["dag_run"].run_id
    run_id_path = getRunIdPath(run_id)
    job_id = ti.hostname.split("-")[-1]
    conf = context["dag_run"].conf

    # inputs
    if 'training_data_path' not in conf:
        training_data_path = ti.xcom_pull(task_ids='Auto_data_matching_crossing', key='result_path')
        conf['training_data_path'] = training_data_path

    # outputs
    result_path = ''
    if 'result_path' not in conf:
        result_path = project_path + '/airflow_runs/' + run_id_path + '/prediction/result_path'
        conf['result_path'] = result_path
    result_path = conf["result_path"] + "/" + str(job_id)
    ti.xcom_push(key="result_path", value=result_path)

    params = var_key_lst.get("common", {})
    params.update(var_key_lst.get("Auto_data_matching_prediction", {}))

    write_hosts = 'echo "192.168.1.28    spark.master" >> /etc/hosts'
    print(write_hosts)
    print(subprocess.check_output(write_hosts, shell=True, stderr=subprocess.STDOUT))

    install_phcli = 'pip3 install phcli==1.2.3'
    print(install_phcli)
    print(subprocess.check_output(install_phcli, shell=True, stderr=subprocess.STDOUT))

    exec_phcli_submit = 'phcli maxauto --runtime python3 --group Auto_data_matching --path Auto_data_matching_prediction --cmd submit ' \
                        '--owner "{}" --run_id "{}" --job_id "{}" --context "{}" "{}"'.format(str(owner), str(run_id), str(job_id), str(params), str(conf))
    print(exec_phcli_submit)
    print(subprocess.check_output(exec_phcli_submit, shell=True, stderr=subprocess.STDOUT))
    
    # key = ti.xcom_pull(task_ids='test', key='key').decode("UTF-8")
    # ti.xcom_push(key="key", value=key)

Auto_data_matching_prediction = PythonOperator(
    task_id='Auto_data_matching_prediction',
    provide_context=True,
    python_callable=Auto_data_matching_prediction_cmd,
    dag=dag
)
############## == Auto_data_matching_prediction == ###################


############## == Auto_data_matching_prediction_report == ###################
def Auto_data_matching_prediction_report_cmd(**context):
    ti = context['task_instance']
    owner = default_args['owner']
    run_id = context["dag_run"].run_id
    run_id_path = getRunIdPath(run_id)
    job_id = ti.hostname.split("-")[-1]
    conf = context["dag_run"].conf

    # inputs
    if 'training_data_path' not in conf:
        training_data_path = ti.xcom_pull(task_ids='Auto_data_matching_crossing', key='result_path')
        conf['training_data_path'] = training_data_path
    if 'predictions_path' not in conf:
        predictions_path = ti.xcom_pull(task_ids='Auto_data_matching_prediction', key='result_path')
        conf['predictions_path'] = predictions_path

    # outputs
    positive_result_path = ''
    if 'positive_result_path' not in conf:
        positive_result_path = project_path + '/airflow_runs/' + run_id_path + '/prediction_report/positive_result_path'
        conf['positive_result_path'] = positive_result_path
    positive_result_path = conf["positive_result_path"] + "/" + str(job_id)
    ti.xcom_push(key="positive_result_path", value=positive_result_path)
    
    negative_result_path = ''
    if 'negative_result_path' not in conf:
        negative_result_path = project_path + '/airflow_runs/' + run_id_path + '/prediction_report/negative_result_path'
        conf['negative_result_path'] = negative_result_path
    negative_result_path = conf["negative_result_path"] + "/" + str(job_id)
    ti.xcom_push(key="negative_result_path", value=negative_result_path)
    
    positive_result_path_csv = ''
    if 'positive_result_path_csv' not in conf:
        positive_result_path_csv = project_path + '/airflow_runs/' + run_id_path + '/prediction_report/positive_result_path_csv'
        conf['positive_result_path_csv'] = positive_result_path_csv
    positive_result_path_csv = conf["positive_result_path_csv"] + "/" + str(job_id)
    ti.xcom_push(key="positive_result_path_csv", value=positive_result_path_csv)
    
    negative_result_path_csv = ''
    if 'negative_result_path_csv' not in conf:
        negative_result_path_csv = project_path + '/airflow_runs/' + run_id_path + '/prediction_report/negative_result_path_csv'
        conf['negative_result_path_csv'] = negative_result_path_csv
    negative_result_path_csv = conf["negative_result_path_csv"] + "/" + str(job_id)
    ti.xcom_push(key="negative_result_path_csv", value=negative_result_path_csv)
    
    lost_data_path = ''
    if 'lost_data_path' not in conf:
        lost_data_path = project_path + '/airflow_runs/' + run_id_path + '/prediction_report/lost_data_path'
        conf['lost_data_path'] = lost_data_path
    lost_data_path = conf["lost_data_path"] + "/" + str(job_id)
    ti.xcom_push(key="lost_data_path", value=lost_data_path)

    params = var_key_lst.get("common", {})
    params.update(var_key_lst.get("Auto_data_matching_prediction_report", {}))

    write_hosts = 'echo "192.168.1.28    spark.master" >> /etc/hosts'
    print(write_hosts)
    print(subprocess.check_output(write_hosts, shell=True, stderr=subprocess.STDOUT))

    install_phcli = 'pip3 install phcli==1.2.3'
    print(install_phcli)
    print(subprocess.check_output(install_phcli, shell=True, stderr=subprocess.STDOUT))

    exec_phcli_submit = 'phcli maxauto --runtime python3 --group Auto_data_matching --path Auto_data_matching_prediction_report --cmd submit ' \
                        '--owner "{}" --run_id "{}" --job_id "{}" --context "{}" "{}"'.format(str(owner), str(run_id), str(job_id), str(params), str(conf))
    print(exec_phcli_submit)
    print(subprocess.check_output(exec_phcli_submit, shell=True, stderr=subprocess.STDOUT))


    # key = ti.xcom_pull(task_ids='test', key='key').decode("UTF-8")
    # ti.xcom_push(key="key", value=key)

Auto_data_matching_prediction_report = PythonOperator(
    task_id='Auto_data_matching_prediction_report',
    provide_context=True,
    python_callable=Auto_data_matching_prediction_report_cmd,
    dag=dag
)
############## == Auto_data_matching_prediction_report == ###################


############## == Auto_data_matching_prediction_report_division == ###################
def Auto_data_matching_prediction_report_division_cmd(**context):
    ti = context['task_instance']
    owner = default_args['owner']
    run_id = context["dag_run"].run_id
    run_id_path = getRunIdPath(run_id)
    job_id = ti.hostname.split("-")[-1]
    conf = context["dag_run"].conf
    
    # inputs
    if 'training_data_path' not in conf:
        training_data_path = ti.xcom_pull(task_ids='Auto_data_matching_crossing', key='result_path')
        conf['training_data_path'] = training_data_path
    if 'split_data_path' not in conf:
        split_data_path = ti.xcom_pull(task_ids='Auto_data_matching_crossing', key='split_data_path')
        conf['split_data_path'] = split_data_path
    if 'positive_result_path' not in conf:
        positive_result_path = ti.xcom_pull(task_ids='Auto_data_matching_prediction_report', key='positive_result_path')
        conf['positive_result_path'] = positive_result_path
    if 'negative_result_path' not in conf:
        negative_result_path = ti.xcom_pull(task_ids='Auto_data_matching_prediction_report', key='negative_result_path')
        conf['negative_result_path'] = negative_result_path

    # outputs
    true_positive_result_path = ''
    true_negative_result_path = ''
    false_positive_result_path = ''
    false_negative_result_path = ''
    true_positive_result_path_csv = ''
    true_negative_result_path_csv = ''
    false_positive_result_path_csv = ''
    false_negative_result_path_csv = ''
    lost_data_path = ''
    final_report_path = ''
    mnf_check_path = ''
    spec_check_path = ''
    dosage_check_path = ''
    if 'true_positive_result_path' not in conf:
        true_positive_result_path = project_path + '/airflow_runs/' + run_id_path + '/prediction_report_division/true_positive_result_path'
        conf['true_positive_result_path'] = true_positive_result_path
    if 'true_negative_result_path' not in conf:
        true_negative_result_path = project_path + '/airflow_runs/' + run_id_path + '/prediction_report_division/true_negative_result_path'
        conf['true_negative_result_path'] = true_negative_result_path
    if 'false_positive_result_path' not in conf:
        false_positive_result_path = project_path + '/airflow_runs/' + run_id_path + '/prediction_report_division/false_positive_result_path'
        conf['false_positive_result_path'] = false_positive_result_path
    if 'false_negative_result_path' not in conf:
        false_negative_result_path = project_path + '/airflow_runs/' + run_id_path + '/prediction_report_division/false_negative_result_path'
        conf['false_negative_result_path'] = false_negative_result_path
    if 'true_positive_result_path_csv' not in conf:
        true_positive_result_path_csv = project_path + '/airflow_runs/' + run_id_path + '/prediction_report_division/true_positive_result_path_csv'
        conf['true_positive_result_path_csv'] = true_positive_result_path_csv
    if 'true_negative_result_path_csv' not in conf:
        true_negative_result_path_csv = project_path + '/airflow_runs/' + run_id_path + '/prediction_report_division/true_negative_result_path_csv'
        conf['true_negative_result_path_csv'] = true_negative_result_path_csv
    if 'false_positive_result_path_csv' not in conf:
        false_positive_result_path_csv = project_path + '/airflow_runs/' + run_id_path + '/prediction_report_division/false_positive_result_path_csv'
        conf['false_positive_result_path_csv'] = false_positive_result_path_csv
    if 'false_negative_result_path_csv' not in conf:
        false_negative_result_path_csv = project_path + '/airflow_runs/' + run_id_path + '/prediction_report_division/false_negative_result_path_csv'
        conf['false_negative_result_path_csv'] = false_negative_result_path_csv
    if 'lost_data_path' not in conf:
        lost_data_path = project_path + '/airflow_runs/' + run_id_path + '/prediction_report_division/lost_data_path'
        conf['lost_data_path'] = lost_data_path
    if 'final_report_path' not in conf:
        final_report_path = project_path + '/airflow_runs/' + run_id_path + '/prediction_report_division/final_report_path'
        conf['final_report_path'] = final_report_path
    if 'mnf_check_path' not in conf:
        mnf_check_path = project_path + '/airflow_runs/' + run_id_path + '/prediction_report_division/mnf_check_path'
        conf['mnf_check_path'] = mnf_check_path
    if 'spec_check_path' not in conf:
        spec_check_path = project_path + '/airflow_runs/' + run_id_path + '/prediction_report_division/spec_check_path'
        conf['spec_check_path'] = spec_check_path
    if 'dosage_check_path' not in conf:
        dosage_check_path = project_path + '/airflow_runs/' + run_id_path + '/prediction_report_division/final_report_path'
        conf['dosage_check_path'] = dosage_check_path
        
    true_positive_result_path = conf["true_positive_result_path"] + "/" + str(job_id)
    true_negative_result_path = conf["true_negative_result_path"] + "/" + str(job_id)
    false_positive_result_path = conf["false_positive_result_path"] + "/" + str(job_id)
    false_negative_result_path = conf["false_negative_result_path"] + "/" + str(job_id)
    true_positive_result_path_csv = conf["true_positive_result_path_csv"] + "/" + str(job_id)
    true_negative_result_path_csv = conf["true_negative_result_path_csv"] + "/" + str(job_id)
    false_positive_result_path_csv = conf["false_positive_result_path_csv"] + "/" + str(job_id)
    false_negative_result_path_csv = conf["false_negative_result_path_csv"] + "/" + str(job_id)
    lost_data_path = conf["lost_data_path"] + "/" + str(job_id)
    final_report_path = conf["final_report_path"] + "/" + str(job_id)
    mnf_check_path = conf["mnf_check_path"] + "/" + str(job_id)
    spec_check_path = conf["spec_check_path"] + "/" + str(job_id)
    dosage_check_path = conf["dosage_check_path"] + "/" + str(job_id)
    
    ti.xcom_push(key="true_positive_result_path", value=true_positive_result_path)
    ti.xcom_push(key="true_negative_result_path", value=true_negative_result_path)
    ti.xcom_push(key="false_positive_result_path", value=false_positive_result_path)
    ti.xcom_push(key="false_negative_result_path", value=false_negative_result_path)
    ti.xcom_push(key="lost_data_path", value=lost_data_path)
    ti.xcom_push(key="final_report_path", value=final_report_path)

    params = var_key_lst.get("common", {})
    params.update(var_key_lst.get("Auto_data_matching_prediction_report_division", {}))

    write_hosts = 'echo "192.168.1.28    spark.master" >> /etc/hosts'
    print(write_hosts)
    print(subprocess.check_output(write_hosts, shell=True, stderr=subprocess.STDOUT))

    install_phcli = 'pip3 install phcli==1.2.3'
    print(install_phcli)
    print(subprocess.check_output(install_phcli, shell=True, stderr=subprocess.STDOUT))

    exec_phcli_submit = 'phcli maxauto --runtime python3 --group Auto_data_matching --path Auto_data_matching_prediction_report_division --cmd submit ' \
                        '--owner "{}" --run_id "{}" --job_id "{}" --context "{}" "{}"'.format(str(owner), str(run_id), str(job_id), str(params), str(conf))
    print(exec_phcli_submit)
    print(subprocess.check_output(exec_phcli_submit, shell=True, stderr=subprocess.STDOUT))

    # key = ti.xcom_pull(task_ids='test', key='key').decode("UTF-8")
    # ti.xcom_push(key="key", value=key)

Auto_data_matching_prediction_report_division = PythonOperator(
    task_id='Auto_data_matching_prediction_report_division',
    provide_context=True,
    python_callable=Auto_data_matching_prediction_report_division_cmd,
    dag=dag
)
############## == Auto_data_matching_prediction_report_division == ###################


############## == Auto_data_matching_training == ###################
def Auto_data_matching_training_cmd(**context):
    ti = context['task_instance']
    owner = default_args['owner']
    run_id = context["dag_run"].run_id
    run_id_path = getRunIdPath(run_id)
    job_id = ti.hostname.split("-")[-1]
    conf = context["dag_run"].conf

    # inputs
    if 'training_data_path' not in conf:
        training_data_path = ti.xcom_pull(task_ids='Auto_data_matching_crossing', key='result_path')
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

    params = var_key_lst.get("common", {})
    params.update(var_key_lst.get("Auto_data_matching_training", {}))

    write_hosts = 'echo "192.168.1.28    spark.master" >> /etc/hosts'
    print(write_hosts)
    print(subprocess.check_output(write_hosts, shell=True, stderr=subprocess.STDOUT))

    install_phcli = 'pip3 install phcli==1.2.3'
    print(install_phcli)
    print(subprocess.check_output(install_phcli, shell=True, stderr=subprocess.STDOUT))

    exec_phcli_submit = 'phcli maxauto --runtime python3 --group Auto_data_matching --path Auto_data_matching_training --cmd submit ' \
                        '--owner "{}" --run_id "{}" --job_id "{}" --context "{}" "{}"'.format(str(owner), str(run_id), str(job_id), str(params), str(conf))
    print(exec_phcli_submit)
    print(subprocess.check_output(exec_phcli_submit, shell=True, stderr=subprocess.STDOUT))

    # key = ti.xcom_pull(task_ids='test', key='key').decode("UTF-8")
    # ti.xcom_push(key="key", value=key)

Auto_data_matching_training = PythonOperator(
    task_id='Auto_data_matching_training',
    provide_context=True,
    python_callable=Auto_data_matching_training_cmd,
    dag=dag
)
############## == Auto_data_matching_training == ###################


############## == Auto_data_matching_training_validate == ###################
def Auto_data_matching_training_validate_cmd(**context):
    ti = context['task_instance']
    owner = default_args['owner']
    run_id = context["dag_run"].run_id
    run_id_path = getRunIdPath(run_id)
    job_id = ti.hostname.split("-")[-1]
    conf = context["dag_run"].conf

    # inputs
    if "validate_path" not in conf:
        validate_path = ti.xcom_pull(task_ids='Auto_data_matching_training', key='validate_path')
        conf['validate_path'] = validate_path
    if "model_path" not in conf:
        model_path = ti.xcom_pull(task_ids='Auto_data_matching_training', key='model_path')
        conf['model_path'] = model_path

    # outputs
    validate_output_path = ''
    if 'validate_output_path' not in conf:
        validate_output_path = project_path + '/airflow_runs/' + run_id_path + '/training_validate/validate_output_path'
        conf['validate_output_path'] = validate_output_path
    validate_output_path = conf["validate_output_path"] + "/" + str(job_id)
    ti.xcom_push(key="validate_output_path", value=validate_output_path)

    params = var_key_lst.get("common", {})
    params.update(var_key_lst.get("Auto_data_matching_training_validate", {}))

    write_hosts = 'echo "192.168.1.28    spark.master" >> /etc/hosts'
    print(write_hosts)
    print(subprocess.check_output(write_hosts, shell=True, stderr=subprocess.STDOUT))

    install_phcli = 'pip3 install phcli==1.2.3'
    print(install_phcli)
    print(subprocess.check_output(install_phcli, shell=True, stderr=subprocess.STDOUT))

    exec_phcli_submit = 'phcli maxauto --runtime python3 --group Auto_data_matching --path Auto_data_matching_training_validate --cmd submit ' \
                        '--owner "{}" --run_id "{}" --job_id "{}" --context "{}" "{}"'.format(str(owner), str(run_id), str(job_id), str(params), str(conf))
    print(exec_phcli_submit)
    print(subprocess.check_output(exec_phcli_submit, shell=True, stderr=subprocess.STDOUT))

    # key = ti.xcom_pull(task_ids='test', key='key').decode("UTF-8")
    # ti.xcom_push(key="key", value=key)

Auto_data_matching_training_validate = PythonOperator(
    task_id='Auto_data_matching_training_validate',
    provide_context=True,
    python_callable=Auto_data_matching_training_validate_cmd,
    dag=dag
)
############## == Auto_data_matching_training_validate == ###################

def getRunIdPath(airflowRunID):
    run_id_dt_str = airflowRunID.split('__')[1].split('+')[0]
    dt = datetime.strptime(run_id_dt_str, '%Y-%m-%dT%H:%M:%S.%f')
    # Python2.7 no support for datetime.timestamp()
    ts = (dt - datetime(1970, 1, 1)).total_seconds()
    run_id_path = str(int(ts))
    return run_id_path

Auto_data_matching_crossing >> Auto_data_matching_training >> Auto_data_matching_training_validate
Auto_data_matching_crossing >> Auto_data_matching_prediction >> Auto_data_matching_prediction_report >> Auto_data_matching_prediction_report_division
