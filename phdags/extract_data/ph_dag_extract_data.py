import os
import uuid
import string
import subprocess
from datetime import timedelta, datetime
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
    dag_id="extract_data",
    tags=['Extract Data', '提数'],
    default_args=default_args,
    schedule_interval=None,
    description="extract data",
    dagrun_timeout=timedelta(minutes=60)
)

var_key_lst = Variable.get("%s__SPARK_CONF" % (
    dag.dag_id), deserialize_json=True, default_var={})

default_extract_data_from = "s3a://ph-stream/common/public/max_result/0.0.5/extract_data_out/out_{}_{}"
date = datetime.now().strftime("%Y_%m_%d")

############## == extract_data_extract == ###################


def extract_data_extract_cmd(**context):
    ti = context['task_instance']
    owner = default_args['owner']
    run_id = context["dag_run"].run_id
    job_id = ti.hostname.split("-")[-1]
    conf = context["dag_run"].conf

    params = var_key_lst.get("common", {})
    params.update(var_key_lst.get("extract_data_extract", {}))

    write_hosts = 'echo "192.168.1.28    spark.master" >> /etc/hosts'
    print(write_hosts)
    print(subprocess.check_output(write_hosts, shell=True,
                                  stderr=subprocess.STDOUT).decode("utf-8"))

    install_phcli = 'pip3 install phcli==1.2.3'
    print(install_phcli)
    print(subprocess.check_output(install_phcli, shell=True,
                                  stderr=subprocess.STDOUT).decode("utf-8"))

    exec_phcli_submit = 'LANG=C.UTF-8 phcli maxauto --runtime python3 --group extract_data --path extract_data_extract --cmd submit ' \
        '--owner "{}" --run_id "{}" --job_id "{}" --context "{}" "{}"'.format(
            str(owner), str(run_id), str(job_id), str(params), str(conf))
    print(exec_phcli_submit)
    print(subprocess.check_output(exec_phcli_submit,
                                  shell=True, stderr=subprocess.STDOUT).decode("utf-8"))

    # key = ti.xcom_pull(task_ids='test', key='key').decode("UTF-8")
    # ti.xcom_push(key="key", value=key)


extract_data_extract = PythonOperator(
    task_id='extract_data_extract',
    provide_context=True,
    python_callable=extract_data_extract_cmd,
    dag=dag
)
############## == extract_data_extract == ###################


############## == extract_data_copy == ###################
def extract_data_copy_cmd(**context):
    ti = context['task_instance']
    owner = default_args['owner']
    run_id = context["dag_run"].run_id
    job_id = ti.hostname.split("-")[-1]
    conf = context["dag_run"].conf

    if "out_path" not in conf.keys():
        conf["from"] = default_extract_data_from.format(
            date, conf["out_suffix"])
    else:
        conf["from"] = "{}/out_{}_{}".format(conf["out_put"],
                                             date, conf["out_suffix"])

    default_copy_to_path = "s3a://ph-stream/public/asset/jobs/runId_" + \
        str(uuid.uuid4()) + "/extract_data/jobId_" + str(uuid.uuid4())
    ti.xcom_push(key="copyPath", value=default_copy_to_path)
    conf["to"] = default_copy_to_path

    params = var_key_lst.get("common", {})
    params.update(var_key_lst.get("extract_data_copy", {}))

    write_hosts = 'echo "192.168.1.28    spark.master" >> /etc/hosts'
    print(write_hosts)
    print(subprocess.check_output(write_hosts, shell=True,
                                  stderr=subprocess.STDOUT).decode("utf-8"))

    install_phcli = 'pip3 install phcli==1.2.3'
    print(install_phcli)
    print(subprocess.check_output(install_phcli, shell=True,
                                  stderr=subprocess.STDOUT).decode("utf-8"))

    exec_phcli_submit = 'LANG=C.UTF-8 phcli maxauto --runtime python3 --group extract_data --path extract_data_copy --cmd submit ' \
        '--owner "{}" --run_id "{}" --job_id "{}" --context "{}" "{}"'.format(
            str(owner), str(run_id), str(job_id), str(params), str(conf))
    print(exec_phcli_submit)
    print(subprocess.check_output(exec_phcli_submit,
                                  shell=True, stderr=subprocess.STDOUT).decode("utf-8"))

    # key = ti.xcom_pull(task_ids='test', key='key').decode("UTF-8")
    # ti.xcom_push(key="key", value=key)


extract_data_copy = PythonOperator(
    task_id='extract_data_copy',
    provide_context=True,
    python_callable=extract_data_copy_cmd,
    dag=dag
)
############## == extract_data_copy == ###################


############## == preset_write_asset == ###################
def preset_write_asset_cmd(**context):
    ti = context['task_instance']
    owner = default_args['owner']
    run_id = context["dag_run"].run_id
    job_id = ti.hostname.split("-")[-1]
    conf = context["dag_run"].conf

    params = var_key_lst.get("common", {})
    params.update(var_key_lst.get("preset_write_asset", {}))

    write_hosts = 'echo "192.168.1.28    spark.master" >> /etc/hosts'
    print(write_hosts)
    print(subprocess.check_output(write_hosts, shell=True,
                                  stderr=subprocess.STDOUT).decode("utf-8"))

    install_phcli = 'pip3 install phcli==1.2.3'
    print(install_phcli)
    print(subprocess.check_output(install_phcli, shell=True,
                                  stderr=subprocess.STDOUT).decode("utf-8"))

    exec_phcli_submit = 'LANG=C.UTF-8 phcli maxauto --runtime python3 --group extract_data --path preset_write_asset --cmd submit ' \
                        '--owner "{}" --run_id "{}" --job_id "{}" --context "{}" "{}"'.format(
                            str(owner), str(run_id), str(job_id), str(params), str(conf))
    print(exec_phcli_submit)
    print(subprocess.check_output(exec_phcli_submit,
                                  shell=True, stderr=subprocess.STDOUT).decode("utf-8"))

    # key = ti.xcom_pull(task_ids='test', key='key').decode("UTF-8")
    # ti.xcom_push(key="key", value=key)


preset_write_asset = PythonOperator(
    task_id='preset_write_asset',
    provide_context=True,
    python_callable=preset_write_asset_cmd,
    dag=dag
)
############## == preset_write_asset == ###################


############## == extract_data_email == ###################
def extract_data_email_cmd(**context):
    ti = context['task_instance']
    owner = default_args['owner']
    run_id = context["dag_run"].run_id
    job_id = ti.hostname.split("-")[-1]
    conf = context["dag_run"].conf
    task_id = context['task'].task_id
    conf["subject"] = "提数结果"
    path = ti.xcom_pull(task_ids='extract_data_copy',
                        key='copyPath') + "/" + conf["out_suffix"]
    if task_id == "succeed":
        conf["content"] = '''
            链接24小时后过期
            时间：{}
            S3路径：{}
        '''.format(date, path)
    else:
        conf["content"] = "error"

    params = var_key_lst.get("common", {})
    params.update(var_key_lst.get("extract_data_email", {}))

    write_hosts = 'echo "192.168.1.28    spark.master" >> /etc/hosts'
    print(write_hosts)
    print(subprocess.check_output(write_hosts, shell=True,
                                  stderr=subprocess.STDOUT).decode("utf-8"))

    install_phcli = 'pip3 install phcli==1.2.3'
    print(install_phcli)
    print(subprocess.check_output(install_phcli, shell=True,
                                  stderr=subprocess.STDOUT).decode("utf-8"))

    exec_phcli_submit = 'LANG=C.UTF-8 phcli maxauto --runtime python3 --group extract_data --path extract_data_email --cmd submit ' \
        '--owner "{}" --run_id "{}" --job_id "{}" --context "{}" "{}"'.format(
            str(owner), str(run_id), str(job_id), str(params), str(conf))
    print(exec_phcli_submit)
    print(subprocess.check_output(exec_phcli_submit,
                                  shell=True, stderr=subprocess.STDOUT).decode("utf-8"))

    # key = ti.xcom_pull(task_ids='test', key='key').decode("UTF-8")
    # ti.xcom_push(key="key", value=key)


email_succeed = PythonOperator(
    task_id='succeed',
    provide_context=True,
    python_callable=extract_data_email_cmd,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    depends_on_past=False,
    dag=dag
)

email_failed = PythonOperator(
    task_id='failed',
    provide_context=True,
    python_callable=extract_data_email_cmd,
    trigger_rule=TriggerRule.ONE_FAILED,
    depends_on_past=False,
    dag=dag
)
############## == extract_data_email == ###################


extract_data_extract >> extract_data_copy >> preset_write_asset >> [
    email_succeed, email_failed]
