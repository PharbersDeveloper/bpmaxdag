import uuid
import subprocess
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago
from airflow.models import DAG, Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# XComs 有很大问题
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
    tags=["Extract Data", "提数"],
    default_args=default_args,
    schedule_interval=None,
    description="extract data",
    dagrun_timeout=timedelta(minutes=60)
)

var_key_lst = Variable.get("%s__SPARK_CONF" % (
    dag.dag_id), deserialize_json=True, default_var={})

default_extract_data_from = "s3a://ph-stream/common/public/max_result/0.0.5/extract_data_out/out_{}_{}"
date = datetime.now().strftime("%Y_%m_%d")


def replaceSpace(args):
    for item in list(args.keys()):
        args[item] = args[item].replace(" ", "")
    return args

############## == extract_data_extract == ###################


def extract_data_extract_cmd(**context):
    ti = context['task_instance']
    owner = default_args['owner']
    run_id = context["dag_run"].run_id
    job_id = ti.hostname
    args = replaceSpace(context["dag_run"].conf)
    if "owner" in args.keys():
        owner = args["owner"]

    params = var_key_lst.get("common", {})
    params.update(var_key_lst.get("extract_data_extract", {}))

    install_phcli = 'pip3 install phcli==2.0.5'
    print(install_phcli)
    print(subprocess.check_output(install_phcli, shell=True,
                                  stderr=subprocess.STDOUT).decode("utf-8"))

    exec_phcli_submit = 'phcli maxauto online_run --group extract_data --name extract_data_extract ' \
        '--owner "{}" --run_id "{}" --job_id "{}" --context "{}" "{}"'.format(
            str(owner), str(run_id), str(job_id), str(params), str(args))
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
    job_id = ti.hostname
    args = replaceSpace(context["dag_run"].conf)

    if "owner" in args.keys():
        owner = args["owner"]

    if "out_path" not in args.keys():
        args["from"] = default_extract_data_from.format(
            date, args["out_suffix"].replace(" ", ""))
    else:
        args["from"] = "{}/out_{}_{}".format(args["out_put"],
                                             date, args["out_suffix"].replace(" ", ""))

    default_copy_to_path = "s3a://ph-stream/public/asset/jobs/runId_" + \
        str(uuid.uuid4()) + "/extract_data/jobId_" + str(uuid.uuid4())
    ti.xcom_push(key="copyPath", value=default_copy_to_path)
    args["to"] = default_copy_to_path
    args["extract_data_out"] = default_copy_to_path

    params = var_key_lst.get("common", {})
    params.update(var_key_lst.get("extract_data_copy", {}))

    install_phcli = 'pip3 install phcli==2.0.5'
    print(install_phcli)
    print(subprocess.check_output(install_phcli, shell=True,
                                  stderr=subprocess.STDOUT).decode("utf-8"))

    exec_phcli_submit = 'phcli maxauto online_run --group extract_data --name extract_data_copy ' \
        '--owner "{}" --run_id "{}" --job_id "{}" --context "{}" "{}"'.format(
            str(owner), str(run_id), str(job_id), str(params), str(args))
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
    job_id = ti.hostname
    args = replaceSpace(context["dag_run"].conf)

    if "owner" in args.keys():
        owner = args["owner"]

    path = ti.xcom_pull(task_ids='extract_data_copy',
                        key='copyPath') + "/out_{}_{}.zip".format(date, args["out_suffix"])

    args["extract_data_out"] = path

    params = var_key_lst.get("common", {})
    params.update(var_key_lst.get("preset_write_asset", {}))

    install_phcli = 'pip3 install phcli==2.0.5'
    print(install_phcli)
    print(subprocess.check_output(install_phcli, shell=True,
                                  stderr=subprocess.STDOUT).decode("utf-8"))

    exec_phcli_submit = 'phcli maxauto online_run --group extract_data --name preset_write_asset ' \
        '--owner "{}" --run_id "{}" --job_id "{}" --context "{}" "{}"'.format(
            str(owner), str(run_id), str(job_id), str(params), str(args))
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
    job_id = ti.hostname
    args = replaceSpace(context["dag_run"].conf)
    task_id = context['task'].task_id

    if "owner" in args.keys():
        owner = args["owner"]

    args["subject"] = "提数结果"
    path = ti.xcom_pull(task_ids='extract_data_copy',
                        key='copyPath') + "/out_{}_{}.zip".format(date, args["out_suffix"])
    if task_id == "succeed":
        args["content"] = '''
            提数时间：{}
            AWS_S3路径：{}
        '''.format(date, path)
    else:
        args["content"] = "error"

    params = var_key_lst.get("common", {})
    params.update(var_key_lst.get("extract_data_email", {}))

    install_phcli = 'pip3 install phcli==2.0.5'
    print(install_phcli)
    print(subprocess.check_output(install_phcli, shell=True,
                                  stderr=subprocess.STDOUT).decode("utf-8"))

    exec_phcli_submit = 'phcli maxauto online_run --group extract_data --name extract_data_email ' \
        '--owner "{}" --run_id "{}" --job_id "{}" --context "{}" "{}"'.format(
            str(owner), str(run_id), str(job_id), str(params), str(args))
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


############## == extract_data_packaging == ###################
def extract_data_packaging_cmd(**context):
    ti = context['task_instance']
    owner = default_args['owner']
    run_id = context["dag_run"].run_id
    job_id = ti.hostname
    args = replaceSpace(context["dag_run"].conf)

    if "owner" in args.keys():
        owner = args["owner"]

    path = ti.xcom_pull(task_ids='extract_data_copy',
                        key='copyPath')

    args["from"] = path
    args["out_suffix"] = "out_{}_{}".format(date, args["out_suffix"])

    params = var_key_lst.get("common", {})
    params.update(var_key_lst.get("extract_data_packaging", {}))

    install_phcli = 'pip3 install phcli==2.0.5'
    print(install_phcli)
    print(subprocess.check_output(install_phcli, shell=True,
                                  stderr=subprocess.STDOUT).decode("utf-8"))

    exec_phcli_submit = 'phcli maxauto online_run --group extract_data --name extract_data_packaging ' \
                        '--owner "{}" --run_id "{}" --job_id "{}" --context "{}" "{}"'.format(
                            str(owner), str(run_id), str(job_id), str(params), str(args))
    print(exec_phcli_submit)
    print(subprocess.check_output(exec_phcli_submit,
                                  shell=True, stderr=subprocess.STDOUT).decode("utf-8"))

    # key = ti.xcom_pull(task_ids='test', key='key').decode("UTF-8")
    # ti.xcom_push(key="key", value=key)


extract_data_packaging = PythonOperator(
    task_id='extract_data_packaging',
    provide_context=True,
    python_callable=extract_data_packaging_cmd,
    dag=dag
)
############## == extract_data_packaging == ###################


extract_data_extract >> extract_data_copy >> extract_data_packaging >> preset_write_asset >> [
    email_succeed, email_failed]
