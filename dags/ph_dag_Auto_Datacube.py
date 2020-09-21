import os
import subprocess
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
import uuid
import string

# trigger json {"version":"2020-08-11", "max_result_path":"s3a://ph-stream/common/public/max_result/0.0.4"}

args = {
    "owner": "jeorch",
    "start_date": days_ago(1),
    "email": ['czhang@data-pharbers.com'],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
dag = DAG(
    dag_id="Auto_Datacube", default_args=args,
    schedule_interval=None,
    description="A Datacube Auto Job Example",
    dagrun_timeout=timedelta(minutes=60))
var_key_lst = (subprocess.check_output("airflow variables", shell=True)).decode('utf-8').split('\n')
var_key_lst = [key for key in var_key_lst if key.upper().startswith(dag.dag_id.upper())]
dag_params = [(key, Variable.get(key)) for key in var_key_lst]
common_task_params = []
spec_task_params = {}
for k, v in dag_params:
    k_lst = k.split('__')
    if len(k_lst) == 3:
        common_task_params.append(('__'.join(k_lst[1:]), v))
    if len(k_lst) == 4:
        spec_params = spec_task_params.get(k_lst[1], [])
        spec_params.append(('__'.join(k_lst[2:]), v))
        spec_task_params[k_lst[1].lower()] = spec_params

############## == generate_id strat == ###################
def generate_id(**context):
    ti = context['task_instance']
    run_id = str(uuid.uuid4())
    ti.xcom_push(key="run_id", value=run_id)
    return run_id

generate_runId = PythonOperator(
    task_id='generate_id',
    provide_context=True,
    python_callable=generate_id,
    dag=dag
)
############## == generate_id end == ###################

############## == metacube start == ###################
def metacube_func(**context):

    jobId = ''

    conf = context["dag_run"].conf
    print("origin conf is ", conf)

    ti = context['task_instance']

    runId = ti.xcom_pull(task_ids='generate_id', key='run_id').decode("UTF-8")
    print("runId is ", runId)
    conf['run_id'] = runId

    # output path
    cuboids_path = ''
    lattices_path = ''
    dimensions_path = ''
    if ('cuboids_path' not in conf.keys()) or ('lattices_path' not in conf.keys()) or ('dimensions_path' not in conf.keys()):
        jobName = "metacube"
        if 'version' not in conf.keys():
            raise Exception("Invalid version!", version)
        version = conf['version']
        jobId = str(uuid.uuid4())
        print("jobId is " + jobId)
        destPath = "s3a://ph-max-auto/" + version +"/jobs/runId_" + runId + "/" + jobName +"/jobId_" + jobId
        print("DestPath is {}.".format(destPath))
        cuboids_path = destPath + "/meta/cuboids"
        lattices_path = destPath + "/meta/lattices"
        dimensions_path = destPath + "/meta/dimensions"
        conf[u'cuboids_path'] = cuboids_path
        conf[u'lattices_path'] = lattices_path
        conf[u'dimensions_path'] = dimensions_path
    cuboids_path = conf[u'cuboids_path']
    lattices_path = conf[u'lattices_path']
    dimensions_path = conf[u'dimensions_path']
    print(str(conf))

    if jobId == '':
        jobId = lattices_content_path.split("jobId_")[1].split("/")[0]
        print("jobId = %s" % jobId)

    params=dict(common_task_params + spec_task_params.get("metacube".lower(), []))
    print(params)

    subprocess.call('echo "192.168.1.28    spark.master" >> /etc/hosts', shell=True)
    subprocess.call("pip install 'phcli==0.2.16'", shell=True)
    print subprocess.check_output('phcli maxauto --cmd submit --path metacube --context "{}" "{}"'.format(str(params), str(conf)), shell=True)

    ti.xcom_push(key="cuboids_path", value=cuboids_path)
    ti.xcom_push(key="lattices_path", value=lattices_path)
    ti.xcom_push(key="dimensions_path", value=dimensions_path)
    return jobId

metacube = PythonOperator(
    task_id='metacube',
    provide_context=True,
    python_callable=metacube_func,
    dag=dag
)
############## == metacube end == ###################

############## == cleancube start == ###################
def cleancube_func(**context):

    jobId = ''

    conf = context["dag_run"].conf
    print("origin conf is ", conf)

    ti = context['task_instance']

    runId = ti.xcom_pull(task_ids='generate_id', key='run_id').decode("UTF-8")
    print("runId is ", runId)

    if 'max_result_path' not in conf.keys():
        raise Exception("Invalid max_result_path!", max_result_path)

    # output path
    cleancube_result_path = ''
    if 'cleancube_result_path' not in conf.keys():
        jobName = "cleancube"
        if 'version' not in conf.keys():
            raise Exception("Invalid version!", version)
        version = conf['version']
        jobId = str(uuid.uuid4())
        print("jobId is " + jobId)
        destPath = "s3a://ph-max-auto/" + version +"/jobs/runId_" + runId + "/" + jobName +"/jobId_" + jobId
        print("DestPath is {}.".format(destPath))
        cleancube_result_path = destPath + "/content"
        conf[u'cleancube_result_path'] = cleancube_result_path
    cleancube_result_path = conf[u'cleancube_result_path']
    print(str(conf))

    if jobId == '':
        jobId = lattices_content_path.split("jobId_")[1].split("/")[0]
        print("jobId = %s" % jobId)

    params=dict(common_task_params + spec_task_params.get("cleancube".lower(), []))
    print(params)

    subprocess.call('echo "192.168.1.28    spark.master" >> /etc/hosts', shell=True)
    subprocess.call("pip install 'phcli==0.2.16'", shell=True)
    print subprocess.check_output('phcli maxauto --cmd submit --path cleancube --context "{}" "{}"'.format(str(params), str(conf)), shell=True)

    ti.xcom_push(key="cleancube_result_path", value=cleancube_result_path)
    return jobId

cleancube = PythonOperator(
    task_id='cleancube',
    provide_context=True,
    python_callable=cleancube_func,
    dag=dag
)
############## == cleancube end == ###################

############## == lattices start == ###################
def lattices_func(**context):

    jobId = ''

    conf = context["dag_run"].conf
    print("origin conf is ", conf)

    ti = context['task_instance']

    runId = ti.xcom_pull(task_ids='generate_id', key='run_id').decode("UTF-8")
    print("runId is ", runId)

    lattices_path = ti.xcom_pull(task_ids='metacube', key='lattices_path').decode("UTF-8")
    conf[u'lattices_path'] = lattices_path
    print("lattices_path is ", lattices_path)
    if not lattices_path:
        raise Exception("Invalid lattices_path!", lattices_path)

    cleancube_result_path = ti.xcom_pull(task_ids='cleancube', key='cleancube_result_path').decode("UTF-8")
    conf[u'cleancube_result_path'] = cleancube_result_path
    print("cleancube_result_path is ", cleancube_result_path)
    if not cleancube_result_path:
        raise Exception("Invalid cleancube_result_path!", cleancube_result_path)

    # output path
    lattices_content_path = ''
    if 'lattices_content_path' not in conf.keys():
        jobName = "lattices"
        if 'version' not in conf.keys():
            raise Exception("Invalid version!", version)
        version = conf['version']
        jobId = str(uuid.uuid4())
        print("jobId is " + jobId)
        destPath = "s3a://ph-max-auto/" + version +"/jobs/runId_" + runId + "/" + jobName +"/jobId_" + jobId
        print("DestPath is {}.".format(destPath))
        lattices_content_path = destPath + "/content"
        conf[u'lattices_content_path'] = lattices_content_path
    lattices_content_path = conf[u'lattices_content_path']
    print(str(conf))

    if jobId == '':
        jobId = lattices_content_path.split("jobId_")[1].split("/")[0]
        print("jobId = %s" % jobId)

    params=dict(common_task_params + spec_task_params.get("lattices".lower(), []))
    print(params)

    subprocess.call('echo "192.168.1.28    spark.master" >> /etc/hosts', shell=True)
    subprocess.call("pip install 'phcli==0.2.16'", shell=True)
    print subprocess.check_output('phcli maxauto --cmd submit --path lattices --context "{}" "{}"'.format(str(params), str(conf)), shell=True)

    ti.xcom_push(key="lattices_content_path", value=lattices_content_path)
    return jobId

lattices = PythonOperator(
    task_id='lattices',
    provide_context=True,
    python_callable=lattices_func,
    dag=dag
)
############## == lattices end == ###################

############## == bucketlattices start == ###################
def bucketlattices_func(**context):

    jobId = ''

    conf = context["dag_run"].conf
    print("origin conf is ", conf)

    ti = context['task_instance']

    runId = ti.xcom_pull(task_ids='generate_id', key='run_id').decode("UTF-8")
    print("runId is ", runId)

    # input
    lattices_content_path = ti.xcom_pull(task_ids='lattices', key='lattices_content_path').decode("UTF-8")
    conf[u'lattices_content_path'] = lattices_content_path
    print("lattices_content_path is ", lattices_content_path)
    if not lattices_content_path:
        raise Exception("Invalid lattices_content_path!", lattices_content_path)

    # output path
    lattices_bucket_content_path = ''
    if 'lattices_bucket_content_path' not in conf.keys():
        jobName = "bucketlattices"
        if 'version' not in conf.keys():
            raise Exception("Invalid version!", version)
        version = conf['version']
        jobId = str(uuid.uuid4())
        print("jobId is " + jobId)
        destPath = "s3a://ph-max-auto/" + version +"/jobs/runId_" + runId + "/" + jobName +"/jobId_" + jobId
        print("DestPath is {}.".format(destPath))
        lattices_bucket_content_path = destPath + "/content"
        conf[u'lattices_bucket_content_path'] = lattices_bucket_content_path
    lattices_bucket_content_path = conf[u'lattices_bucket_content_path']
    print(str(conf))

    if jobId == '':
        jobId = lattices_bucket_content_path.split("jobId_")[1].split("/")[0]
        print("jobId = %s" % jobId)

    params=dict(common_task_params + spec_task_params.get("bucketlattices".lower(), []))
    print(params)

    subprocess.call('echo "192.168.1.28    spark.master" >> /etc/hosts', shell=True)
    subprocess.call("pip install 'phcli==0.2.16'", shell=True)
    print subprocess.check_output('phcli maxauto --cmd submit --path bucketlattices --context "{}" "{}"'.format(str(params), str(conf)), shell=True)

    ti.xcom_push(key="lattices_bucket_content_path", value=lattices_bucket_content_path)
    return jobId

bucketlattices = PythonOperator(
    task_id='bucketlattices',
    provide_context=True,
    python_callable=bucketlattices_func,
    dag=dag
)
############## == bucketlattices end == ###################

############## == latticesresult start == ###################
def latticesresult_func(**context):

    jobId = ''

    conf = context["dag_run"].conf
    print("origin conf is ", conf)

    ti = context['task_instance']

    runId = ti.xcom_pull(task_ids='generate_id', key='run_id').decode("UTF-8")
    print("runId is ", runId)

    # input path
    lattices_path = ti.xcom_pull(task_ids='metacube', key='lattices_path').decode("UTF-8")
    conf[u'lattices_path'] = lattices_path
    print("lattices_path is ", lattices_path)
    if not lattices_path:
        raise Exception("Invalid lattices_path!", lattices_path)

    dimensions_path = ti.xcom_pull(task_ids='metacube', key='dimensions_path').decode("UTF-8")
    conf[u'dimensions_path'] = dimensions_path
    print("dimensions_path is ", dimensions_path)
    if not dimensions_path:
        raise Exception("Invalid dimensions_path!", dimensions_path)

    lattices_bucket_content_path = ti.xcom_pull(task_ids='bucketlattices', key='lattices_bucket_content_path').decode("UTF-8")
    conf[u'lattices_bucket_content_path'] = lattices_bucket_content_path
    print("lattices_bucket_content_path is ", lattices_bucket_content_path)
    if not lattices_bucket_content_path:
        raise Exception("Invalid lattices_bucket_content_path!", lattices_bucket_content_path)

    # output path
    lattices_result_path = ''
    if 'lattices_result_path' not in conf.keys():
        jobName = "latticesresult"
        if 'version' not in conf.keys():
            raise Exception("Invalid version!", version)
        version = conf['version']
        jobId = str(uuid.uuid4())
        print("jobId is " + jobId)
        destPath = "s3a://ph-max-auto/" + version +"/jobs/runId_" + runId + "/" + jobName +"/jobId_" + jobId
        print("DestPath is {}.".format(destPath))
        lattices_result_path = destPath + "/content"
        conf[u'lattices_result_path'] = lattices_result_path
    lattices_result_path = conf[u'lattices_result_path']
    print(str(conf))

    if jobId == '':
        jobId = lattices_result_path.split("jobId_")[1].split("/")[0]
        print("jobId = %s" % jobId)

    params=dict(common_task_params + spec_task_params.get("latticesresult".lower(), []))
    print(params)

    subprocess.call('echo "192.168.1.28    spark.master" >> /etc/hosts', shell=True)
    subprocess.call("pip install 'phcli==0.2.16'", shell=True)
    print subprocess.check_output('phcli maxauto --cmd submit --path latticesresult --context "{}" "{}"'.format(str(params), str(conf)), shell=True)

    ti.xcom_push(key="lattices_result_path", value=lattices_result_path)
    return jobId

latticesresult = PythonOperator(
    task_id='latticesresult',
    provide_context=True,
    python_callable=latticesresult_func,
    dag=dag
)
############## == latticesresult end == ###################

############## == finalresult start == ###################
def finalresult_func(**context):

    jobId = ''

    conf = context["dag_run"].conf
    print("origin conf is ", conf)

    ti = context['task_instance']

    runId = ti.xcom_pull(task_ids='generate_id', key='run_id').decode("UTF-8")
    print("runId is ", runId)

    # input path
    dimensions_path = ti.xcom_pull(task_ids='metacube', key='dimensions_path').decode("UTF-8")
    conf[u'dimensions_path'] = dimensions_path
    print("dimensions_path is ", dimensions_path)
    if not dimensions_path:
        raise Exception("Invalid dimensions_path!", dimensions_path)

    lattices_result_path = ti.xcom_pull(task_ids='latticesresult', key='lattices_result_path').decode("UTF-8")
    conf[u'lattices_result_path'] = lattices_result_path
    print("lattices_result_path is ", lattices_result_path)
    if not lattices_result_path:
        raise Exception("Invalid lattices_result_path!", lattices_result_path)

    # output path
    final_result_path = ''
    if 'final_result_path' not in conf.keys():
        jobName = "finalresult"
        if 'version' not in conf.keys():
            raise Exception("Invalid version!", version)
        version = conf['version']
        jobId = str(uuid.uuid4())
        print("jobId is " + jobId)
        destPath = "s3a://ph-max-auto/" + version +"/jobs/runId_" + runId + "/" + jobName +"/jobId_" + jobId
        print("DestPath is {}.".format(destPath))
        final_result_path = destPath + "/content"
        conf[u'final_result_path'] = final_result_path
    final_result_path = conf[u'final_result_path']
    print(str(conf))

    if jobId == '':
        jobId = final_result_path.split("jobId_")[1].split("/")[0]
        print("jobId = %s" % jobId)

    params=dict(common_task_params + spec_task_params.get("finalresult".lower(), []))
    print(params)

    subprocess.call('echo "192.168.1.28    spark.master" >> /etc/hosts', shell=True)
    subprocess.call("pip install 'phcli==0.2.16'", shell=True)
    print subprocess.check_output('phcli maxauto --cmd submit --path finalresult --context "{}" "{}"'.format(str(params), str(conf)), shell=True)

    ti.xcom_push(key="final_result_path", value=final_result_path)
    return jobId

finalresult = PythonOperator(
    task_id='finalresult',
    provide_context=True,
    python_callable=finalresult_func,
    dag=dag
)
############## == finalresult end == ###################

############## == calmeasures start == ###################
def calmeasures_func(**context):

    jobId = ''

    conf = context["dag_run"].conf
    print("origin conf is ", conf)

    ti = context['task_instance']

    runId = ti.xcom_pull(task_ids='generate_id', key='run_id').decode("UTF-8")
    print("runId is ", runId)

    # input path
    final_result_path = ti.xcom_pull(task_ids='finalresult', key='final_result_path').decode("UTF-8")
    conf[u'final_result_path'] = final_result_path
    print("final_result_path is ", final_result_path)
    if not final_result_path:
        raise Exception("Invalid final_result_path!", final_result_path)

    # output path
    ver_measures_content_path = ''
    if 'ver_measures_content_path' not in conf.keys():
        jobName = "calmeasures"
        if 'version' not in conf.keys():
            raise Exception("Invalid version!", version)
        version = conf['version']
        jobId = str(uuid.uuid4())
        print("jobId is " + jobId)
        destPath = "s3a://ph-max-auto/" + version +"/jobs/runId_" + runId + "/" + jobName +"/jobId_" + jobId
        print("DestPath is {}.".format(destPath))
        ver_measures_content_path = destPath + "/content"
        conf[u'ver_measures_content_path'] = ver_measures_content_path
    ver_measures_content_path = conf[u'ver_measures_content_path']
    print(str(conf))

    if jobId == '':
        jobId = ver_measures_content_path.split("jobId_")[1].split("/")[0]
        print("jobId = %s" % jobId)

    params=dict(common_task_params + spec_task_params.get("calmeasures".lower(), []))
    print(params)

    subprocess.call('echo "192.168.1.28    spark.master" >> /etc/hosts', shell=True)
    subprocess.call("pip install 'phcli==0.2.16'", shell=True)
    print subprocess.check_output('phcli maxauto --cmd submit --path calmeasures --context "{}" "{}"'.format(str(params), str(conf)), shell=True)

    ti.xcom_push(key="ver_measures_content_path", value=ver_measures_content_path)
    return jobId

calmeasures = PythonOperator(
    task_id='calmeasures',
    provide_context=True,
    python_callable=calmeasures_func,
    dag=dag
)
############## == calmeasures end == ###################

############## == horcalmeasures start == ###################
def horcalmeasures_func(**context):

    jobId = ''

    conf = context["dag_run"].conf
    print("origin conf is ", conf)

    ti = context['task_instance']

    runId = ti.xcom_pull(task_ids='generate_id', key='run_id').decode("UTF-8")
    print("runId is ", runId)

    # input path
    ver_measures_content_path = ti.xcom_pull(task_ids='calmeasures', key='ver_measures_content_path').decode("UTF-8")
    conf[u'ver_measures_content_path'] = ver_measures_content_path
    print("ver_measures_content_path is ", ver_measures_content_path)
    if not ver_measures_content_path:
        raise Exception("Invalid ver_measures_content_path!", ver_measures_content_path)

    # output path
    hor_measures_content_path = ''
    if 'hor_measures_content_path' not in conf.keys():
        jobName = "horcalmeasures"
        if 'version' not in conf.keys():
            raise Exception("Invalid version!", version)
        version = conf['version']
        jobId = str(uuid.uuid4())
        print("jobId is " + jobId)
        destPath = "s3a://ph-max-auto/" + version +"/jobs/runId_" + runId + "/" + jobName +"/jobId_" + jobId
        print("DestPath is {}.".format(destPath))
        hor_measures_content_path = destPath + "/content"
        conf[u'hor_measures_content_path'] = hor_measures_content_path
    hor_measures_content_path = conf[u'hor_measures_content_path']
    print(str(conf))

    if jobId == '':
        jobId = hor_measures_content_path.split("jobId_")[1].split("/")[0]
        print("jobId = %s" % jobId)

    params=dict(common_task_params + spec_task_params.get("horcalmeasures".lower(), []))
    print(params)

    subprocess.call('echo "192.168.1.28    spark.master" >> /etc/hosts', shell=True)
    subprocess.call("pip install 'phcli==0.2.16'", shell=True)
    print subprocess.check_output('phcli maxauto --cmd submit --path horcalmeasures --context "{}" "{}"'.format(str(params), str(conf)), shell=True)

    ti.xcom_push(key="hor_measures_content_path", value=hor_measures_content_path)
    return jobId

horcalmeasures = PythonOperator(
    task_id='horcalmeasures',
    provide_context=True,
    python_callable=horcalmeasures_func,
    dag=dag
)
############## == horcalmeasures end == ###################

############## == write2postgres start == ###################
def write2postgres_func(**context):

    jobId = ''

    conf = context["dag_run"].conf
    print("origin conf is ", conf)

    ti = context['task_instance']

    runId = ti.xcom_pull(task_ids='generate_id', key='run_id').decode("UTF-8")
    print("runId is ", runId)

    # input path
    hor_measures_content_path = ti.xcom_pull(task_ids='horcalmeasures', key='hor_measures_content_path').decode("UTF-8")
    conf[u'hor_measures_content_path'] = hor_measures_content_path
    print("hor_measures_content_path is ", hor_measures_content_path)
    if not hor_measures_content_path:
        raise Exception("Invalid hor_measures_content_path!", hor_measures_content_path)

    # output path
    postgres_uri = ''
    if 'postgres_uri' not in conf.keys():
        postgres_uri = Variable.get("GEN_CUBE__POSTGRES_URL")
        postgres_user = Variable.get("GEN_CUBE__POSTGRES_USER")
        postgres_pass = Variable.get("GEN_CUBE__POSTGRES_PASS")
        if not postgres_uri:
            raise Exception("Invalid postgres_uri!", postgres_uri)
        conf[u'postgres_uri'] = postgres_uri
        conf[u'postgres_user'] = postgres_user
        conf[u'postgres_pass'] = postgres_pass
    postgres_uri = conf[u'postgres_uri']
    print(str(conf))

    if jobId == '':
        jobId = postgres_uri
        print("jobId = %s" % jobId)

    params=dict(common_task_params + spec_task_params.get("write2postgres".lower(), []))
    print(params)

    subprocess.call('echo "192.168.1.28    spark.master" >> /etc/hosts', shell=True)
    subprocess.call("pip install 'phcli==0.2.16'", shell=True)
    print subprocess.check_output('phcli maxauto --cmd submit --path write2postgres --context "{}" "{}"'.format(str(params), str(conf)), shell=True)

    ti.xcom_push(key="postgres_uri", value=postgres_uri)
    return jobId

write2postgres = PythonOperator(
    task_id='write2postgres',
    provide_context=True,
    python_callable=write2postgres_func,
    dag=dag
)
############## == write2postgres end == ###################

############## == sqs-email start == ###################
key = Variable.get("SQS_INVOKE__AWS_ACCESS_KEY_ID")
secret = Variable.get("SQS_INVOKE__AWS_SECRET_ACCESS_KEY")
queueName = Variable.get("SQS_INVOKE__QUEUE_NAME")

import boto3
def send_sqs(**context):

    md_id = str(uuid.uuid4())
    sqs = boto3.resource('sqs', aws_access_key_id=key, aws_secret_access_key=secret)
    queue = sqs.get_queue_by_name(QueueName=queueName)
    response = queue.send_message(
        MessageBody='SendEmailHandle',
        MessageGroupId='sqs-invoke-demo',
        MessageDeduplicationId=md_id,
        MessageAttributes={
            "To": {
                "DataType": "String",
                "StringValue": "czhang@data-pharbers.com"
            },
            "Subject": {
                "DataType": "String",
                "StringValue": "from airflow"
            },
            "ContentType": {
                "DataType": "String",
                "StringValue": "text/plain"
            },
            "Content": {
                "DataType": "String",
                "StringValue": context['task'].task_id
            }
        }
    )
    return response

succeed_operator = PythonOperator(
    task_id='succeed',
    provide_context=True,
    python_callable=send_sqs,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    depends_on_past=False,
    dag=dag
)

failed_operator = PythonOperator(
    task_id='failed',
    provide_context=True,
    python_callable=send_sqs,
    trigger_rule=TriggerRule.ONE_FAILED,
    depends_on_past=False,
    dag=dag
)
############## == sqs-email end == ###################

generate_runId >> metacube >> cleancube >> lattices >> bucketlattices >> latticesresult >> finalresult >> calmeasures >> horcalmeasures >> write2postgres >> [succeed_operator, failed_operator]
