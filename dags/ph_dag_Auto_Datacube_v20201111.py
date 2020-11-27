#coding=utf-8
import os
import shlex
import subprocess
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago
from airflow.models import DAG, Variable
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
import uuid
import string

# trigger json {"version":"2020-08-11","time_left":"201801","time_right":"201912","start":"201801","end":"201912"}

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
    dag_id="Auto_Datacube_v20201111", default_args=args,
    schedule_interval=None,
    description="A Datacube Auto Job Example",
    dagrun_timeout=timedelta(minutes=60))
var_key_lst = Variable.get("%s__SPARK_CONF" % (dag.dag_id), deserialize_json=True, default_var={})

############## == variables define strat == ###################
project_dict = {'Jingxin':'京新', 'Kangzhe':'康哲', 'Huiyu':'汇宇', 'Haikun':'海坤', 'Beida':'贝达'}
# ['AZ','Astellas','Pfizer']
project_list = Variable.get("%s__PROJECT_LIST" % (dag.dag_id)).split(',')
selector_list = []
start_job_list = []
############## == variables define strat == ###################

############## == common_functions_define strat == ###################
def getTaskId_DagRunConf_Ti(**context):
    task_id = context['task'].task_id
    conf = context["dag_run"].conf
    ti = context['task_instance']
    return task_id, conf, ti

def getDestPath(conf, jobName):
    if 'version' not in conf.keys():
        raise Exception("No version in conf!")
    version = conf['version']
    if 'run_id' not in conf.keys():
        raise Exception("No run_id in conf!")
    runId = conf['run_id']
    jobId = str(uuid.uuid4())
    print("jobId is " + jobId)
    destPath = "s3a://ph-max-auto/" + version +"/cube/jobs/runId_" + runId + "/" + jobName +"/jobId_" + jobId
    print("DestPath is {}.".format(destPath))
    return destPath

def execSparkSubmit(namespace, jobName, conf):
    params=dict(var_key_lst.get("common", {}).items() + var_key_lst.get(jobName, {}).items())
    print(params)
    subprocess.call('echo "192.168.1.28    spark.master" >> /etc/hosts', shell=True)
    subprocess.call("pip install 'phcli==0.3.6'", shell=True)
    # subprocess.call('phcli maxauto --runtime python3 --cmd submit --namespace "{}" --path "{}" --context "{}" "{}"'.format(namespace, jobName, str(params), str(conf)), shell=True)
    shell_cmd = 'phcli maxauto --runtime python3 --cmd submit --namespace "{}" --path "{}" --context "{}" "{}"'.format(namespace, jobName, str(params), str(conf))
    cmd = shlex.split(shell_cmd)
    p = subprocess.Popen(cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    while p.poll() is None:
        line = p.stdout.readline()
        line = line.strip()
        if line:
            print('Spark submit logs: [{}]'.format(line))
    if p.returncode == 0:
        print('Spark submit succeed.')
    else:
        raise Exception('Spark submit failed with {}'.format(jobName))
############## == common_functions_define end == ###################

############## == generate_runId strat == ###################
def generate_runId_func(**context):
    ti = context['task_instance']
    run_id = str(uuid.uuid4())
    ti.xcom_push(key="run_id", value=run_id)
    return run_id

generate_runId = PythonOperator(
    task_id='generate_runId',
    provide_context=True,
    python_callable=generate_runId_func,
    dag=dag
)
############## == generate_runId end == ###################

############## == metacube start == ###################
def metacube_func(**context):
    task_id, conf, ti = getTaskId_DagRunConf_Ti(**context)
    jobName = task_id
    runId = ti.xcom_pull(task_ids='generate_runId', key='run_id').decode("UTF-8")
    if runId == '':
        raise Exception("Invalid runId!", runId)
    conf['run_id'] = runId

    # output path
    cuboids_path = ''
    lattices_path = ''
    dimensions_path = ''
    if ('cuboids_path' not in conf.keys()) or ('lattices_path' not in conf.keys()) or ('dimensions_path' not in conf.keys()):
        destPath = getDestPath(conf, jobName)
        cuboids_path = destPath + "/meta/cuboids"
        lattices_path = destPath + "/meta/lattices"
        dimensions_path = destPath + "/meta/dimensions"
        conf[u'cuboids_path'] = cuboids_path
        conf[u'lattices_path'] = lattices_path
        conf[u'dimensions_path'] = dimensions_path
    print(str(conf))
    cuboids_path = conf[u'cuboids_path']
    lattices_path = conf[u'lattices_path']
    dimensions_path = conf[u'dimensions_path']
    ti.xcom_push(key="cuboids_path", value=cuboids_path)
    ti.xcom_push(key="lattices_path", value=lattices_path)
    ti.xcom_push(key="dimensions_path", value=dimensions_path)

    execSparkSubmit((dag.dag_id), jobName, conf)
    return task_id

metacube = PythonOperator(
    task_id='metacube',
    provide_context=True,
    python_callable=metacube_func,
    dag=dag
)
############## == metacube end == ###################

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

succeed = PythonOperator(
    task_id='succeed',
    provide_context=True,
    python_callable=send_sqs,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    depends_on_past=False,
    dag=dag
)

failed = PythonOperator(
    task_id='failed',
    provide_context=True,
    python_callable=send_sqs,
    trigger_rule=TriggerRule.ONE_FAILED,
    depends_on_past=False,
    dag=dag
)
############## == sqs-email end == ###################

############## == extract_data with project start == ###################
def extract_data_func(**context):
    task_id, conf, ti = getTaskId_DagRunConf_Ti(**context)
    jobName = task_id.split('__')[0]
    project = task_id.split('__')[1]
    runId = ti.xcom_pull(task_ids='generate_runId', key='run_id').decode("UTF-8")
    if runId == '':
        raise Exception("Invalid runId!", runId)
    conf['run_id'] = runId

    # output path
    extract_out_path = ''
    if 'extract_out_path' not in conf.keys():
        destPath = getDestPath(conf, jobName)
        extract_out_path = destPath + "/content"
        conf[u'extract_out_path'] = extract_out_path
    extract_out_path = conf[u'extract_out_path']
    conf[u'out_path'] = extract_out_path
    conf[u'project'] = project
    if project in project_dict:
        conf[u'project'] = project_dict[project]
    print(str(conf))
    date = datetime.now().strftime("%Y_%m_%d")
    extract_out_path = extract_out_path + "/out_" + date + "_test/out_" + date + "_test.csv"
    ti.xcom_push(key="extract_out_path", value=extract_out_path)

    execSparkSubmit("Auto_extract_data", jobName, conf)
    return task_id

extract_data_start = DummyOperator(task_id="extract_data_start", dag=dag)
extract_data_end = DummyOperator(task_id="extract_data_end", dag=dag)
############## == extract_data with project end == ###################

############## == cleanextractdata with project strat == ###################
def cleanextractdata_func(**context):
    task_id, conf, ti = getTaskId_DagRunConf_Ti(**context)
    jobName = task_id.split('__')[0]
    project = task_id.split('__')[1]
    runId = ti.xcom_pull(task_ids='generate_runId', key='run_id').decode("UTF-8")
    if runId == '':
        raise Exception("Invalid runId!", runId)
    conf['run_id'] = runId

    # input path
    if 'max_result_path__%s' % project not in conf.keys():
        max_result_path = ti.xcom_pull(task_ids='extract_data__%s' % project, key='extract_out_path').decode("UTF-8")
        if max_result_path == '':
            raise Exception("Invalid max_result_path!", max_result_path)
        conf['max_result_path'] = max_result_path
    else:
        conf['max_result_path'] = conf['max_result_path__%s' % project]

    # output path
    cleancube_result_path = ''
    if 'cleancube_result_path' not in conf.keys():
        destPath = getDestPath(conf, jobName)
        cleancube_result_path = destPath + "/content"
        conf[u'cleancube_result_path'] = cleancube_result_path
    cleancube_result_path = conf[u'cleancube_result_path']
    print(str(conf))
    ti.xcom_push(key="cleancube_result_path", value=cleancube_result_path)

    execSparkSubmit((dag.dag_id), jobName, conf)
    return task_id

cleanextractdata_start = DummyOperator(task_id="cleanextractdata_start", dag=dag)
cleanextractdata_end = DummyOperator(task_id="cleanextractdata_end", dag=dag)
############## == cleanextractdata end == ###################

############## == lattices start == ###################
def lattices_func(**context):
    task_id, conf, ti = getTaskId_DagRunConf_Ti(**context)
    jobName = task_id.split('__')[0]
    project = task_id.split('__')[1]
    runId = ti.xcom_pull(task_ids='generate_runId', key='run_id').decode("UTF-8")
    if runId == '':
        raise Exception("Invalid runId!", runId)
    conf['run_id'] = runId

    # input path
    if 'lattices_path' not in conf.keys():
        lattices_path = ti.xcom_pull(task_ids='metacube', key='lattices_path').decode("UTF-8")
        conf[u'lattices_path'] = lattices_path
    else:
        conf[u'lattices_path'] = conf['lattices_path']

    if 'cleancube_result_path__%s' % project not in conf.keys():
        cleancube_result_path = ti.xcom_pull(task_ids='cleanextractdata__%s' % project, key='cleancube_result_path').decode("UTF-8")
        conf[u'cleancube_result_path'] = cleancube_result_path
    else:
        conf[u'cleancube_result_path'] = conf['cleancube_result_path__%s' % project]

    # output path
    lattices_content_path = ''
    if 'lattices_content_path' not in conf.keys():
        destPath = getDestPath(conf, jobName)
        lattices_content_path = destPath + "/content"
        conf[u'lattices_content_path'] = lattices_content_path
    lattices_content_path = conf[u'lattices_content_path']
    print(str(conf))
    ti.xcom_push(key="lattices_content_path", value=lattices_content_path)

    execSparkSubmit((dag.dag_id), jobName, conf)
    return task_id

lattices_start = DummyOperator(task_id="lattices_start", dag=dag)
lattices_end = DummyOperator(task_id="lattices_end", dag=dag)
############## == lattices end == ###################

############## == bucketlattices start == ###################
def bucketlattices_func(**context):
    task_id, conf, ti = getTaskId_DagRunConf_Ti(**context)
    jobName = task_id.split('__')[0]
    project = task_id.split('__')[1]
    runId = ti.xcom_pull(task_ids='generate_runId', key='run_id').decode("UTF-8")
    if runId == '':
        raise Exception("Invalid runId!", runId)
    conf['run_id'] = runId

    # input path
    if 'lattices_content_path__%s' % project not in conf.keys():
        lattices_content_path = ti.xcom_pull(task_ids='lattices__%s' % project, key='lattices_content_path').decode("UTF-8")
        conf[u'lattices_content_path'] = lattices_content_path
    else:
        conf[u'lattices_content_path'] = conf['lattices_content_path__%s' % project]

    # output path
    lattices_bucket_content_path = ''
    if 'lattices_bucket_content_path' not in conf.keys():
        destPath = getDestPath(conf, jobName)
        lattices_bucket_content_path = destPath + "/content"
        conf[u'lattices_bucket_content_path'] = lattices_bucket_content_path
    lattices_bucket_content_path = conf[u'lattices_bucket_content_path']
    print(str(conf))
    ti.xcom_push(key="lattices_bucket_content_path", value=lattices_bucket_content_path)

    execSparkSubmit((dag.dag_id), jobName, conf)
    return task_id

bucketlattices_start = DummyOperator(task_id="bucketlattices_start", dag=dag)
bucketlattices_end = DummyOperator(task_id="bucketlattices_end", dag=dag)
############## == bucketlattices end == ###################

############## == latticesresult start == ###################
def latticesresult_func(**context):
    task_id, conf, ti = getTaskId_DagRunConf_Ti(**context)
    jobName = task_id.split('__')[0]
    project = task_id.split('__')[1]
    runId = ti.xcom_pull(task_ids='generate_runId', key='run_id').decode("UTF-8")
    if runId == '':
        raise Exception("Invalid runId!", runId)
    conf['run_id'] = runId

    # input path
    if 'lattices_path' not in conf.keys():
        lattices_path = ti.xcom_pull(task_ids='metacube', key='lattices_path').decode("UTF-8")
        conf[u'lattices_path'] = lattices_path
    else:
        conf[u'lattices_path'] = conf['lattices_path']

    if 'dimensions_path' not in conf.keys():
        dimensions_path = ti.xcom_pull(task_ids='metacube', key='dimensions_path').decode("UTF-8")
        conf[u'dimensions_path'] = dimensions_path
    else:
        conf[u'dimensions_path'] = conf['dimensions_path']

    if 'cleancube_result_path__%s' % project not in conf.keys():
        cleancube_result_path = ti.xcom_pull(task_ids='cleanextractdata__%s' % project, key='cleancube_result_path').decode("UTF-8")
        conf[u'cleancube_result_path'] = cleancube_result_path
    else:
        conf[u'cleancube_result_path'] = conf['cleancube_result_path__%s' % project]

    if 'lattices_bucket_content_path__%s' % project not in conf.keys():
        lattices_bucket_content_path = ti.xcom_pull(task_ids='bucketlattices__%s' % project, key='lattices_bucket_content_path').decode("UTF-8")
        conf[u'lattices_bucket_content_path'] = lattices_bucket_content_path
    else:
        conf[u'lattices_bucket_content_path'] = conf['lattices_bucket_content_path__%s' % project]

    # output path
    lattices_result_path = ''
    if 'lattices_result_path' not in conf.keys():
        destPath = getDestPath(conf, jobName)
        lattices_result_path = destPath + "/content"
        conf[u'lattices_result_path'] = lattices_result_path
    lattices_result_path = conf[u'lattices_result_path']
    print(str(conf))
    ti.xcom_push(key="lattices_result_path", value=lattices_result_path)

    execSparkSubmit((dag.dag_id), jobName, conf)
    return task_id

latticesresult_start = DummyOperator(task_id="latticesresult_start", dag=dag)
latticesresult_end = DummyOperator(task_id="latticesresult_end", dag=dag)
############## == latticesresult end == ###################

############## == finalresult start == ###################
def finalresult_func(**context):
    task_id, conf, ti = getTaskId_DagRunConf_Ti(**context)
    jobName = task_id.split('__')[0]
    project = task_id.split('__')[1]
    runId = ti.xcom_pull(task_ids='generate_runId', key='run_id').decode("UTF-8")
    if runId == '':
        raise Exception("Invalid runId!", runId)
    conf['run_id'] = runId

    # input path
    if 'dimensions_path' not in conf.keys():
        dimensions_path = ti.xcom_pull(task_ids='metacube', key='dimensions_path').decode("UTF-8")
        conf[u'dimensions_path'] = dimensions_path
    else:
        conf[u'dimensions_path'] = conf['dimensions_path']

    if 'lattices_result_path__%s' % project not in conf.keys():
        lattices_result_path = ti.xcom_pull(task_ids='latticesresult__%s' % project, key='lattices_result_path').decode("UTF-8")
        conf[u'lattices_result_path'] = lattices_result_path
    else:
        conf[u'lattices_result_path'] = conf['lattices_result_path__%s' % project]

    # output path
    final_result_path = ''
    if 'final_result_path' not in conf.keys():
        destPath = getDestPath(conf, jobName)
        final_result_path = destPath + "/content"
        conf[u'final_result_path'] = final_result_path
    final_result_path = conf[u'final_result_path']
    print(str(conf))
    ti.xcom_push(key="final_result_path", value=final_result_path)

    execSparkSubmit((dag.dag_id), jobName, conf)
    return task_id

finalresult_start = DummyOperator(task_id="finalresult_start", dag=dag)
finalresult_end = DummyOperator(task_id="finalresult_end", dag=dag)
############## == finalresult end == ###################

############## == calmeasures start == ###################
def calmeasures_func(**context):
    task_id, conf, ti = getTaskId_DagRunConf_Ti(**context)
    jobName = task_id.split('__')[0]
    project = task_id.split('__')[1]
    runId = ti.xcom_pull(task_ids='generate_runId', key='run_id').decode("UTF-8")
    if runId == '':
        raise Exception("Invalid runId!", runId)
    conf['run_id'] = runId

    # input path
    if 'final_result_path__%s' % project not in conf.keys():
        final_result_path = ti.xcom_pull(task_ids='finalresult__%s' % project, key='final_result_path').decode("UTF-8")
        conf[u'final_result_path'] = final_result_path
    else:
        conf[u'final_result_path'] = conf['final_result_path__%s' % project]

    # output path
    ver_measures_content_path = ''
    if 'ver_measures_content_path' not in conf.keys():
        destPath = getDestPath(conf, jobName)
        ver_measures_content_path = destPath + "/content"
        conf[u'ver_measures_content_path'] = ver_measures_content_path
    ver_measures_content_path = conf[u'ver_measures_content_path']
    print(str(conf))
    ti.xcom_push(key="ver_measures_content_path", value=ver_measures_content_path)

    execSparkSubmit((dag.dag_id), jobName, conf)
    return task_id

calmeasures_start = DummyOperator(task_id="calmeasures_start", dag=dag)
calmeasures_end = DummyOperator(task_id="calmeasures_end", dag=dag)
############## == calmeasures end == ###################

############## == horcalmeasures start == ###################
def horcalmeasures_func(**context):
    task_id, conf, ti = getTaskId_DagRunConf_Ti(**context)
    jobName = task_id.split('__')[0]
    project = task_id.split('__')[1]
    runId = ti.xcom_pull(task_ids='generate_runId', key='run_id').decode("UTF-8")
    if runId == '':
        raise Exception("Invalid runId!", runId)
    conf['run_id'] = runId

    # input path
    if 'ver_measures_content_path__%s' % project not in conf.keys():
        ver_measures_content_path = ti.xcom_pull(task_ids='calmeasures__%s' % project, key='ver_measures_content_path').decode("UTF-8")
        conf[u'ver_measures_content_path'] = ver_measures_content_path
    else:
        conf[u'ver_measures_content_path'] = conf['ver_measures_content_path__%s' % project]

    # output path
    hor_measures_content_path = ''
    if 'hor_measures_content_path' not in conf.keys():
        destPath = getDestPath(conf, jobName)
        hor_measures_content_path = destPath + "/content"
        conf[u'hor_measures_content_path'] = hor_measures_content_path
    hor_measures_content_path = conf[u'hor_measures_content_path']
    print(str(conf))
    ti.xcom_push(key="hor_measures_content_path", value=hor_measures_content_path)

    execSparkSubmit((dag.dag_id), jobName, conf)
    return task_id

horcalmeasures_start = DummyOperator(task_id="horcalmeasures_start", dag=dag)
horcalmeasures_end = DummyOperator(task_id="horcalmeasures_end", dag=dag)
############## == horcalmeasures end == ###################

############## == write2postgres start == ###################
def write2postgres_func(**context):
    task_id, conf, ti = getTaskId_DagRunConf_Ti(**context)
    jobName = task_id.split('__')[0]
    project = task_id.split('__')[1]
    runId = ti.xcom_pull(task_ids='generate_runId', key='run_id').decode("UTF-8")
    if runId == '':
        raise Exception("Invalid runId!", runId)
    conf['run_id'] = runId

    # input path
    if 'hor_measures_content_path__%s' % project not in conf.keys():
        hor_measures_content_path = ti.xcom_pull(task_ids='horcalmeasures__%s' % project, key='hor_measures_content_path').decode("UTF-8")
        conf[u'hor_measures_content_path'] = hor_measures_content_path
    else:
        conf[u'hor_measures_content_path'] = conf['hor_measures_content_path__%s' % project]

    # output path
    table_name = project
    ti.xcom_push(key="table_name", value=table_name)

    execSparkSubmit((dag.dag_id), jobName, conf)
    return task_id

write2postgres_start = DummyOperator(task_id="write2postgres_start", dag=dag)
write2postgres_end = DummyOperator(task_id="write2postgres_end", dag=dag)
############## == write2postgres end == ###################

############## == job combine with project_list start == ###################
for project in project_list:
    extract_data = PythonOperator(
        task_id='extract_data__%s' % project,
        provide_context=True,
        python_callable=extract_data_func,
        dag=dag
    )
    extract_data_start >> extract_data >> extract_data_end

    cleanextractdata = PythonOperator(
        task_id='cleanextractdata__%s' % project,
        provide_context=True,
        python_callable=cleanextractdata_func,
        dag=dag
    )
    cleanextractdata_start >> cleanextractdata >> cleanextractdata_end

    lattices = PythonOperator(
        task_id='lattices__%s' % project,
        provide_context=True,
        python_callable=lattices_func,
        dag=dag
    )
    lattices_start >> lattices >> lattices_end

    bucketlattices = PythonOperator(
        task_id='bucketlattices__%s' % project,
        provide_context=True,
        python_callable=bucketlattices_func,
        dag=dag
    )
    bucketlattices_start >> bucketlattices >> bucketlattices_end

    latticesresult = PythonOperator(
        task_id='latticesresult__%s' % project,
        provide_context=True,
        python_callable=latticesresult_func,
        dag=dag
    )
    latticesresult_start >> latticesresult >> latticesresult_end

    finalresult = PythonOperator(
        task_id='finalresult__%s' % project,
        provide_context=True,
        python_callable=finalresult_func,
        dag=dag
    )
    finalresult_start >> finalresult >> finalresult_end

    calmeasures = PythonOperator(
        task_id='calmeasures__%s' % project,
        provide_context=True,
        python_callable=calmeasures_func,
        dag=dag
    )
    calmeasures_start >> calmeasures >> calmeasures_end

    horcalmeasures = PythonOperator(
        task_id='horcalmeasures__%s' % project,
        provide_context=True,
        python_callable=horcalmeasures_func,
        dag=dag
    )
    horcalmeasures_start >> horcalmeasures >> horcalmeasures_end

    write2postgres = PythonOperator(
        task_id='write2postgres__%s' % project,
        provide_context=True,
        python_callable=write2postgres_func,
        dag=dag
    )
    write2postgres_start >> write2postgres >> write2postgres_end
############## == job combine with project_list end == ###################

############## == empty_task start == ###################
empty_task = DummyOperator(task_id='empty_task', dag=dag)
############## == empty_task end == ###################

############## == selector start == ###################
selector_list.append(write2postgres_start)
selector_list.append(horcalmeasures_start)
selector_list.append(calmeasures_start)
selector_list.append(finalresult_start)
selector_list.append(latticesresult_start)
selector_list.append(bucketlattices_start)
selector_list.append(lattices_start)
selector_list.append(cleanextractdata_start)
selector_list.append(extract_data_start)
selector_list.append(generate_runId)
selector_list.append(metacube)
selector_list.append(empty_task)
start_job_list.append("generate_runId")
start_job_list.append("extract_data_start")
start_job_list.append("cleanextractdata_start")
start_job_list.append("lattices_start")
start_job_list.append("bucketlattices_start")
start_job_list.append("latticesresult_start")
start_job_list.append("finalresult_start")
start_job_list.append("calmeasures_start")
start_job_list.append("horcalmeasures_start")
start_job_list.append("write2postgres_start")

start_job = Variable.get("%s__START_JOB" % (dag.dag_id))
if start_job == "write2postgres_start":
    generate_runId >> write2postgres_start
    write2postgres_end >> [succeed, failed]
elif start_job == "horcalmeasures_start":
    generate_runId >> horcalmeasures_start
    horcalmeasures_end >> write2postgres_start
    write2postgres_end >> [succeed, failed]
elif start_job == "calmeasures_start":
    generate_runId >> calmeasures_start
    calmeasures_end >> horcalmeasures_start
    horcalmeasures_end >> write2postgres_start
    write2postgres_end >> [succeed, failed]
elif start_job == "finalresult_start":
    generate_runId >> finalresult_start
    finalresult_end >> calmeasures_start
    calmeasures_end >> horcalmeasures_start
    horcalmeasures_end >> write2postgres_start
    write2postgres_end >> [succeed, failed]
elif start_job == "latticesresult_start":
    generate_runId >> latticesresult_start
    latticesresult_end >> finalresult_start
    finalresult_end >> calmeasures_start
    calmeasures_end >> horcalmeasures_start
    horcalmeasures_end >> write2postgres_start
    write2postgres_end >> [succeed, failed]
elif start_job == "bucketlattices_start":
    generate_runId >> bucketlattices_start
    bucketlattices_end >> latticesresult_start
    latticesresult_end >> finalresult_start
    finalresult_end >> calmeasures_start
    calmeasures_end >> horcalmeasures_start
    horcalmeasures_end >> write2postgres_start
    write2postgres_end >> [succeed, failed]
elif start_job == "lattices_start":
    generate_runId >> lattices_start
    lattices_end >> bucketlattices_start
    bucketlattices_end >> latticesresult_start
    latticesresult_end >> finalresult_start
    finalresult_end >> calmeasures_start
    calmeasures_end >> horcalmeasures_start
    horcalmeasures_end >> write2postgres_start
    write2postgres_end >> [succeed, failed]
elif start_job == "cleanextractdata_start":
    generate_runId >> cleanextractdata_start
    cleanextractdata_end >> lattices_start
    lattices_end >> bucketlattices_start
    bucketlattices_end >> latticesresult_start
    latticesresult_end >> finalresult_start
    finalresult_end >> calmeasures_start
    calmeasures_end >> horcalmeasures_start
    horcalmeasures_end >> write2postgres_start
    write2postgres_end >> [succeed, failed]
elif start_job == "extract_data_start":
    generate_runId >> extract_data_start
    extract_data_end >> cleanextractdata_start
    cleanextractdata_end >> lattices_start
    lattices_end >> bucketlattices_start
    bucketlattices_end >> latticesresult_start
    latticesresult_end >> finalresult_start
    finalresult_end >> calmeasures_start
    calmeasures_end >> horcalmeasures_start
    horcalmeasures_end >> write2postgres_start
    write2postgres_end >> [succeed, failed]
else:
    generate_runId >> metacube
    metacube >> extract_data_start
    extract_data_end >> cleanextractdata_start
    cleanextractdata_end >> lattices_start
    lattices_end >> bucketlattices_start
    bucketlattices_end >> latticesresult_start
    latticesresult_end >> finalresult_start
    finalresult_end >> calmeasures_start
    calmeasures_end >> horcalmeasures_start
    horcalmeasures_end >> write2postgres_start
    write2postgres_end >> [succeed, failed]

def selector_func(**context):
    if start_job in start_job_list:
        return "generate_runId"
    return "empty_task"

selector = BranchPythonOperator(
	task_id='selector',
	provide_context=True,
	python_callable=selector_func,
	dag=dag
)
# ############## == selector end == ###################
#
selector >> selector_list
