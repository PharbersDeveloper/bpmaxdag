import os
import subprocess
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.models import DAG, Variable
from airflow.operators.bash_operator import BashOperator

args = {
    "owner": "clock",
    "start_date": days_ago(1),
    "email": ['airflow@example.com'],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
dag = DAG(
    dag_id="create_hive_table", default_args=args,
    schedule_interval=None,
    description="Create Hive Table And Email Notice DAG",
    dagrun_timeout=timedelta(minutes=60))
    
var_key_lst = Variable.get("%s__SPARK_CONF" % (dag.dag_id), deserialize_json=True, default_var={})

############## == create_hive_table == ###################
create_hive_table_cmd = """
echo "192.168.1.28    spark.master" >> /etc/hosts
pip3 install 'phcli==0.3.6'
phcli maxauto --runtime python3 --cmd submit --namespace create_hive_table --path create_hive_table --context "{{ params }}" "{{ dag_run.conf }}"
"""
create_hive_table = BashOperator(
                    task_id="create_hive_table",
                    bash_command=create_hive_table_cmd,
                    dag=dag,
                    params=dict(var_key_lst.get("common", {}).items() +
                                var_key_lst.get("create_hive_table", {}).items())
               )
############## == create_hive_table == ###################


############## == sqs-email start == ###################
import uuid
import boto3
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

key = Variable.get("SQS_INVOKE__AWS_ACCESS_KEY_ID")
secret = Variable.get("SQS_INVOKE__AWS_SECRET_ACCESS_KEY")
queueName = Variable.get("SQS_INVOKE__QUEUE_NAME")

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
                "StringValue": "zyqi@data-pharbers.com"
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


create_hive_table >> [succeed_operator, failed_operator]