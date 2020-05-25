
from airflow.operators.bash_operator import BashOperator
from airflow.models import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from phconfig import PhYAMLConfig

args = {
    "owner": "alfred_owner_plase_change_it",
    "start_date": days_ago(1),
    "email": ['airflow@example.com'],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="alfred_test_id_plase_change_it", default_args=args,
    schedule_interval="days=1",
    description="A Max Auto Job Example",
    dagrun_timeout=timedelta(minutes=60))


def create_dag_job(name, command):
    return BashOperator(
        task_id=name,
        bash_command=command,
        dag=dag,
    )


############## == test1 == ###################
config_test1 = PhYAMLConfig("/Users/alfredyang/Desktop/code/pharbers/BP_Max_AutoJob/phjobs/test", "test1")
config_test1.load_yaml()
cb_test1 = ""
entry_point = config_test1.spec.containers.code
if "/" not in entry_point:
    entry_point = "/Users/alfredyang/Desktop/code/pharbers/BP_Max_AutoJob/phjobs/test" + "/" + entry_point
    cb = ["python", entry_point]
    for arg in config_test1.spec.containers.args:
        cb.append("--" + arg.key + "=" + str(arg.value))
    cb_test1 = cb

test1 = create_dag_job("test1", cb_test1)
############## == test1 == ###################


############## == test2 == ###################
config_test2 = PhYAMLConfig("/Users/alfredyang/Desktop/code/pharbers/BP_Max_AutoJob/phjobs/test", "test2")
config_test2.load_yaml()
cb_test2 = ""
entry_point = config_test2.spec.containers.code
if "/" not in entry_point:
    entry_point = "/Users/alfredyang/Desktop/code/pharbers/BP_Max_AutoJob/phjobs/test" + "/" + entry_point
    cb = ["python", entry_point]
    for arg in config_test2.spec.containers.args:
        cb.append("--" + arg.key + "=" + str(arg.value))
    cb_test2 = cb

test2 = create_dag_job("test2", cb_test2)
############## == test2 == ###################


test1 >> test2
