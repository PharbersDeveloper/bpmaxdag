import os
import subprocess
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.models import DAG, Variable
from airflow.operators.bash_operator import BashOperator

args = {
    "owner": "zyyin",
    "start_date": days_ago(1),
    "email": ['airflow@example.com'],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="BPBatchDAG", default_args=args,
    schedule_interval=None,
    description="cpa_match",
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



############## == job1_distinct == ###################
job1_distinct_cmd = """
echo "192.168.1.28    spark.master" >> /etc/hosts
pip install 'phcli==0.2.16'
phcli maxauto --cmd submit --path job1_distinct --context "{{ params }}" "{{ dag_run.conf }}"
"""

job1_distinct = BashOperator(
                    task_id="job1_distinct",
                    bash_command=job1_distinct_cmd,
                    dag=dag,
                    params=dict(common_task_params +
                                spec_task_params.get("job1_distinct".lower(), []))
               )
############## == job1_distinct == ###################




############## == job2_human_replace == ###################
job2_human_replace_cmd = """
echo "192.168.1.28    spark.master" >> /etc/hosts
pip install 'phcli==0.2.16'
phcli maxauto --cmd submit --path job2_human_replace --context "{{ params }}" "{{ dag_run.conf }}"
"""

job2_human_replace = BashOperator(
                    task_id="job2_human_replace",
                    bash_command=job2_human_replace_cmd,
                    dag=dag,
                    params=dict(common_task_params +
                                spec_task_params.get("job2_human_replace".lower(), []))
               )
############## == job2_human_replace == ###################




############## == job3_join == ###################
job3_join_cmd = """
echo "192.168.1.28    spark.master" >> /etc/hosts
pip install 'phcli==0.2.16'
phcli maxauto --cmd submit --path job3_join --context "{{ params }}" "{{ dag_run.conf }}"
"""

job3_join = BashOperator(
                    task_id="job3_join",
                    bash_command=job3_join_cmd,
                    dag=dag,
                    params=dict(common_task_params +
                                spec_task_params.get("job3_join".lower(), []))
               )
############## == job3_join == ###################




############## == job4_edit_distance == ###################
job4_edit_distance_cmd = """
echo "192.168.1.28    spark.master" >> /etc/hosts
pip install 'phcli==0.2.16'
phcli maxauto --cmd submit --path job4_edit_distance --context "{{ params }}" "{{ dag_run.conf }}"
"""

job4_edit_distance = BashOperator(
                    task_id="job4_edit_distance",
                    bash_command=job4_edit_distance_cmd,
                    dag=dag,
                    params=dict(common_task_params +
                                spec_task_params.get("job4_edit_distance".lower(), []))
               )
############## == job4_edit_distance == ###################




############## == job5_match == ###################
job5_match_cmd = """
echo "192.168.1.28    spark.master" >> /etc/hosts
pip install 'phcli==0.2.16'
phcli maxauto --cmd submit --path job5_match --context "{{ params }}" "{{ dag_run.conf }}"
"""

job5_match = BashOperator(
                    task_id="job5_match",
                    bash_command=job5_match_cmd,
                    dag=dag,
                    params=dict(common_task_params +
                                spec_task_params.get("job5_match".lower(), []))
               )
############## == job5_match == ###################




############## == job6_joinback == ###################
job6_joinback_cmd = """
echo "192.168.1.28    spark.master" >> /etc/hosts
pip install 'phcli==0.2.16'
phcli maxauto --cmd submit --path job6_joinback --context "{{ params }}" "{{ dag_run.conf }}"
"""

job6_joinback = BashOperator(
                    task_id="job6_joinback",
                    bash_command=job6_joinback_cmd,
                    dag=dag,
                    params=dict(common_task_params +
                                spec_task_params.get("job6_joinback".lower(), []))
               )
############## == job6_joinback == ###################



job1_distinct >> job2_human_replace >> job3_join >> job4_edit_distance >> job5_match >> job6_joinback
