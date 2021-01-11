import os
import subprocess
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.models import DAG, Variable
from airflow.operators.bash_operator import BashOperator
args = {
    "owner": "jqyu",
    "start_date": days_ago(1),
    "email": ['airflow@example.com'],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
dag = DAG(
    dag_id="output_pot_universe", default_args=args,
    schedule_interval=None,
    description="Output Potential Universe",
    dagrun_timeout=timedelta(minutes=60))
var_key_lst = Variable.get("%s__SPARK_CONF" % (dag.dag_id), deserialize_json=True, default_var={})
############## == datamart_output_hosp_pot == ###################
datamart_output_hosp_pot_cmd = """
echo "192.168.1.28    spark.master" >> /etc/hosts
pip3 install 'phcli==0.3.6'
phcli maxauto --runtime python3 --cmd submit --namespace output_pot_universe --path datamart_output_hosp_pot --context "{{ params }}" "{{ dag_run.conf }}"
"""
datamart_output_hosp_pot = BashOperator(
                    task_id="datamart_output_hosp_pot",
                    bash_command=datamart_output_hosp_pot_cmd,
                    dag=dag,
                    params=dict(var_key_lst.get("common", {}).items() +
                                var_key_lst.get("datamart_output_hosp_pot", {}).items())
               )
############## == datamart_output_hosp_pot == ###################
datamart_output_hosp_pot
