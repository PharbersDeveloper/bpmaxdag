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
    dag_id="Auto_Datamart", default_args=args,
    schedule_interval=None,
    description="Auto Transform Datamart",
    dagrun_timeout=timedelta(minutes=60))
var_key_lst = Variable.get("%s__SPARK_CONF" % (dag.dag_id), deserialize_json=True, default_var={})
############## == datamart_dim_mnfs == ###################
datamart_dim_mnfs_cmd = """
echo "192.168.1.28    spark.master" >> /etc/hosts
pip3 install 'phcli==0.3.6'
phcli maxauto --runtime python3 --cmd submit --namespace Auto_Datamart --path datamart_dim_mnfs --context "{{ params }}" "{{ dag_run.conf }}"
"""
datamart_dim_mnfs = BashOperator(
                    task_id="datamart_dim_mnfs",
                    bash_command=datamart_dim_mnfs_cmd,
                    dag=dag,
                    params=dict(var_key_lst.get("common", {}).items() +
                                var_key_lst.get("datamart_dim_mnfs", {}).items())
               )
############## == datamart_dim_mnfs == ###################
############## == datamart_dim_prod == ###################
datamart_dim_prod_cmd = """
echo "192.168.1.28    spark.master" >> /etc/hosts
pip3 install 'phcli==0.3.6'
phcli maxauto --runtime python3 --cmd submit --namespace Auto_Datamart --path datamart_dim_prod --context "{{ params }}" "{{ dag_run.conf }}"
"""
datamart_dim_prod = BashOperator(
                    task_id="datamart_dim_prod",
                    bash_command=datamart_dim_prod_cmd,
                    dag=dag,
                    params=dict(var_key_lst.get("common", {}).items() +
                                var_key_lst.get("datamart_dim_prod", {}).items())
               )
############## == datamart_dim_prod == ###################
############## == datamart_dim_hosp == ###################
datamart_dim_hosp_cmd = """
echo "192.168.1.28    spark.master" >> /etc/hosts
pip3 install 'phcli==0.3.6'
phcli maxauto --runtime python3 --cmd submit --namespace Auto_Datamart --path datamart_dim_hosp --context "{{ params }}" "{{ dag_run.conf }}"
"""
datamart_dim_hosp = BashOperator(
                    task_id="datamart_dim_hosp",
                    bash_command=datamart_dim_hosp_cmd,
                    dag=dag,
                    params=dict(var_key_lst.get("common", {}).items() +
                                var_key_lst.get("datamart_dim_hosp", {}).items())
               )
############## == datamart_dim_hosp == ###################
############## == datamart_dim_alias == ###################
datamart_dim_alias_cmd = """
echo "192.168.1.28    spark.master" >> /etc/hosts
pip3 install 'phcli==0.3.6'
phcli maxauto --runtime python3 --cmd submit --namespace Auto_Datamart --path datamart_dim_alias --context "{{ params }}" "{{ dag_run.conf }}"
"""
datamart_dim_alias = BashOperator(
                    task_id="datamart_dim_alias",
                    bash_command=datamart_dim_alias_cmd,
                    dag=dag,
                    params=dict(var_key_lst.get("common", {}).items() +
                                var_key_lst.get("datamart_dim_alias", {}).items())
               )
############## == datamart_dim_alias == ###################
############## == datamart_dim_clean_rules == ###################
datamart_dim_clean_rules_cmd = """
echo "192.168.1.28    spark.master" >> /etc/hosts
pip3 install 'phcli==0.3.6'
phcli maxauto --runtime python3 --cmd submit --namespace Auto_Datamart --path datamart_dim_clean_rules --context "{{ params }}" "{{ dag_run.conf }}"
"""
datamart_dim_clean_rules = BashOperator(
                    task_id="datamart_dim_clean_rules",
                    bash_command=datamart_dim_clean_rules_cmd,
                    dag=dag,
                    params=dict(var_key_lst.get("common", {}).items() +
                                var_key_lst.get("datamart_dim_clean_rules", {}).items())
               )
############## == datamart_dim_clean_rules == ###################
############## == datamart_fact_chc_prod == ###################
datamart_fact_chc_prod_cmd = """
echo "192.168.1.28    spark.master" >> /etc/hosts
pip3 install 'phcli==0.3.6'
phcli maxauto --runtime python3 --cmd submit --namespace Auto_Datamart --path datamart_fact_chc_prod --context "{{ params }}" "{{ dag_run.conf }}"
"""
datamart_fact_chc_prod = BashOperator(
                    task_id="datamart_fact_chc_prod",
                    bash_command=datamart_fact_chc_prod_cmd,
                    dag=dag,
                    params=dict(var_key_lst.get("common", {}).items() +
                                var_key_lst.get("datamart_fact_chc_prod", {}).items())
               )
############## == datamart_fact_chc_prod == ###################
############## == datamart_fact_chc_hosp == ###################
datamart_fact_chc_hosp_cmd = """
echo "192.168.1.28    spark.master" >> /etc/hosts
pip3 install 'phcli==0.3.6'
phcli maxauto --runtime python3 --cmd submit --namespace Auto_Datamart --path datamart_fact_chc_hosp --context "{{ params }}" "{{ dag_run.conf }}"
"""
datamart_fact_chc_hosp = BashOperator(
                    task_id="datamart_fact_chc_hosp",
                    bash_command=datamart_fact_chc_hosp_cmd,
                    dag=dag,
                    params=dict(var_key_lst.get("common", {}).items() +
                                var_key_lst.get("datamart_fact_chc_hosp", {}).items())
               )
############## == datamart_fact_chc_hosp == ###################
############## == datamart_fact_chc_date == ###################
datamart_fact_chc_date_cmd = """
echo "192.168.1.28    spark.master" >> /etc/hosts
pip3 install 'phcli==0.3.6'
phcli maxauto --runtime python3 --cmd submit --namespace Auto_Datamart --path datamart_fact_chc_date --context "{{ params }}" "{{ dag_run.conf }}"
"""
datamart_fact_chc_date = BashOperator(
                    task_id="datamart_fact_chc_date",
                    bash_command=datamart_fact_chc_date_cmd,
                    dag=dag,
                    params=dict(var_key_lst.get("common", {}).items() +
                                var_key_lst.get("datamart_fact_chc_date", {}).items())
               )
############## == datamart_fact_chc_date == ###################
############## == datamart_fact_chc_etc == ###################
datamart_fact_chc_etc_cmd = """
echo "192.168.1.28    spark.master" >> /etc/hosts
pip3 install 'phcli==0.3.6'
phcli maxauto --runtime python3 --cmd submit --namespace Auto_Datamart --path datamart_fact_chc_etc --context "{{ params }}" "{{ dag_run.conf }}"
"""
datamart_fact_chc_etc = BashOperator(
                    task_id="datamart_fact_chc_etc",
                    bash_command=datamart_fact_chc_etc_cmd,
                    dag=dag,
                    params=dict(var_key_lst.get("common", {}).items() +
                                var_key_lst.get("datamart_fact_chc_etc", {}).items())
               )
############## == datamart_fact_chc_etc == ###################
############## == datamart_fact_chc == ###################
datamart_fact_chc_cmd = """
echo "192.168.1.28    spark.master" >> /etc/hosts
pip3 install 'phcli==0.3.6'
phcli maxauto --runtime python3 --cmd submit --namespace Auto_Datamart --path datamart_fact_chc --context "{{ params }}" "{{ dag_run.conf }}"
"""
datamart_fact_chc = BashOperator(
                    task_id="datamart_fact_chc",
                    bash_command=datamart_fact_chc_cmd,
                    dag=dag,
                    params=dict(var_key_lst.get("common", {}).items() +
                                var_key_lst.get("datamart_fact_chc", {}).items())
               )
############## == datamart_fact_chc == ###################
############## == datamart_fact_cpa_prod == ###################
datamart_fact_cpa_prod_cmd = """
echo "192.168.1.28    spark.master" >> /etc/hosts
pip3 install 'phcli==0.3.6'
phcli maxauto --runtime python3 --cmd submit --namespace Auto_Datamart --path datamart_fact_cpa_prod --context "{{ params }}" "{{ dag_run.conf }}"
"""
datamart_fact_cpa_prod = BashOperator(
                    task_id="datamart_fact_cpa_prod",
                    bash_command=datamart_fact_cpa_prod_cmd,
                    dag=dag,
                    params=dict(var_key_lst.get("common", {}).items() +
                                var_key_lst.get("datamart_fact_cpa_prod", {}).items())
               )
############## == datamart_fact_cpa_prod == ###################
############## == datamart_fact_cpa_hosp == ###################
datamart_fact_cpa_hosp_cmd = """
echo "192.168.1.28    spark.master" >> /etc/hosts
pip3 install 'phcli==0.3.6'
phcli maxauto --runtime python3 --cmd submit --namespace Auto_Datamart --path datamart_fact_cpa_hosp --context "{{ params }}" "{{ dag_run.conf }}"
"""
datamart_fact_cpa_hosp = BashOperator(
                    task_id="datamart_fact_cpa_hosp",
                    bash_command=datamart_fact_cpa_hosp_cmd,
                    dag=dag,
                    params=dict(var_key_lst.get("common", {}).items() +
                                var_key_lst.get("datamart_fact_cpa_hosp", {}).items())
               )
############## == datamart_fact_cpa_hosp == ###################
############## == datamart_fact_cpa_date == ###################
datamart_fact_cpa_date_cmd = """
echo "192.168.1.28    spark.master" >> /etc/hosts
pip3 install 'phcli==0.3.6'
phcli maxauto --runtime python3 --cmd submit --namespace Auto_Datamart --path datamart_fact_cpa_date --context "{{ params }}" "{{ dag_run.conf }}"
"""
datamart_fact_cpa_date = BashOperator(
                    task_id="datamart_fact_cpa_date",
                    bash_command=datamart_fact_cpa_date_cmd,
                    dag=dag,
                    params=dict(var_key_lst.get("common", {}).items() +
                                var_key_lst.get("datamart_fact_cpa_date", {}).items())
               )
############## == datamart_fact_cpa_date == ###################
############## == datamart_fact_cpa_etc == ###################
datamart_fact_cpa_etc_cmd = """
echo "192.168.1.28    spark.master" >> /etc/hosts
pip3 install 'phcli==0.3.6'
phcli maxauto --runtime python3 --cmd submit --namespace Auto_Datamart --path datamart_fact_cpa_etc --context "{{ params }}" "{{ dag_run.conf }}"
"""
datamart_fact_cpa_etc = BashOperator(
                    task_id="datamart_fact_cpa_etc",
                    bash_command=datamart_fact_cpa_etc_cmd,
                    dag=dag,
                    params=dict(var_key_lst.get("common", {}).items() +
                                var_key_lst.get("datamart_fact_cpa_etc", {}).items())
               )
############## == datamart_fact_cpa_etc == ###################
############## == datamart_fact_cpa == ###################
datamart_fact_cpa_cmd = """
echo "192.168.1.28    spark.master" >> /etc/hosts
pip3 install 'phcli==0.3.6'
phcli maxauto --runtime python3 --cmd submit --namespace Auto_Datamart --path datamart_fact_cpa --context "{{ params }}" "{{ dag_run.conf }}"
"""
datamart_fact_cpa = BashOperator(
                    task_id="datamart_fact_cpa",
                    bash_command=datamart_fact_cpa_cmd,
                    dag=dag,
                    params=dict(var_key_lst.get("common", {}).items() +
                                var_key_lst.get("datamart_fact_cpa", {}).items())
               )
############## == datamart_fact_cpa == ###################
############## == datamart_fact_max_result_prod == ###################
datamart_fact_max_result_prod_cmd = """
echo "192.168.1.28    spark.master" >> /etc/hosts
pip3 install 'phcli==0.3.6'
phcli maxauto --runtime python3 --cmd submit --namespace Auto_Datamart --path datamart_fact_max_result_prod --context "{{ params }}" "{{ dag_run.conf }}"
"""
datamart_fact_max_result_prod = BashOperator(
                    task_id="datamart_fact_max_result_prod",
                    bash_command=datamart_fact_max_result_prod_cmd,
                    dag=dag,
                    params=dict(var_key_lst.get("common", {}).items() +
                                var_key_lst.get("datamart_fact_max_result_prod", {}).items())
               )
############## == datamart_fact_max_result_prod == ###################
############## == datamart_fact_max_result_hosp == ###################
datamart_fact_max_result_hosp_cmd = """
echo "192.168.1.28    spark.master" >> /etc/hosts
pip3 install 'phcli==0.3.6'
phcli maxauto --runtime python3 --cmd submit --namespace Auto_Datamart --path datamart_fact_max_result_hosp --context "{{ params }}" "{{ dag_run.conf }}"
"""
datamart_fact_max_result_hosp = BashOperator(
                    task_id="datamart_fact_max_result_hosp",
                    bash_command=datamart_fact_max_result_hosp_cmd,
                    dag=dag,
                    params=dict(var_key_lst.get("common", {}).items() +
                                var_key_lst.get("datamart_fact_max_result_hosp", {}).items())
               )
############## == datamart_fact_max_result_hosp == ###################
############## == datamart_fact_max_result_etc == ###################
datamart_fact_max_result_etc_cmd = """
echo "192.168.1.28    spark.master" >> /etc/hosts
pip3 install 'phcli==0.3.6'
phcli maxauto --runtime python3 --cmd submit --namespace Auto_Datamart --path datamart_fact_max_result_etc --context "{{ params }}" "{{ dag_run.conf }}"
"""
datamart_fact_max_result_etc = BashOperator(
                    task_id="datamart_fact_max_result_etc",
                    bash_command=datamart_fact_max_result_etc_cmd,
                    dag=dag,
                    params=dict(var_key_lst.get("common", {}).items() +
                                var_key_lst.get("datamart_fact_max_result_etc", {}).items())
               )
############## == datamart_fact_max_result_etc == ###################
############## == datamart_fact_max_result == ###################
datamart_fact_max_result_cmd = """
echo "192.168.1.28    spark.master" >> /etc/hosts
pip3 install 'phcli==0.3.6'
phcli maxauto --runtime python3 --cmd submit --namespace Auto_Datamart --path datamart_fact_max_result --context "{{ params }}" "{{ dag_run.conf }}"
"""
datamart_fact_max_result = BashOperator(
                    task_id="datamart_fact_max_result",
                    bash_command=datamart_fact_max_result_cmd,
                    dag=dag,
                    params=dict(var_key_lst.get("common", {}).items() +
                                var_key_lst.get("datamart_fact_max_result", {}).items())
               )
############## == datamart_fact_max_result == ###################

############## == skip_task DummyOperator == ###################
from airflow.operators.dummy_operator import DummyOperator
skip_task = DummyOperator(task_id='skip_task', dag=dag)
############## == skip_task DummyOperator == ###################


############## == select_data_source == ###################
from airflow.operators.python_operator import BranchPythonOperator
def select_data_source(**kwargs):
	conf = kwargs["dag_run"].conf
	data_source = conf.get('data_source', '')
	if data_source == 'standard':
		return 'standard'
	elif data_source == 'original':
		return 'original'
	else:
		return 'skip_task'

start = BranchPythonOperator(
	task_id='start',
	provide_context=True,
	python_callable=select_data_source,
	dag=dag
)
############## == select_data_source == ###################


############## == select_data_table == ###################
from airflow.operators.python_operator import BranchPythonOperator
def select_data_table_by_standard(**kwargs):
	conf = kwargs["dag_run"].conf
	data_table = conf.get('data_table', '')
	if data_table == 'mnfs':
		return 'datamart_dim_mnfs'
	elif data_table == 'prod':
		return 'datamart_dim_prod'
	elif data_table == 'hosp':
		return 'datamart_dim_hosp'
	elif data_table == 'alias':
		return 'datamart_dim_alias'
	elif data_table == 'clean_rules':
		return 'datamart_dim_clean_rules'
	else:
		return 'skip_task'

standard = BranchPythonOperator(
	task_id='standard',
	provide_context=True,
	python_callable=select_data_table_by_standard,
	dag=dag
)


datamart_fact_cpa_start = DummyOperator(task_id='datamart_fact_cpa_start', dag=dag)
datamart_fact_chc_start = DummyOperator(task_id='datamart_fact_chc_start', dag=dag)
datamart_fact_max_result_start = DummyOperator(task_id='datamart_fact_max_result_start', dag=dag)

def select_data_table_by_original(**kwargs):
	conf = kwargs["dag_run"].conf
	data_table = conf.get('data_table', '')
	if data_table == 'cpa':
		return 'datamart_fact_cpa_start'
	elif data_table == 'chc':
		return 'datamart_fact_chc_start'
	elif data_table == 'max_result':
		return 'datamart_fact_max_result_start'
	else:
		return 'skip_task'

original = BranchPythonOperator(
	task_id='original',
	provide_context=True,
	python_callable=select_data_table_by_original,
	dag=dag
)
############## == select_data_table == ###################     
	
standard >> [datamart_dim_mnfs, datamart_dim_prod, datamart_dim_hosp, datamart_dim_alias, datamart_dim_clean_rules, skip_task]

datamart_fact_cpa_start >> [datamart_fact_cpa_prod, datamart_fact_cpa_hosp, datamart_fact_cpa_date, datamart_fact_cpa_etc] >> datamart_fact_cpa
datamart_fact_chc_start >> [datamart_fact_chc_prod, datamart_fact_chc_hosp, datamart_fact_chc_date, datamart_fact_chc_etc] >> datamart_fact_chc
datamart_fact_max_result_start >> [datamart_fact_max_result_prod, datamart_fact_max_result_hosp, datamart_fact_max_result_etc] >> datamart_fact_max_result
original >> [datamart_fact_cpa_start, datamart_fact_chc_start, datamart_fact_max_result_start, skip_task]

start >> [standard, original, skip_task]


