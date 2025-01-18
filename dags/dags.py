# spark_dag.py

from airflow import DAG 
from airflow.operators.bash import BashOperator # Updated import path 
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(dag_id='spark_dag', default_args=default_args, schedule='@daily')

spark_job = BashOperator(
    task_id='spark_job',
    bash_command='python recon.py',
    dag=dag
)
