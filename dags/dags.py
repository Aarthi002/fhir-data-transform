# ~/airflow/dags/spark_dag.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1)
}

dag = DAG(dag_id='spark_dag', default_args=default_args, schedule_interval='@daily')

run_spark_job = BashOperator(
    task_id='run_spark_job',
    bash_command='source /Users/aarthimurali/Documents/GitHub/fhir-data-transform/venv/bin/activate && python /Users/aarthimurali/Documents/GitHub/fhir-data-transform/recon.py',
    dag=dag
)

