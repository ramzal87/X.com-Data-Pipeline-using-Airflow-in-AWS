from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from xtweet_etl import xtweet_etl

default_args = {
    'owner': 'ramzal',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 20),
    'email': ['mc.ramzal@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'xtweet_dag',
    default_args=default_args,
    description='x tweet data ETL process',
    schedule_interval=timedelta(days=1)
)

run_etl = PythonOperator(
    task_id = "etl01",
    python_callable = xtweet_etl,
    dag = dag
)

run_etl