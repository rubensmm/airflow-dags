from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 10, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('KettleGetBeerTags', default_args=default_args, schedule_interval=timedelta(days=1))

t1 = BashOperator(
    task_id='RunKettle',
    bash_command='/Users/codek/apps/kettle-remix-20190130/data-integration/kitchen.sh -file=/Users/codek/projects/airflow/jb-extract-beer-tags.kjb',
    dag=dag)
