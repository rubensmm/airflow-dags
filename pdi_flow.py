from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow_pentaho.operators.kettle import KitchenOperator
from airflow_pentaho.operators.kettle import PanOperator
from airflow_pentaho.operators.carte import CarteJobOperator
from airflow_pentaho.operators.carte import CarteTransOperator

DAG_NAME = 'pdi_flow'
DEFAULT_ARGS = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG(dag_id=DAG_NAME,
         default_args=DEFAULT_ARGS,
         dagrun_timeout=timedelta(hours=2),
         schedule_interval='30 0 * * *') as dag:

    job1 = KitchenOperator(
        dag=dag,
        task_id='job1',
        xcom_push=True,
        directory='C:\tmp',
        job='JobKettle',
        params={'date': '{{ ds }}'})

    trans1 = PanOperator(
        dag=dag,
        task_id='trans1',
        xcom_push=True,
        directory='/home/bi',
        trans='test_trans',
        params={'date': '{{ ds }}'})

    trans2 = CarteTransOperator(
        dag=dag,
        task_id='trans2',
        trans='/home/bi/test_trans',
        params={'date': '{{ ds }}'})

    job3 = CarteJobOperator(
        dag=dag,
        task_id='job3',
        job='/home/bi/test_job',
        params={'date': '{{ ds }}'})

    job1 >> trans1 >> trans2 >> job3
