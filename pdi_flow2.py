from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow_pentaho.operators.kettle import KitchenOperator
from airflow_pentaho.operators.kettle import PanOperator
from airflow_pentaho.operators.carte import CarteJobOperator
from airflow_pentaho.operators.carte import CarteTransOperator

DAG_NAME = 'pdi_flow2'
DEFAULT_ARGS = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    #'retries': 1,
    #'retry_delay': timedelta(minutes=1),
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG(dag_id=DAG_NAME,
         default_args=DEFAULT_ARGS,
         dagrun_timeout=timedelta(hours=2),
         schedule_interval='30 0 * * *') as dag:

      loop = CarteTransOperator(
        dag=dag,
        task_id='loop',
        trans='/home/rmendonc/data-integration/transformacoes/LOOP.ktr',
        #trans='C:/tmp/LOOP',
        params={'date': '{{ ds }}'}
      )

      copia = CarteTransOperator(
        dag=dag,
        task_id='copia',
        trans='/home/rmendonc/data-integration/transformacoes/COPIA_ARQUIVO.ktr',
        #trans='C:/tmp/COPIA_ARQUIVO',
        params={'date': '{{ ds }}'}
      )

      loop >> copia