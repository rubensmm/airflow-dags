from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow_pentaho.operators.kettle import KitchenOperator
from airflow_pentaho.operators.kettle import PanOperator
from airflow_pentaho.operators.carte import CarteJobOperator
from airflow_pentaho.operators.carte import CarteTransOperator

DAG_NAME = 'pdi_flow_job'
DEFAULT_ARGS = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG(dag_id=DAG_NAME,
         default_args=DEFAULT_ARGS,
         dagrun_timeout=timedelta(hours=2),
         schedule_interval='30 0 * * *') as dag:

      job_loop = CarteJobOperator(
        dag=dag,
        task_id="job_loop",
        job="C:/tmp/ETL_AIRFLOW/Job_loop.kjb",
        params={"date": "{{ ds }}"})

      job_copia = CarteJobOperator(
        dag=dag,
        task_id="job_copia",
        job="C:/tmp/ETL_AIRFLOW/Job_copia_arquivo.kjb",
        params={"date": "{{ ds }}"})
  
      job_loop >> job_copia
