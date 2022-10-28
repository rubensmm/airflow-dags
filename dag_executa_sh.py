from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.contrib.hooks import SSHHook

sshHook = SSHHook(conn_id=SSH_ETL_DESENV)

with DAG(
    "dag_executa_sh",start_date = datetime(2022,10,5),
    schedule_interval = '5 * * * *', catchup = False) as dag:
    
    executa_sh = SSHExecuteOperator(
        task_id="executa_sh",
        bash_command="/home/etl/etl/sh/teste_rubens.sh -h",
        ssh_hook=sshHook,
        dag=dag
    )

    executa_sh