from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.contrib.hooks.ssh_hook import SSHHook

def executa_sh():
    ssh = SSHHook(ssh_conn_id='SSH_ETL_DESENV')
    ssh_client = None
    try:
        ssh_client = ssh.get_conn()
        ssh_client.load_system_host_keys()
        #ssh_client.exec_command('/home/etl/etl/sh/teste_rubens.sh -h')
        ssh_client.exec_command('/home/etl/etl/sh/job_auditor_eletronico_loop.sh -h')
    finally:
        if ssh_client:
            ssh_client.close()

with DAG(
    "dag_executa_sh",start_date = datetime(2022,10,5),
    schedule_interval = '5 * * * *', catchup = False) as dag:
    
    call_ssh_task = PythonOperator(
        task_id='call_ssh_task',
        python_callable=executa_sh,
        dag=dag
    )

    [call_ssh_task]
