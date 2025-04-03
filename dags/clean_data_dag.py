from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime
#
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 1),
    'retries': 1,
}

with DAG(
    dag_id='clean_store_data_dag',
    default_args=default_args,
    schedule_interval=None,  # manuel tetiklenecek
    catchup=False,
    description='Cleans dirty store transactions using Pandas',
) as dag:

    run_clean_script = SSHOperator(
        task_id='run_clean_script_on_spark',
        ssh_conn_id='ssh_spark_client',
        command='python3 /dataops/scripts/clean_store_data.py',

    )

    run_clean_script
