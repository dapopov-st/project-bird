from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 7, 28),
    'depends_on_past': False,
    'email':['dpopovvelasco@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'email_on_success': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
    }

dag = DAG(
    'get_and_push_bird_observations',
    default_args=default_args,
    description='A simple DAG to get and push bird observations',
    schedule_interval=timedelta(days=3),
    catchup=False #disable backfilling
)

with dag:
    get_bird_observations = BashOperator(
        task_id='get_and_push_bird_observations',
        bash_command='python3 /home/airflow/gcs/dags/scripts/get_data_and_push.py',
    )

 


