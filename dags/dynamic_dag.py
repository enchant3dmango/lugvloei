from airflow import DAG
from plugins.constants.miscellaneous import BRONZE, MYSQL_TO_BQ, POSTGRES_TO_BQ, SILVER
from plugins.utils.dag_config_reader import get_yaml_config_files
from datetime import timedelta, datetime
import os
import sys

import yaml

from airflow.operators.python import PythonOperator

sys.path.append(os.environ['AIRFLOW_HOME'])

config_files = get_yaml_config_files(f'{os.getcwd()}/configs', '*.yaml')

for config_file in config_files:
    with open(config_file) as file:
        config = yaml.safe_load(file)
    if BRONZE in config.get('dag')['type']:
        dag_id = f'{BRONZE}_{config.get("database")["name"]}.{config.get("database")["table"]}'
    elif SILVER in config.get('dag')['type']:
        dag_id = f'{SILVER}_{config.get("database")["name"]}.{config.get("database")["table"]}'

    behavior = config.get('dag')['behavior']

    owner = config.get('dag')['owner']
    email = config.get('dag')['email']
    depend_on_past = behavior['depends_on_past']
    email_on_failure = behavior['email_on_failure']
    email_on_retry = behavior['email_on_retry']
    schedule_interval = behavior['schedule_interval']
    catchup = behavior['catch_up']
    concurrency = behavior['concurrency']
    retries = behavior['retry']['count']
    retry_delay = timedelta(minutes=behavior['retry']['delay'])

    default_args = {
        'owner': owner,
        'depend_on_past': depend_on_past,
        'email': email,
        'email_on_failure': email_on_failure,
        'email_on_retry': email_on_retry,
        'retries': retries,
        'retry_delay': retry_delay,
    }

    with DAG(dag_id=dag_id, start_date=datetime(2023, 6, 10), default_args=default_args, catchup=catchup,
             concurrency=concurrency, schedule_interval=schedule_interval) as dag:

        if MYSQL_TO_BQ in config.get('dag')['type']:
            def print_me(message):
                print(message)

            task = PythonOperator(
                task_id=f"submit_task_{config.get('database')['name']}_{config.get('database')['table']}",
                python_callable=print_me,
                op_kwargs={'message': config},
                dag=dag  # Add the 'dag' parameter
            )

            task  # Add the task to the DAG
