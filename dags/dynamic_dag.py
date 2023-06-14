from airflow import DAG
from plugins.constants.miscellaneous import BRONZE, MYSQL_TO_BQ, POSTGRES_TO_BQ, SILVER
from plugins.utils.dag_config_reader import get_yaml_config_files
from datetime import timedelta, datetime
import os

import yaml

from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task

config_files = get_yaml_config_files(f'{os.environ["PYTHONPATH"]}/dags/configs', '*.yaml')

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
    schedule = behavior['schedule']
    catchup = behavior['catch_up']
    max_active_tasks = behavior['max_active_tasks']
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
    
    print(config)

    @dag(dag_id=dag_id, start_date=datetime(2023, 6, 10), default_args=default_args, catchup=catchup, max_active_tasks=max_active_tasks, schedule=schedule)
    def dynamic_generated_dag():
            @task
            def print_me(message):
                print(message)

            @task
            def say_hello():
                print('Hello')

            print_me(config) >> say_hello()

    dynamic_generated_dag()
            
