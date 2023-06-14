from __future__ import annotations

from plugins.constants.miscellaneous import BRONZE, SILVER
from plugins.utils.dag_config_reader import get_yaml_config_files
from datetime import timedelta
import os

import yaml
import pendulum

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
    start_date = tuple(map(int, behavior['start_date'].split(',')))
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
        'catchup': catchup,
        'max_active_tasks': max_active_tasks,
        'schedule': schedule,
        'timezone': 'Asia/Jakarta'
    }

    @dag(dag_id=dag_id, start_date=pendulum.datetime(*start_date, tz='Asia/Jakarta'), default_args=default_args)
    def dynamic_generated_dag():
            @task
            def print_me(message):
                print(message)

            @task
            def say_hello():
                print('Hello')

            print_me(config) >> say_hello()

    dynamic_generated_dag()
            
