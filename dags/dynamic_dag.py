from datetime import timedelta, datetime
import os
from airflow.decorators import dag, task

import yaml

from plugins.utils.dag_config_reader import get_yaml_config_files
from plugins.constants.miscellaneous import BRONZE, MYSQL_TO_BQ, POSTGRES_TO_BQ, SILVER


config_files = get_yaml_config_files(os.getcwd(), '*.yaml')

for config_file in config_files:
    with open(config_file) as file:
        config = yaml.safe_load(file)
    if BRONZE in config.get('dag')['type']:
        dag_id = f'{BRONZE}_{config.get("database")["name"]}.{config.get("database")["table"]}'
    elif SILVER in config.get('dag')['type']:
        dag_id = f'{SILVER}_{config.get("database")["name"]}.{config.get("database")["table"]}'

    owner = config.get('dag')['owner']
    depend_on_past = config.get('dag')['depends_on_past']
    email = config.get('dag')['email']
    email_on_failure = config.get('dag')['email_on_failure']
    email_on_retry = config.get('dag')['email_on_retry']
    schedule_interval = config.get('dag')['schedule_interval']
    catchup = config.get('dag')['catchup']
    concurrency = config.get('dag')['concurrency']
    retries = config.get('dag')['retry']['count']
    retry_delay = timedelta(minutes=config.get('dag')['retry']['delay'])

    default_args = {
        'owner': owner,
        'depend_on_past': depend_on_past,
        'email': email,
        'email_on_failure': email_on_failure,
        'email_on_retry': email_on_retry,
        'schedule_interval': schedule_interval,
        'catchup': catchup,
        'concurrency': concurrency,
        'retries': retries,
        'retry_delay': timedelta(minutes=retry_delay),
    }

    @dag(dag_id=dag_id, start_date=datetime(2023, 6, 10), default_args=default_args)
    def generator():
        if MYSQL_TO_BQ in config.get('dag')['type']:
            @task
            def print_me(message):
                print(message)
            print_me(config)
    generator()
