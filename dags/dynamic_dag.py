from datetime import timedelta, datetime
import os
import sys
from airflow.decorators import dag, task

import yaml

sys.path.append(os.environ['AIRFLOW_HOME'])

from plugins.utils.dag_config_reader import get_yaml_config_files
from plugins.constants.miscellaneous import BRONZE, MYSQL_TO_BQ, POSTGRES_TO_BQ, SILVER


config_files = get_yaml_config_files(f'{os.getcwd()}\configs', '*.yaml')
generated_dags = []  # List to store the generated DAGs

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
        'schedule_interval': schedule_interval,
        'catchup': catchup,
        'concurrency': concurrency,
        'retries': retries,
        'retry_delay': retry_delay,
    }

    @dag(dag_id=dag_id, start_date=datetime(2023, 6, 10), default_args=default_args)
    def dynamic_generated_dag():
        if MYSQL_TO_BQ in config.get('dag')['type']:
            @task
            def print_me(message):
                print(message)
            print_me(config)
    generated_dags.append(dynamic_generated_dag)  # Add the generator function to the list

# Now you can execute the generated DAGs
for dag_generator in generated_dags:
    dag_generator()
