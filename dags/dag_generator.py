from datetime import timedelta
import os
from airflow.decorators import dag, task

import yaml

from generators import mysql_to_bq, postgres_to_bq
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

    default_args = {
        'owner': config.get('dag')['owner'],
        'depend_on_past': config.get('dag')['depends_on_past'],
        'email': config.get('dag')['email'],
        'email_on_failure': config.get('dag')['email_on_failure'],
        'email_on_retry': config.get('dag')['email_on_retry'],
        'schedule_interval': config.get('dag')['schedule_interval'],
        'concurrency': config.get('dag')['concurrency'],
        'retries': config.get('dag')['retry']['count'],
        'retry_delay': timedelta(minutes=config.get('dag')['retry']['delay']),
    }

    @dag(dag_id=dag_id, start_date=config.get('dag')['start_date'], default_args=default_args)
    def dag_generator():
        if MYSQL_TO_BQ in config.get('dag')['type']:
            mysql_to_bq.generate_dag(config, dag_id)
        elif POSTGRES_TO_BQ in config.get('dag')['type']:
            postgres_to_bq.generate_dag(config, dag_id)
    dag_generator()
