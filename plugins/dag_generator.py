from __future__ import annotations

import os
from datetime import timedelta

import pendulum
import yaml
from airflow.decorators import dag

from plugins.dag_config_reader import get_yaml_config_files
from plugins.dag_task_generator import generate_task

dag_config_files = get_yaml_config_files(
    f'{os.environ["PYTHONPATH"]}/dags', '*.yaml')

for dag_config_file in dag_config_files:
    with open(dag_config_file) as file:
        dag_config = yaml.safe_load(file)

    dag_id = dag_config.get('dag')['name']
    dag_behavior = dag_config.get('dag')['behavior']
    dag_tags = dag_config.get('dag')['tags']
    dag_owner = dag_config.get('dag')['owner']

    depend_on_past = dag_behavior['depends_on_past']
    start_date = tuple(map(int, dag_behavior['start_date'].split(',')))
    schedule_interval = dag_behavior['schedule_interval']
    catchup = dag_behavior['catch_up']
    retries = dag_behavior['retry']['count']
    retry_delay = timedelta(minutes=dag_behavior['retry']['delay_in_minute'])

    task_type = dag_config.get('task')['type']

    default_args = {
        'owner': dag_owner,
        'email': ['data.engineer@sirclo.com'],
        'depend_on_past': depend_on_past,
        'retries': retries,
        'retry_delay': retry_delay
    }

    @dag(catchup=catchup, dag_id=dag_id, default_args=default_args, schedule_interval=schedule_interval,
         start_date=pendulum.datetime(*start_date, tz='Asia/Jakarta'), tags=dag_tags)
    def dynamic_generated_dag(config):
        generate_task(dag_id=dag_id, task_config=config)
    dynamic_generated_dag(config=dag_config.get('task'))
