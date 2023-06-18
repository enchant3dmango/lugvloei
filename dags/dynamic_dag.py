from __future__ import annotations

from plugins.dag_config_reader import get_yaml_config_files
from plugins.dag_task_generator import generate_task
from datetime import timedelta
import os


import yaml
import pendulum

from airflow.decorators import dag

config_files = get_yaml_config_files(
    f'{os.environ["PYTHONPATH"]}/dags/configs', '*.yaml')

for config_file in config_files:
    with open(config_file) as file:
        cfg = yaml.safe_load(file)

    dag_id = cfg.get('dag')['name']
    dag_behavior = cfg.get('dag')['behavior']

    owner = cfg.get('dag')['owner']
    depend_on_past = dag_behavior['depends_on_past']
    start_date = tuple(map(int, dag_behavior['start_date'].split(',')))
    schedule = dag_behavior['schedule']
    catchup = dag_behavior['catch_up']
    retries = dag_behavior['retry']['count']
    retry_delay = timedelta(minutes=dag_behavior['retry']['delay_in_minute'])

    task_type = cfg.get('task')['type']

    default_args = {
        'owner': owner,
        'email': ['data.engineer@sirclo.com'],
        'depend_on_past': depend_on_past,
        'retries': retries,
        'retry_delay': retry_delay,
        'catchup': catchup,
        'schedule': schedule
    }

    @dag(dag_id=dag_id, start_date=pendulum.datetime(*start_date, tz='Asia/Jakarta'), default_args=default_args)
    def dynamic_generated_dag(config):
        generate_task(dag_id=dag_id, task_config=config)
    dynamic_generated_dag(config=cfg.get('task'))
