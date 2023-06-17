from __future__ import annotations

from plugins.constants.miscellaneous import RDBMS_TO_BQ
from plugins.dag_config_reader import get_yaml_config_files
from datetime import timedelta
import os

from generators import rdbms_to_bq

import yaml
import pendulum

from airflow.decorators import dag, task

config_files = get_yaml_config_files(
    f'{os.environ["PYTHONPATH"]}/dags/configs', '*.yaml')

for config_file in config_files:
    with open(config_file) as file:
        config = yaml.safe_load(file)

    dag_id = config.get('dag')['name']
    behavior = config.get('dag')['behavior']

    owner = config.get('dag')['owner']
    depend_on_past = behavior['depends_on_past']
    start_date = tuple(map(int, behavior['start_date'].split(',')))
    schedule = behavior['schedule']
    catchup = behavior['catch_up']
    retries = behavior['retry']['count']
    retry_delay = timedelta(minutes=behavior['retry']['delay_in_minute'])

    task_type = config.get('task')['type']

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
    def dynamic_generated_dag(task_type):
        if task_type in RDBMS_TO_BQ:
            task_flow = rdbms_to_bq.generate_task_flow()
        # TODO: Add other task_type generator here

        # Add the task flow to dag
        task_flow
    dynamic_generated_dag(task_type=task_type)
