from __future__ import annotations

from datetime import timedelta

import pendulum
import yaml

from airflow.decorators import dag
from plugins.constants.types import DE_DAG_OWNER_NAME, PYTHONPATH
from plugins.constants.variables import DAG_GENERATOR_FEATURE_FLAG
from plugins.task_generator import generate_tasks
from plugins.utilities.generic import get_config_files
from plugins.utilities.slack import on_failure_callback

if DAG_GENERATOR_FEATURE_FLAG:
    config_files = get_config_files(
        f'{PYTHONPATH}/dags', 'dag.yaml')

    for config_file in config_files:
        with open(config_file) as file:
            config = yaml.safe_load(file)

        dag_config = config.get('dag')
        dag_id = dag_config['name']
        dag_owner = dag_config['owner']
        dag_tags = dag_config['tags']
        dag_priority_weight = dag_config['priority_weight']
        dag_behavior = dag_config['behavior']

        # DAG behavior
        depend_on_past = dag_behavior['depends_on_past']
        start_date = tuple(map(int, dag_behavior['start_date'].split(',')))
        schedule = dag_behavior['schedule']
        catchup = dag_behavior['catch_up']
        retries = dag_behavior['retry']['count']
        retry_delay = timedelta(
            minutes=dag_behavior['retry']['delay_in_minute'])

        default_args = {
            'owner': dag_owner,
            'priority_weight': dag_priority_weight,
            'email': [DE_DAG_OWNER_NAME],
            'depend_on_past': depend_on_past,
            'retries': retries,
            'retry_delay': retry_delay,
            'on_failure_callback': on_failure_callback
        }

        @dag(catchup=catchup, dag_id=dag_id, default_args=default_args, schedule=schedule,
             start_date=pendulum.datetime(*start_date, tz='Asia/Jakarta'), tags=dag_tags,
             template_searchpath=PYTHONPATH)
        def generate_dag():
            generate_tasks(dag_id=dag_id, config=config.get('task'))
        generate_dag()
