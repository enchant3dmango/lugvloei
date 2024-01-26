from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from plugins.constants.types import DE_DAG_OWNER_NAME
from plugins.utilities.slack import on_failure_callback, on_success_callback

start_date = pendulum.datetime(2023, 9, 1, tz='Asia/Jakarta')
tags = [
    'utility'
]

default_args = {
    'owner': DE_DAG_OWNER_NAME,
    'priority_weight': 1,
    'email': [DE_DAG_OWNER_NAME],
    'depend_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': on_failure_callback
}

# TODO: Add configuration validator function

configuration_template = {
    "dag_id": "foo.bar",
    "start_date": "2023-09-01",
    "end_date": "2023-09-07"
}


def configuration_reader(**kwargs):
    logging.info('Parsing configuration.')

    dag_run = kwargs.get('dag_run')
    logging.info(dag_run)

    try:
        logging.info(f"""Backfill configuration:
        dag_id={dag_run.conf['dag_id']}
        start_date={dag_run.conf['start_date']}
        end_date={dag_run.conf['end_date']}
        """)
    except:
        raise Exception(
            'Wrong backfill configuration, the configuration should contain dag_id, start_date, and end_date. Kindly check the trigger configuration!\n' +
            f'Here is an example configuration: {configuration_template}'
        )


@dag(catchup=False, dag_id='backfiller', default_args=default_args,
     start_date=start_date, tags=tags, schedule=None)
def generate_dag():
    configuration_reader_task = PythonOperator(
        task_id='configuration_reader',
        provide_context=True,
        python_callable=configuration_reader
    )

    executor_task = BashOperator(
        task_id='executor',
        bash_command='airflow dags backfill --disable-retry --rerun-failed-tasks --reset-dagruns  -s {start_date} -e {end_date} {dag_id} -y'.format(
            start_date="{{ dag_run.conf['start_date'] }}",
            end_date="{{ dag_run.conf['end_date'] }}",
            dag_id="{{ dag_run.conf['dag_id'] }}"
        ),
        on_success_callback=on_success_callback
    )

    configuration_reader_task.set_downstream(executor_task)


generate_dag()
