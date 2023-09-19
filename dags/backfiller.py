from __future__ import annotations

from datetime import timedelta
import logging

import pendulum
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from plugins.constants.types import DE_DAG_OWNER_NAME, PYTHONPATH
from plugins.utilities.slack import on_failure_callback

start_date = pendulum.datetime(2023, 9, 1, tz='Asia/Jakarta')
tags = [
    'backfill'
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


@dag(catchup=False, dag_id='backfiller', default_args=default_args,
     start_date=start_date, tags=tags)
def configuration_reader(**kwargs):
    logging.info('Parsing configuration.')
    try:
        logging.info(f"""Backfill configuration:
        dag_id={kwargs['dag_run'].conf['dag_id']}
        start_date={kwargs['dag_run'].conf['start_date']}
        end_date={kwargs['dag_run'].conf['end_date']}
        """)
    except:
        raise Exception(
            'Wrong backfill configuration, the configuration should contain dag_id, start_date, and end_date. Kindly check the trigger configuration!')


configuration_reader_task = PythonOperator(
    task_id='configuration_reader',
    provide_context=True,
    python_callable=configuration_reader
)

backfill_task = BashOperator(
    task_id='backfill',
    bash_command='airflow dags backfill --disable-retry -s {start_date} -e {end_date} --verbose {dag_id}'.format(
        start_date="{{ dag_run.conf['start_date'] }}",
        end_date="{{ dag_run.conf['end_date'] }}",
        dag_id="{{ dag_run.conf['dag_id'] }}"
    )
)

configuration_reader_task >> backfill_task
