from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

from plugins.constants.types import DE_DAG_OWNER_NAME
from plugins.utilities.slack import on_failure_callback, on_success_callback

start_date = pendulum.datetime(2023, 8, 1, tz="Asia/Jakarta")
tags = ["utility"]

# Take a timestamp of 3 months ago as the limit
now = pendulum.now("Asia/Jakarta")
retention_period = now.subtract(months=1)

default_args = {
    "owner": DE_DAG_OWNER_NAME,
    "priority_weight": 1,
    "email": [DE_DAG_OWNER_NAME],
    "depend_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
    "on_success_callback": on_success_callback,
}


@dag(
    catchup=False,
    dag_id="metadata_cleaner",
    default_args=default_args,
    start_date=start_date,
    tags=tags,
    schedule="@monthly",
)
def generate_dag():
    execute = BashOperator(
        task_id="execute",
        bash_command="airflow db clean --clean-before-timestamp {clean_before_timestamp} --skip-archive --verbose --yes --tables {tables}".format(
            clean_before_timestamp=retention_period,
            tables="callback_request,celery_taskmeta,celery_tasksetmeta,dag,dag_run,dataset_event,import_error,job,log,session,sla_miss,task_fail,task_instance,task_reschedule,xcom",
        ),
    )

    execute


generate_dag()
