from __future__ import annotations

import datetime
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="basher",
    schedule=datetime.timedelta(hours=4),
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
) as dag:
    
    task = BashOperator(
        task_id='task',
        bash_command='pip list | grep airflow'
    )
    
    task
