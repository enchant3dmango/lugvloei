import os

from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from plugins.constants.miscellaneous import (MYSQL_TO_BQ, POSTGRES_TO_BQ,
                                             RDBMS_TO_BQ, SUBMIT_SPARK_JOB)


def generate_task(dag_id, task_config):
    if MYSQL_TO_BQ in RDBMS_TO_BQ.__members__ or POSTGRES_TO_BQ in RDBMS_TO_BQ.__members__:
        return EmptyOperator(task_id=SUBMIT_SPARK_JOB)
