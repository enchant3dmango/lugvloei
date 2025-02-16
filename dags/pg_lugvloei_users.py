from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.decorators import dag
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from google.cloud.bigquery import DestinationFormat, SourceFormat, WriteDisposition

from utilities.constants.types import DE_DAG_OWNER_NAME
from utilities.constants.variables import GCP_CONN_ID, GCS_DATA_LAKE_BUCKET
from utilities.slack import on_failure_callback, on_success_callback

start_date = pendulum.datetime(2025, 1, 25, tz="Asia/Jakarta")
tags = ["postgresql"]

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
    dag_id="pg_lugvloei_users",
    default_args=default_args,
    start_date=start_date,
    tags=tags,
    schedule="@daily",
)
def generate_dag():
    ts_nodash = "{{ data_interval_start.astimezone(dag.timezone).strftime('%Y-%m-%d %H:%M:%S') }}"
    filename = f"lugvloei/users/{ts_nodash}/users"

    extract = PostgresToGCSOperator(
        task_id="extract",
        postgres_conn_id="pg_lugvloei",
        gcp_conn_id=GCP_CONN_ID,
        bucket=GCS_DATA_LAKE_BUCKET,
        export_format=DestinationFormat.NEWLINE_DELIMITED_JSON,
        filename=filename + "__{}.json",
        sql="SELECT id, name, email, created_at, updated_at FROM users",
        write_on_empty=True
    )

    load = GCSToBigQueryOperator(
        task_id="load",
        bucket=GCS_DATA_LAKE_BUCKET,
        destination_project_dataset_table="lugvloei.users",
        source_objects=[filename + "*.json"],
        source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
        gcp_conn_id=GCP_CONN_ID,
        write_disposition=WriteDisposition.WRITE_TRUNCATE
    )

    extract.set_downstream(load)


generate_dag()
