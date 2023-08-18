from airflow.models import Connection
from airflow.hooks.base import BaseHook

SLACK_WEBHOOK_CONNECTION_ID = BaseHook.get_connection('SLACK_WEBHOOK_CONNECTION_ID').host

