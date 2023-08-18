from airflow.hooks.base import BaseHook

SLACK_WEBHOOK_CONNECTION_ID = 'SLACK_WEBHOOK_CONNECTION_ID'
SLACK_WEBHOOK_TOKEN = BaseHook.get_connection(
    'SLACK_WEBHOOK_CONNECTION_ID').password
