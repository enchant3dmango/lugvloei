from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

from plugins.constants.connections import (SLACK_WEBHOOK_CONNECTION_ID,
                                           SLACK_WEBHOOK_TOKEN)


def generate_message(context):
    dag_id         = context.get('task_instance').dag_id
    task_id        = context.get('task_instance').task_id
    log_url        = context.get('task_instance').log_url
    execution_date = context.get('execution_date')
    error_message  = (str(context['exception'])[:140] + '...') if len(
        str(context['exception'])) > 140 else str(context['exception'])

    return {
        "text": ":alert: [Airflow] Task Failed, kindly fix it soon!",
        "attachments": [
            {
                "color": "#E01E5A",
                "fields": [
                    {
                        "title": "Dag ID:",
                        "value": f"{dag_id}"
                    },
                    {
                        "title": "Task ID:",
                        "value": "_<{}|{}>_".format(log_url, task_id)
                    },
                    {
                        "title": "Message:",
                        "value": f"{error_message}"
                    }
                ],
                "footer": f"Execution Date : {execution_date}"
            }
        ]
    }


def on_failure_callback(context):
    """ 
    Send alert into Slack alert channel everytime a DAG failed.
    """

    operator = SlackWebhookOperator(
        task_id='on_failure_callback',
        slack_webhook_conn_id=SLACK_WEBHOOK_CONNECTION_ID,
        webhook_token=SLACK_WEBHOOK_TOKEN,
        message=generate_message(context=context),
    )

    return operator.execute(context=context)
