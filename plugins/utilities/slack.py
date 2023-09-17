from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

from plugins.constants.connections import SLACK_WEBHOOK_CONNECTION_ID


def generate_failure_message(context):
    dag_id         = context.get('task_instance').dag_id
    dag_owner      = context.get("dag").owner
    task_id        = context.get('task_instance').task_id
    log_url        = context.get('task_instance').log_url
    retry_count    = context.get('task_instance').try_number - 1
    run_id         = context.get('task_instance').run_id
    error_message  = (str(context['exception'])[:140] + '...') if len(
        str(context['exception'])) > 140 else str(context['exception'])

    return {
        "text": ":alert: *Task Failure Alert!*",
        "attachments": [
            {
                "color": "#E01E5A",
                "fields": [
                    {
                        "title": "DAG ID:",
                        "value": f"*{dag_id}*",
                        "short": True
                    },
                    {
                        "title": "Task ID:",
                        "value": f"_<{log_url}|{task_id}>_",
                        "short": True
                    },
                    {
                        "title": "DAG Owner:",
                        "value": f"{dag_owner}",
                        "short": True
                    },
                    {
                        "title": "Retry Count:",
                        "value": f"{retry_count}",
                        "short": True
                    },
                    {
                        "title": "Message:",
                        "value": f"{error_message}",
                        "short": False
                    }
                ],
                "footer": f"Run ID : {run_id}",
            }
        ]
    }


def on_failure_callback(context):
    """ 
    Send alert into Slack alert channel everytime a DAG failed.
    """

    operator = SlackWebhookOperator(
        task_id               = 'on_failure_callback',
        slack_webhook_conn_id = SLACK_WEBHOOK_CONNECTION_ID,
        message               = generate_failure_message(context=context)['text'],
        attachments           = generate_failure_message(context=context)['attachments']
    )

    return operator.execute(context=context)