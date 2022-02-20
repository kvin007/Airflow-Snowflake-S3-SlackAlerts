import logging
import requests

from airflow.hooks.base import BaseHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

SLACK_CONN_ID = 'slack_conn_id'


def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password

    slack_msg = f"""
            :red_circle: Task Failed. 
            *Task*: {context.get('task_instance').task_id}  
            *Dag*: {context.get('task_instance').dag_id} 
            *Execution Time*: {context.get('execution_date')}  
            *Log Url*: {context.get('task_instance').log_url}
            """
    
    failed_alert = SlackWebhookOperator(
        task_id='send_slack_alert',
        http_conn_id=SLACK_CONN_ID,
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='kevin.pereda26')
    
    return failed_alert.execute(context=context)