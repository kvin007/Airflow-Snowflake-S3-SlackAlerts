from typing import Dict, Any
from src.helpers.slack_helper import task_fail_slack_alert


def get_dag_default_args() -> Dict[str, Any]:
    return {
        'owner': 'kevin_pereda',
        'email_on_retry': False,
        'email_on_failure': False,
        'on_failure_callback': task_fail_slack_alert
    }
