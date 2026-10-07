from airflow.plugins_manager import AirflowPlugin

import retry_listener_hooks


class RetryListenerPlugin(AirflowPlugin):
    name = "retry_listener_plugin"
    listeners = [retry_listener_hooks]
