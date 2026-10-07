from datetime import datetime, timedelta

from airflow.sdk import dag, task


@dag(schedule=None, start_date=datetime(2024, 1, 1), catchup=False)
def always_fail_demo():
    @task(retries=1, retry_delay=timedelta(seconds=5))
    def boom():
        raise Exception("always fails")

    boom()


always_fail_demo()
