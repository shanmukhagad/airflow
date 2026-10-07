from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task

COUNTER = Path("/tmp/flaky_counter.txt")


@dag(schedule=None, start_date=datetime(2024, 1, 1), catchup=False)
def flaky_demo():
    @task(retries=4, retry_delay=timedelta(seconds=5))
    def flaky():
        n = int(COUNTER.read_text()) if COUNTER.exists() else 0
        COUNTER.write_text(str(n + 1))
        if n < 2:
            raise Exception(f"Simulated failure on attempt {n + 1}")
        print("Succeeded!")

    flaky()


flaky_demo()
