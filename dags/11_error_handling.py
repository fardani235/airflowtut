"""
Step 11 — Error Handling & Retries
===================================
Concepts: retries, retry_delay, exponential_backoff, email_on_failure, SLA, task timeout

Airflow provides multiple layers of reliability:
  - Task-level retries with configurable delays
  - Exponential backoff to avoid thundering herd
  - Email alerts on failure
  - SLA misses (task took too long)
  - execution_timeout (hard cap on task duration)
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException


@dag(
    dag_id="11_error_handling",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=10),
    },
    tags=["tutorial", "error_handling"],
    # SLA: if any task in this DAG takes longer than 5 minutes, mark SLA miss
    sla_miss_callback=lambda dag, task_list, blocking_task_list, slas, blocking_tis: print(
        f"SLA missed! DAG={dag.dag_id}, Blocking tasks={blocking_task_list}"
    ),
)
def error_handling_demo():
    # 1. Task with retries + exponential backoff
    @task(
        retries=3,
        retry_delay=timedelta(seconds=5),
        retry_exponential_backoff=True,  # 5s → 10s → 20s → 40s
        max_retry_delay=timedelta(minutes=5),
    )
    def unreliable_task():
        import random

        if random.random() < 0.7:
            raise ValueError("Simulated transient failure!")
        print("Task succeeded after possible retries")

    # 2. Task with execution_timeout (hard limit)
    @task(execution_timeout=timedelta(seconds=30))
    def fast_task():
        print("This task must finish within 30 seconds")

    # 3. Task that conditionally skips (not an error)
    @task
    def optional_task():
        import random

        if random.random() < 0.5:
            raise AirflowSkipException("Condition not met, skipping")
        print("Optional task ran")

    # 4. Task with on_failure_callback
    def notify_failure(context):
        print(f"FAILURE NOTIFICATION: task={context['task'].task_id}")
        print(f"Exception: {context.get('exception')}")

    @task(on_failure_callback=notify_failure)
    def monitored_task():
        raise ValueError("This will trigger the failure callback")

    # 5. Task that demonstrates proper error handling
    @task
    def graceful_task():
        try:
            # Simulate a risky operation
            result = 1 / 0
        except ZeroDivisionError:
            print("Gracefully handled division by zero")
            result = None

        return result

    [unreliable_task(), fast_task(), optional_task(), graceful_task()]


dag = error_handling_demo()
