"""
Step 4 — Scheduling Concepts
=============================
Concepts: schedule, start_date, catchup, cron vs timedelta, backfill, max_active_runs

Key facts:
  - schedule=None   → manual trigger only
  - schedule="@daily" → runs at midnight, equivalent to "0 0 * * *"
  - schedule=timedelta(hours=1) → runs every hour
  - start_date: the earliest date your DAG can be scheduled
  - catchup=False:  skip past DAG runs that should have happened
  - catchup=True:   backfill all missing intervals between start_date and now

WARNING: start_date must be in the PAST for the scheduler to trigger runs.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator


# ---- DAG A: daily schedule with catchup=False ----
@dag(
    dag_id="04_scheduling_daily",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["tutorial", "scheduling"],
)
def scheduling_daily():
    @task
    def daily_task():
        print("Running daily — catchup disabled, so only the latest interval runs")

    daily_task()


# ---- DAG B: cron expression every 30 minutes ----
@dag(
    dag_id="04_scheduling_cron",
    start_date=datetime(2024, 1, 1),
    schedule="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["tutorial", "scheduling"],
)
def scheduling_cron():
    BashOperator(
        task_id="cron_task",
        bash_command="echo 'Runs every 30 minutes'",
    )


# ---- DAG C: timedelta schedule (every 2 hours) ----
@dag(
    dag_id="04_scheduling_timedelta",
    start_date=datetime(2024, 1, 1),
    schedule=timedelta(hours=2),
    catchup=False,
    tags=["tutorial", "scheduling"],
)
def scheduling_timedelta():
    @task
    def timedelta_task():
        print("Runs every 2 hours via timedelta")

    timedelta_task()


# ---- DAG D: manual-only (schedule=None) ----
@dag(
    dag_id="04_scheduling_manual",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    tags=["tutorial", "scheduling"],
)
def scheduling_manual():
    @task
    def manual_only():
        print("Only runs when triggered manually")

    manual_only()


for dag_id, dag_obj in [
    ("04_scheduling_daily", scheduling_daily()),
    ("04_scheduling_cron", scheduling_cron()),
    ("04_scheduling_timedelta", scheduling_timedelta()),
    ("04_scheduling_manual", scheduling_manual()),
]:
    dag_obj.dag_id = dag_id
