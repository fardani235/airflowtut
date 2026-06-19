"""
Step 15 — Production Best Practices
====================================
Concepts: idempotency, task atomicity, pools, tags, doc_md, clear naming, DAG versioning

Production-readiness checklist:
  [ ] Idempotent tasks — rerunning produces the same result
  [ ] Atomic tasks — each task does ONE thing well
  [ ] Proper retry configuration
  [ ] Meaningful task_id names (snake_case, descriptive)
  [ ] Tags for filtering in the UI
  [ ] doc_md on DAGs and tasks for inline documentation
  [ ] Pools to limit resource contention
  [ ] Avoid top-level I/O (DB queries, HTTP calls) — use tasks
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

# Clear doc_md for the DAG — shown in the Airflow UI
DAG_DOC = """
# 15 — Best Practices

This DAG demonstrates production-ready patterns.

## Idempotency
This DAG is designed so that re-running any task produces the same output.
The `process_events` task filters by execution_date to avoid double-processing.

## Resource Management
We use `pool` to limit concurrent tasks across the entire Airflow cluster.
"""


@dag(
    dag_id="15_best_practices",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,  # prevent overlapping runs of the same DAG
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "owner": "data_engineering",
    },
    tags=["tutorial", "best_practices", "production"],
    doc_md=DAG_DOC,
)
def best_practices_demo():
    start = EmptyOperator(task_id="start", doc="Pipeline start marker")

    @task(
        doc="Fetch events for the current execution date only. Idempotent.",
        pool="data_fetch_pool",  # limit parallel fetch tasks
    )
    def fetch_events(**context):
        execution_date = context["ds"]
        # Idempotency: always process same date range
        print(f"Fetching events for {execution_date}")
        # In real code: SELECT * FROM events WHERE date = '{execution_date}'
        return f"events_{execution_date}"

    @task
    def enrich_events(events: str) -> str:
        """Enrich events with additional data — deterministic by input."""
        print(f"Enriching {events}")
        return f"enriched_{events}"

    @task(
        pool="data_write_pool",
    )
    def write_results(data: str, **context):
        """Write to destination — idempotent upsert pattern."""
        execution_date = context["ds"]
        print(f"Writing {data} for {execution_date}")
        # In real code: INSERT ... ON CONFLICT DO UPDATE
        # This is safe to re-run — will overwrite with same values

    @task
    def post_processing():
        """Post-processing steps — send notifications, clean up, etc."""
        print("Post-processing complete")

    end = EmptyOperator(task_id="end")

    # Build the pipeline
    events = fetch_events()
    enriched = enrich_events(events)
    start >> events >> enriched >> write_results(enriched) >> post_processing() >> end


dag = best_practices_demo()
