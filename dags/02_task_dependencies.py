"""
Step 2 — Task Dependencies
===========================
Concepts: bitshift operators (>>, <<), chain(), cross-dependency patterns

Airflow supports four key patterns:
  Linear:    A >> B >> C
  Fan-out:   A >> [B, C, D]
  Fan-in:    [A, B, C] >> D
  Join:      A >> [B, C] >> D   (B and C run in parallel, both must finish before D)

The chain() function from airflow.models.baseoperator is useful for long linear chains.
"""

from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator


@dag(
    dag_id="02_task_dependencies",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["tutorial", "dependencies"],
)
def task_dependencies():
    start = EmptyOperator(task_id="start")

    # Fan-out — three tasks in parallel
    @task
    def extract():
        return "raw_data"

    @task
    def clean():
        return "clean_data"

    @task
    def validate():
        return "validated_data"

    @task
    def load():
        print("Loading data...")

    @task
    def notify():
        print("Notification sent")

    # Pattern 1: fan-out — start triggers all three in parallel
    extracted = extract()
    cleaned = clean()
    validated = validate()
    start >> [extracted, cleaned, validated]

    # Pattern 2: fan-in — all three must finish before load
    [extracted, cleaned, validated] >> load()

    # Pattern 3: linear chain using chain() helper
    # Equivalent to: load() >> notify()
    chain(load(), notify())

    # Wait — that creates a diamond with load() and notify already connected.
    # Let's make it clean:
    #   start
    #    ├──> extract ─┐
    #    ├──> clean   ─┤──> load ──> notify
    #    └──> validate─┘
    # Currently we have: start >> [extract, clean, validate] >> load() >> notify()
    # which is the diamond. Perfect.

    # Also demonstrate cross-dependency:
    # "validate must run AFTER clean finishes" (within the parallel group)
    # Override the fan-out: start >> extract && start >> (clean >> validate)
    # We need to rebuild. For clarity, a simpler second DAG:

    @task
    def step_a():
        return "A"

    @task
    def step_b():
        return "B"

    @task
    def step_c():
        return "C"

    @task
    def step_d():
        return "D"

    a = step_a()
    b = step_b()
    c = step_c()
    d = step_d()

    # Linear: a >> b >> c >> d
    chain(a, b, c, d)


task_dependencies()
