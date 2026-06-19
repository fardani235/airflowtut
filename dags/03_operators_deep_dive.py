"""
Step 3 — Operators Deep Dive
=============================
Concepts: PythonOperator with op_kwargs, multiple outputs, BashOperator with env,
          EmptyOperator as placeholder, operator parameters

The @task decorator is syntactic sugar over PythonOperator. Under the hood,
each decorated function becomes a task instance running in a worker.
"""

from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


@dag(
    dag_id="03_operators_deep_dive",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["tutorial", "operators"],
)
def operators_deep_dive():
    # 1. EmptyOperator — a no-op. Useful as a placeholder or for grouping
    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end")

    # 2. @task with op_kwargs equivalent — just use function arguments
    @task
    def process_item(item: str, retries: int = 3):
        print(f"Processing {item} with up to {retries} retries")
        return f"{item}_processed"

    # 3. @task with multiple return values (returns a dict, accessible via XCom)
    @task
    def fetch_metadata():
        return {"size": 1024, "format": "parquet", "compressed": True}

    # 4. @task that consumes multiple upstream outputs
    @task
    def combine(data: str, meta: dict):
        print(f"Data: {data}, Metadata: {meta}")
        return {"status": "ok", "data": data}

    # 5. BashOperator — runs a shell command
    bash_task = BashOperator(
        task_id="bash_example",
        bash_command="""
            echo "Current user: $(whoami)"
            echo "Working dir: $(pwd)"
            echo "Airflow concurrency: {{ conf.get('core', 'parallelism') }}"
        """,
    )

    # 6. BashOperator with environment variables
    BashOperator(
        task_id="bash_with_env",
        bash_command='echo "My env var is $MY_VAR"',
        env={"MY_VAR": "hello_from_airflow"},
        append_env=True,  # keeps existing env, adds MY_VAR
    )

    # Wire it
    item = process_item("sales_data")
    meta = fetch_metadata()
    combined = combine(item, meta)

    begin >> [item, meta] >> combined >> bash_task >> end


dag = operators_deep_dive()
