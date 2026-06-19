"""
Step 10 — Branching
====================
Concepts: BranchPythonOperator, @task.branch, join after branch, conditional workflows

Branching allows your DAG to conditionally execute different paths.
The branch function returns the task_id(s) of the task(s) to execute next.
Tasks on non-selected branches are SKIPPED.

Key: you need a "join" task downstream of ALL branches to converge.
The join task must depend on ALL branches (so it only runs after the chosen path).
"""

from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator


@dag(
    dag_id="10_branching",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["tutorial", "branching"],
)
def branching_demo():
    start = EmptyOperator(task_id="start")
    join = EmptyOperator(task_id="join")

    # Decide which path based on the execution date
    def _choose_branch(**context):
        ds = context["ds"]  # execution date as string YYYY-MM-DD
        day_of_month = int(ds.split("-")[2])

        if day_of_month <= 15:
            return "first_half"
        else:
            return "second_half"

    branch = BranchPythonOperator(
        task_id="branch_task",
        python_callable=_choose_branch,
    )

    @task
    def first_half():
        print("First half of the month — running path A")

    @task
    def second_half():
        print("Second half of the month — running path B")

    # The join must depend on ALL possible branches
    start >> branch >> [first_half(), second_half()] >> join

    # Demonstrating @task.branch (TaskFlow equivalent)
    @task.branch
    def branch_with_tf(**context):
        ds = context["ds"]
        day = int(ds.split("-")[2])
        return "tf_path_a" if day % 2 == 0 else "tf_path_b"

    @task
    def tf_path_a():
        print("Even day — path A (TaskFlow branch)")

    @task
    def tf_path_b():
        print("Odd day — path B (TaskFlow branch)")

    tf_join = EmptyOperator(task_id="tf_join")

    branch_tf = branch_with_tf()
    branch_tf >> [tf_path_a(), tf_path_b()] >> tf_join


dag = branching_demo()
