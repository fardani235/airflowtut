"""
Step 1 — Hello World
=====================
Concepts: DAG, task, PythonOperator, BashOperator, @dag/@task decorators

The @dag decorator (Airflow 2.0+) turns a function into a DAG.
The @task decorator turns a function into a PythonOperator task.
"""

from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator


@dag(
    dag_id="01_hello_world",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["tutorial", "basics"],
)
def hello_world():
    # Task 1 — use the TaskFlow @task decorator (modern, recommended)
    @task
    def greet():
        print("Hello from Airflow!")
        return "greeting_complete"

    # Task 2 — use the classic BashOperator (sometimes BashOperator is simpler)
    print_date = BashOperator(
        task_id="print_date",
        bash_command="echo 'Current date:' && date",
    )

    # Task 3 — another @task that uses the return value of greet() via XCom
    @task
    def confirm(message: str):
        print(f"Received: {message}")
        print("DAG completed successfully")

    # Wire the tasks together
    msg = greet()
    # greet> runs first, then print_date and confirm run in parallel
    msg >> [confirm(msg), print_date]


dag = hello_world()
