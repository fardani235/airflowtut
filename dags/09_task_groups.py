"""
Step 9 — Task Groups
=====================
Concepts: @task_group decorator, TaskGroup context manager, nested groups

TaskGroups group tasks visually in the Airflow UI (Tree/Graph view).
They do NOT affect execution — they are purely organizational.

You can nest TaskGroups to create clear visual hierarchies.
Use TaskGroups when your DAG has >10-15 tasks; they make the graph readable.
"""

from datetime import datetime

from airflow.decorators import dag, task, task_group


@dag(
    dag_id="09_task_groups",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["tutorial", "task_groups"],
)
def task_groups_demo():
    # Method 1: @task_group decorator
    @task_group(group_id="extract_group")
    def extract():
        @task
        def fetch_api():
            return "api_data"

        @task
        def fetch_db():
            return "db_data"

        return fetch_api(), fetch_db()

    # Method 2: stand-alone @task inside a group (via context manager)
    @task
    def transform(source: str):
        return f"transformed_{source}"

    @task
    def load(data: str):
        print(f"Loading {data}")

    @task_group(group_id="processing_group")
    def processing(inputs: tuple):
        api_data, db_data = inputs
        t1 = transform(api_data)
        t2 = transform(db_data)
        # Chain them
        [t1, t2] >> load("combined")

    # Run the pipeline
    raw_data = extract()
    processing(raw_data)

    # Method 3: Nested TaskGroups
    @task
    def start():
        print("Start")

    @task
    def end():
        print("End")

    @task_group(group_id="outer_group")
    def outer():
        @task_group(group_id="inner_group_a")
        def inner_a():
            @task
            def task_a1():
                return "a1"

            @task
            def task_a2():
                return "a2"

            task_a1() >> task_a2()

        @task_group(group_id="inner_group_b")
        def inner_b():
            @task
            def task_b1():
                return "b1"

            task_b1()

        inner_a()
        inner_b()

    start() >> outer() >> end()


dag = task_groups_demo()
