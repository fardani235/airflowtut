"""
Step 5 — XComs (Cross-Communication)
=====================================
Concepts: return values → auto XCom, xcom_pull, custom keys, large data warning

With TaskFlow (@task), any value returned by a task is automatically pushed
to XCom under the key "return_value". Downstream tasks receive XCom values
as function arguments (via taskflow dependency injection).

Key rules:
  - XComs are stored in the metadata DB (by default) → keep values SMALL
  - For large data (>1 MB), use S3/GCS/AzureBlob and pass a URI instead
  - You can also manually push/pull using the task_instance context
"""

from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="05_xcoms",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["tutorial", "xcoms"],
)
def xcoms_demo():
    # 1. Automatic XCom push via return value
    @task
    def get_user_id():
        return 42

    # 2. Receive XCom as a function argument
    @task
    def fetch_user(user_id: int):
        # user_id comes from upstream task's return_value XCom
        print(f"Fetching user {user_id}")
        return {"name": "Alice", "email": "alice@example.com", "user_id": user_id}

    # 3. Manual XCom push/pull (when you can't use TaskFlow)
    #    Use the task_id, key, and context dict to pull.
    @task
    def consume_user(user_data: dict):
        # user_data is the dict returned by fetch_user
        print(f"User: {user_data['name']}, Email: {user_data['email']}")
        # You can also manually push additional XComs
        return "done"

    # 4. XCom with custom keys using the task_instance object directly
    @task
    def produce_custom_key(**context):
        ti = context["ti"]
        ti.xcom_push(key="custom_key", value="hello_custom")
        return "pushed"

    @task
    def consume_custom_key(**context):
        ti = context["ti"]
        value = ti.xcom_pull(task_ids="produce_custom_key", key="custom_key")
        print(f"Pulled custom XCom: {value}")
        return value

    uid = get_user_id()
    user_data = fetch_user(uid)
    consume_user(user_data)

    produce_custom_key() >> consume_custom_key()


dag = xcoms_demo()
