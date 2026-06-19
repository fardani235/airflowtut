"""
Step 6 — Variables
===================
Concepts: Variable.get(), Variable.set(), JSON auto-deserialize, env var fallback

Variables are a key-value store backed by the metadata database.
They're useful for:
  - Configuration that changes without DAG deploys
  - Feature flags
  - Shared constants across DAGs

When the variable value is valid JSON, Variable.get() auto-deserializes it
into a Python dict/list by default (deserialize_json=True is implicit for
complex types).

You can also set variables from environment variables (airflow.cfg or .env).
"""

from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import Variable


@dag(
    dag_id="06_variables",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["tutorial", "variables"],
)
def variables_demo():
    @task
    def show_variables():
        # 1. Simple string variable
        env = Variable.get("environment", default_var="development")
        print(f"Environment: {env}")

        # 2. JSON variable — auto-deserialized to a dict
        try:
            config = Variable.get("app_config", deserialize_json=True)
            print(f"Config: {config}")
            print(f"Database host: {config.get('db_host')}")
        except KeyError:
            Variable.set("app_config", {"db_host": "localhost", "db_port": 5432})
            print("Created default app_config variable")

        # 3. Setting a variable from within a task
        Variable.set("last_run", datetime.now().isoformat())
        print("Updated last_run variable")

    @task
    def use_variable():
        # Example: use a feature flag
        try:
            feature_enabled = Variable.get(
                "enable_new_pipeline", default_var="false"
            )
        except KeyError:
            feature_enabled = "false"

        if feature_enabled.lower() == "true":
            print("New pipeline path")
        else:
            print("Legacy pipeline path")

    show_variables() >> use_variable()


dag = variables_demo()
