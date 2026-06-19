"""
Step 12 — Dynamic DAGs
=======================
Concepts: DAG factory pattern, config-driven DAG generation, single-file factory

Dynamic DAGs = generating multiple DAGs from a single Python file + configuration.
This avoids copy-pasting DAG code for each data source, client, or pipeline stage.

AIRFLOW WARNING: the top-level of a DAG file is parsed by the scheduler every
30 seconds (by default). Keep top-level execution FAST. DAG factory functions
are fine — just don't do heavy I/O at module level.
"""

from datetime import datetime

from airflow.decorators import dag, task

# Configuration-driven DAG generation
_CONFIGS = [
    {"name": "sales", "schedule": "@daily", "source": "/data/sales"},
    {"name": "marketing", "schedule": "@weekly", "source": "/data/marketing"},
    {"name": "engineering", "schedule": "@hourly", "source": "/data/engineering"},
]


def build_dag(cfg: dict):
    dag_id = f"12_dynamic_{cfg['name']}"

    @dag(
        dag_id=dag_id,
        start_date=datetime(2024, 1, 1),
        schedule=cfg["schedule"],
        catchup=False,
        tags=["tutorial", "dynamic", cfg["name"]],
    )
    def generated_dag():
        @task
        def extract(source: str):
            print(f"Extracting from {source}")
            return f"data_from_{source}"

        @task
        def transform(data: str):
            print(f"Transforming {data}")
            return data.replace("data_from_", "transformed_")

        @task
        def load(data: str):
            print(f"Loading {data}")

        data = extract(cfg["source"])
        transformed = transform(data)
        load(transformed)

    return generated_dag()


# Create one DAG per config — this runs at module parse time (fast, no I/O)
for config in _CONFIGS:
    dag_obj = build_dag(config)
    dag_obj.dag_id = f"12_dynamic_{config['name']}"
