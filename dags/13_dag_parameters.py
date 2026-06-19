"""
Step 13 — DAG Parameters & Cross-DAG Triggers
==============================================
Concepts: TriggerDagRunOperator, conf dict, Runtime parameters (Airflow 2.3+),
          DagRun conf, programmatic triggering

Three ways to pass parameters into a DAG:
  1. TriggerDagRunOperator — one DAG triggers another, passing conf
  2. UI "Trigger DAG w/ config" — pass JSON at trigger time
  3. airflow CLI: `airflow dags trigger my_dag --conf '{"key":"val"}'`
"""

from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


# ---- Target DAG (receives parameters) ----
@dag(
    dag_id="13_target_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # only triggered externally
    catchup=False,
    tags=["tutorial", "parameters"],
)
def target_dag():
    @task
    def process_params(**context):
        conf = context["dag_run"].conf or {}
        print(f"Received config: {conf}")
        dataset = conf.get("dataset", "default_dataset")
        mode = conf.get("mode", "full_refresh")
        print(f"Processing dataset={dataset} in mode={mode}")
        return {"dataset": dataset, "mode": mode, "status": "ok"}

    process_params()


target_dag_obj = target_dag()


# ---- Orchestrator DAG (triggers target with params) ----
@dag(
    dag_id="13_trigger_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["tutorial", "parameters"],
)
def trigger_dag():
    trigger = TriggerDagRunOperator(
        task_id="trigger_target",
        trigger_dag_id="13_target_dag",
        conf={
            "dataset": "user_events",
            "mode": "incremental",
            "triggered_by": "13_trigger_dag",
        },
        wait_for_completion=True,  # wait for target DAG to finish
        poke_interval=10,
    )

    @task
    def after_target(**context):
        # Access the triggered DAG run from the context
        triggering_run = context["dag_run"]
        print(f"Triggered from DAG run: {triggering_run.run_id}")
        print("Target DAG completed successfully")

    trigger >> after_target()


trigger_dag()
