"""
Step 8 — Sensors
=================
Concepts: FileSensor, ExternalTaskSensor, mode='poke' vs 'reschedule', timeout, poke_interval

Sensors are a special kind of Operator that WAITS for a condition to be met.
They are useful for:
  - Waiting for a file to land in S3/FTP/local
  - Waiting for another DAG/task to finish (ExternalTaskSensor)
  - Waiting for an API response, DB record, etc.

Two modes:
  - poke (default): the sensor occupies a worker slot continuously while checking
  - reschedule: the sensor releases the worker slot between checks (more efficient)

A sensor can time out — if the condition isn't met within timeout seconds, the task fails.
"""

import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.external_task import ExternalTaskSensor


# First — create a file for the FileSensor to detect
@dag(
    dag_id="08_sensors_file",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["tutorial", "sensors"],
)
def sensors_file():
    # Create the watched directory
    create_dir = BashOperator(
        task_id="create_watch_dir",
        bash_command="mkdir -p /tmp/airflow_tutorial_sensor",
    )

    # Wait for a file to appear
    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath="/tmp/airflow_tutorial_sensor/ready.txt",
        poke_interval=10,  # check every 10 seconds
        timeout=600,  # fail after 10 minutes
        mode="reschedule",  # efficient — doesn't hold a slot
    )

    # Place the file after a delay
    create_ready_file = BashOperator(
        task_id="create_ready_file",
        bash_command="sleep 30 && touch /tmp/airflow_tutorial_sensor/ready.txt",
    )

    @task
    def file_found():
        print("File detected! Proceeding with downstream tasks...")
        # Cleanup
        os.remove("/tmp/airflow_tutorial_sensor/ready.txt")

    create_dir >> [wait_for_file, create_ready_file]
    wait_for_file >> file_found()


# Second — ExternalTaskSensor: wait for another DAG to complete
@dag(
    dag_id="08_sensors_external_task",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["tutorial", "sensors"],
)
def sensors_external_task():
    wait_for_hello = ExternalTaskSensor(
        task_id="wait_for_01_hello_world",
        external_dag_id="01_hello_world",
        external_task_id=None,  # None = wait for the entire DAG to complete
        allowed_states=["success"],
        failed_states=["failed"],
        poke_interval=30,
        timeout=3600,
        mode="reschedule",
    )

    @task
    def after_external():
        print("01_hello_world completed! We can proceed.")

    wait_for_hello >> after_external()


sensors_file()
sensors_external_task()
