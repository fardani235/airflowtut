"""
Step 7 — Connections
=====================
Concepts: HttpHook, Connection URI, best practices, programmatic connection creation

Connections store credentials and endpoints for external systems.
Airflow ships with hooks for: HTTP, Postgres, MySQL, AWS, GCP, Azure, Snowflake, etc.

For this DAG to run, you need a connection. You can create it via:
  - UI: Admin → Connections → Add
  - CLI: airflow connections add 'my_http_conn' --conn-type 'http' --conn-host 'https://api.example.com'
  - Code: this DAG creates one programmatically if it doesn't exist

Best practices:
  - NEVER hardcode credentials in DAG files
  - Use Connections + Hooks for all external system access
  - Rotate secrets via the backend (Vault, AWS Secrets Manager, GCP Secret Manager)
"""

from datetime import datetime

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models import Connection
from airflow.operators.python import PythonOperator
from airflow.settings import Session


def ensure_http_connection():
    """Create a demo HTTP connection if one doesn't exist."""
    session = Session()
    conn = (
        session.query(Connection)
        .filter(Connection.conn_id == "tutorial_http_conn")
        .one_or_none()
    )
    if conn is None:
        conn = Connection(
            conn_id="tutorial_http_conn",
            conn_type="http",
            host="https://jsonplaceholder.typicode.com",
            description="Tutorial HTTP connection to JSONPlaceholder API",
        )
        session.add(conn)
        session.commit()
        print("Created tutorial_http_conn connection")
    session.close()


@dag(
    dag_id="07_connections",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["tutorial", "connections"],
)
def connections_demo():
    @task
    def setup_connection():
        ensure_http_connection()

    @task
    def use_connection():
        # Retrieve connection in a task (do NOT call get_connection in global scope)
        conn = BaseHook.get_connection("tutorial_http_conn")
        print(f"Host: {conn.host}")
        print(f"Login: {conn.login}")
        print(f"Port: {conn.port}")
        print(f"Extra (JSON): {conn.extra_dejson}")

        # Build a full URL from connection parts
        url = f"{conn.host}/posts/1"
        print(f"Would call: {url}")
        # In real usage, you'd do:
        # import requests
        # resp = requests.get(url, auth=(conn.login, conn.password))

    @task
    def inspect_connection():
        # Show all connection attributes
        conn = BaseHook.get_connection("tutorial_http_conn")
        for attr in ["conn_id", "conn_type", "host", "schema", "login", "port", "extra"]:
            print(f"  {attr}: {getattr(conn, attr, 'N/A')}")

    setup_connection() >> use_connection() >> inspect_connection()


dag = connections_demo()
