"""
Step 14 — Testing & Debugging
==============================
Concepts: unittest.TestCase, DAG validation, CLI tasks test, dag.test()

Airflow DAGs are just Python — and Python has great testing tools.

Three approaches (use all):
  1. CLI:  `airflow tasks test <dag_id> <task_id> <execution_date>`
  2. DAG's built-in test():  `python this_dag_file.py` (if dag.test() is called)
  3. pytest/unittest:  import DAG, validate structure, test task logic

This file demonstrates approach 3 (unit testing) inline,
and shows how to use the dag.test() method for quick iteration.
"""

from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import DagBag

# ── The DAG under test ─────────────────────────────────────────────


@dag(
    dag_id="14_testing_and_debugging",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["tutorial", "testing"],
)
def dag_under_test():
    @task
    def add(x: int, y: int) -> int:
        return x + y

    @task
    def double(value: int) -> int:
        return value * 2

    @task
    def display(value: int):
        print(f"Result: {value}")

    result = add(3, 4)
    doubled = double(result)
    display(doubled)


dag_obj = dag_under_test()

# ── Unit tests (run with pytest or unittest) ───────────────────────
# These can be in a separate test file — included here for reference.

import unittest
from unittest.mock import Mock


class TestAirflowDAG(unittest.TestCase):
    def test_dag_loaded(self):
        """Verify the DAG is parsed without errors."""
        dag_bag = DagBag(dag_folder="dags/", include_examples=False)
        dag = dag_bag.get_dag("14_testing_and_debugging")
        self.assertIsNotNone(dag)
        self.assertEqual(len(dag.tasks), 3)

    def test_dag_structure(self):
        """Verify task dependencies form the expected graph."""
        dag = dag_obj
        add_task = dag.get_task("add")
        double_task = dag.get_task("double")
        display_task = dag.get_task("display")

        self.assertIn(double_task, add_task.downstream_list)
        self.assertIn(display_task, double_task.downstream_list)

    def test_task_logic(self):
        """Test the Python functions directly (unit test)."""
        # The functions are nested inside the DAG function, so we reach in:
        add_fn = dag_obj.task_dict["add"].python_callable
        double_fn = dag_obj.task_dict["double"].python_callable

        self.assertEqual(add_fn(2, 3), 5)
        self.assertEqual(double_fn(5), 10)

        # Edge cases
        self.assertEqual(add_fn(-1, 1), 0)
        self.assertEqual(double_fn(0), 0)


# ── Quick test: run this file directly ─────────────────────────────
# `python dags/14_testing_and_debugging.py`
# This triggers a local DAG test run without needing a scheduler.
if __name__ == "__main__":
    dag_obj.test()
    # Or run the unittests:
    # unittest.main()
