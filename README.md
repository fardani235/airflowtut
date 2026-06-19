# Apache Airflow Tutorial — 15 Steps

A structured, hands-on tutorial for Apache Airflow (2.10+ / 3.x). Each step is a
self-contained, runnable DAG file in `dags/`.

## Prerequisites

- Python 3.9+
- pip

## Quick Start

```bash
# 1. Setup (creates venv + installs Airflow + inits DB)
bash scripts/setup.sh
source venv/bin/activate

# 2. Start Airflow (two terminals, or both in background)
airflow api-server --port 8080    # UI at http://localhost:8080
airflow scheduler                 # triggers DAGs

# 3. Trigger a DAG
airflow dags trigger 01_hello_world
```

## Tutorial Map

| # | File | Concepts |
|---|------|----------|
| 01 | `dags/01_hello_world.py` | DAG anatomy, `@dag`/`@task` decorators, `BashOperator` |
| 02 | `dags/02_task_dependencies.py` | Bitshift operators, `chain()`, fan-out/fan-in |
| 03 | `dags/03_operators_deep_dive.py` | `PythonOperator` kwargs, `BashOperator` env, `EmptyOperator` |
| 04 | `dags/04_scheduling.py` | Cron, `timedelta`, `catchup`, `schedule=None` |
| 05 | `dags/05_xcoms.py` | Auto XCom via return values, `xcom_push`/`xcom_pull` |
| 06 | `dags/06_variables.py` | `Variable.get()/set()`, JSON auto-deserialize |
| 07 | `dags/07_connections.py` | `BaseHook.get_connection()`, connection management |
| 08 | `dags/08_sensors.py` | `FileSensor`, `ExternalTaskSensor`, `mode="reschedule"` |
| 09 | `dags/09_task_groups.py` | `@task_group`, nested groups, UI organization |
| 10 | `dags/10_branching.py` | `BranchPythonOperator`, `@task.branch`, join patterns |
| 11 | `dags/11_error_handling.py` | Retries, exponential backoff, SLAs, failure callbacks |
| 12 | `dags/12_dynamic_dags.py` | Config-driven DAG factory, multi-DAG generation |
| 13 | `dags/13_dag_parameters.py` | `TriggerDagRunOperator`, `conf`, cross-DAG params |
| 14 | `dags/14_testing_and_debugging.py` | `unittest.TestCase`, `dag.test()`, task isolation |
| 15 | `dags/15_best_practices.py` | Idempotency, atomic tasks, pools, `doc_md`, tagging |

## How to Run Each DAG

```bash
# Option A — trigger via CLI
airflow dags trigger 01_hello_world

# Option B — backfill a past date range
airflow dags backfill 01_hello_world --start-date 2024-01-01 --end-date 2024-01-02

# Option C — test a single task (no dependencies)
airflow tasks test 01_hello_world greet 2024-01-01

# Option D — run dag.test() inline (for DAGs with __main__ block)
python dags/14_testing_and_debugging.py
```

## Walkthrough

### Step 1 — Hello World
The fundamental building blocks: a DAG (via `@dag` decorator), tasks (via `@task`
decorator and `BashOperator`), and wiring them with `>>`. Notice how the return
value of `greet()` is passed to `confirm()` — this is XCom under the hood.

### Step 2 — Task Dependencies
Three core patterns: **linear** (`A >> B >> C`), **fan-out** (`A >> [B, C, D]`),
and **fan-in** (`[A, B, C] >> D`). The `chain()` helper is syntactic sugar for
long linear sequences.

### Step 3 — Operators Deep Dive
The `@task` decorator is the preferred way to define PythonOperator tasks. You
can pass arguments directly, return multiple values (as dict), and use
`BashOperator` with environment variables via the `env` parameter.

### Step 4 — Scheduling
- `@daily`, `@hourly`, `cron` expressions, and `timedelta` schedules
- `catchup=False` skips past intervals (recommended for most DAGs)
- `schedule=None` for manual-only DAGs
- `start_date` must be in the past for scheduled runs

### Step 5 — XComs
TaskFlow auto-pushes return values to XCom under the key `return_value`.
Downstream tasks receive these values as function arguments. For custom keys,
use `ti.xcom_push()` / `ti.xcom_pull()` via the `**context` dict. Keep XCom
values small — use object storage URIs for large data.

### Step 6 — Variables
Use `Variable.get()` and `Variable.set()` for dynamic configuration. JSON
variables are auto-deserialized when `deserialize_json=True`. Set defaults
with `default_var` to avoid `KeyError`.

### Step 7 — Connections
Connections separate credentials from code. `BaseHook.get_connection()` retrieves
a connection object. Never hardcode credentials in DAG files. This DAG creates
a demo HTTP connection programmatically.

### Step 8 — Sensors
Sensors wait for a condition. `FileSensor` waits for a file to appear.
`ExternalTaskSensor` waits for another DAG to finish. Use `mode="reschedule"`
to free worker slots between checks. Always set `timeout` to avoid indefinite
blocking.

### Step 9 — Task Groups
TaskGroups organize tasks visually in the UI without affecting execution.
Use `@task_group` decorator or the `TaskGroup` context manager. Nesting is
supported for complex pipelines.

### Step 10 — Branching
`BranchPythonOperator` returns the `task_id` of the next task to run.
Non-selected branches are SKIPPED. Always use a "join" task downstream of
ALL branches to converge the pipeline. `@task.branch` is the TaskFlow equivalent.

### Step 11 — Error Handling
Configure `retries`, `retry_delay`, `retry_exponential_backoff`, and
`execution_timeout` per task or via `default_args`. Use `on_failure_callback`
for custom alerts. SLA miss callbacks warn when tasks run too long.

### Step 12 — Dynamic DAGs
Generate multiple DAGs from a single file using a config-driven factory
function. Keep module-level code fast (no I/O) since the scheduler parses
DAG files frequently.

### Step 13 — DAG Parameters
`TriggerDagRunOperator` lets one DAG start another with a `conf` dict.
Use `wait_for_completion=True` to synchronize. Parameters can also be passed
via the UI or `airflow dags trigger --conf`.

### Step 14 — Testing & Debugging
Three approaches: (1) `airflow tasks test` for single-task debugging,
(2) `dag.test()` for local DAG execution, (3) `unittest.TestCase` for
CI/CD pipelines. Test task logic by calling the Python functions directly.

### Step 15 — Best Practices
A production-ready DAG demonstrating: idempotent upsert patterns, atomic
task design, resource pools for concurrency control, `doc_md` for in-UI
documentation, descriptive tags, and `max_active_runs=1` to prevent overlap.

## File Tree

```
airflowtut/
├── README.md
├── requirements.txt
├── scripts/
│   └── setup.sh
└── dags/
    ├── 01_hello_world.py
    ├── 02_task_dependencies.py
    ├── 03_operators_deep_dive.py
    ├── 04_scheduling.py
    ├── 05_xcoms.py
    ├── 06_variables.py
    ├── 07_connections.py
    ├── 08_sensors.py
    ├── 09_task_groups.py
    ├── 10_branching.py
    ├── 11_error_handling.py
    ├── 12_dynamic_dags.py
    ├── 13_dag_parameters.py
    ├── 14_testing_and_debugging.py
    └── 15_best_practices.py
```

## Next Steps

1. Work through the DAGs in order — they build on each other
2. After Step 5, try modifying a DAG to pass custom data through XCom
3. After Step 12, add your own config entries to generate new dynamic DAGs
4. Read the Airflow docs: https://airflow.apache.org/docs/apache-airflow/stable/
